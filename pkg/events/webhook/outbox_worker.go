package webhook_producer

import (
	"context"
	"time"

	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

const (
	outboxTick  = 5 * time.Second
	outboxBatch = 200
	// Faixa bulk (HistorySync): UM chunk em voo por vez, somando TODOS os tenants. Um cliente grande
	// pareando vira um fluxo contínuo e previsível pro CRM (e pro banco dele), em vez de uma rajada. Tick
	// curto porque o gargalo é o tempo de ingestão de cada chunk, não o intervalo.
	bulkTick      = 2 * time.Second
	bulkBatch     = 20
	outboxGiveUp  = 48 * time.Hour // depois disso, um evento que nunca entregou vira dead
	cleanupEvery  = time.Hour
	keepDelivered = 24 * time.Hour     // `delivered` some 24h depois
	keepDead      = 7 * 24 * time.Hour // `dead` some 7 dias depois
)

// backoffFor devolve o adiamento da PRÓXIMA tentativa a partir da contagem de tentativas já feitas.
// 5s → 15s → 1min → 5min → 15min → 30min → 1h (teto). Segura a rajada de retry que amplificou a carga.
func backoffFor(attempts int) time.Duration {
	switch {
	case attempts <= 1:
		return 5 * time.Second
	case attempts == 2:
		return 15 * time.Second
	case attempts == 3:
		return time.Minute
	case attempts == 4:
		return 5 * time.Minute
	case attempts == 5:
		return 15 * time.Minute
	case attempts == 6:
		return 30 * time.Minute
	default:
		return time.Hour
	}
}

// OutboxWorker drena a fila: a cada tick pega as linhas vencidas e tenta entregar, com backoff. Roda numa
// única goroutine (sem concorrência de claim). Sobrevive a restart por construção — as linhas ficam no PG.
type OutboxWorker struct {
	repo       *OutboxRepository
	resolveURL func(instanceID string) (string, bool) // webhook ATUAL da instância (resolvido a cada envio)
	client     *httpSender
	logger     *logger_wrapper.LoggerManager
}

func newOutboxWorker(
	repo *OutboxRepository,
	resolveURL func(instanceID string) (string, bool),
	client *httpSender,
	logger *logger_wrapper.LoggerManager,
) *OutboxWorker {
	return &OutboxWorker{repo: repo, resolveURL: resolveURL, client: client, logger: logger}
}

// StartOutboxWorker sobe o worker de drain numa goroutine. Entrypoint exportado pro main.go — esconde o
// httpSender (interno do pacote). resolveURL devolve o webhook ATUAL da instância (nil/"" = adia sem contar
// como erro permanente).
func StartOutboxWorker(
	ctx context.Context,
	repo *OutboxRepository,
	resolveURL func(instanceID string) (string, bool),
	logger *logger_wrapper.LoggerManager,
) {
	w := newOutboxWorker(repo, resolveURL, newHTTPSender(logger), logger)
	go w.Run(ctx)
}

// Run sobe um loop por faixa. As duas nunca disputam: o ao vivo segue com a latência de sempre enquanto o
// bulk despeja histórico no ritmo que o CRM aguenta.
func (w *OutboxWorker) Run(ctx context.Context) {
	w.logger.GetLogger("outbox").LogInfo("[outbox] worker iniciado (ao vivo: tick %s; histórico: tick %s, 1 em voo)", outboxTick, bulkTick)
	go w.runLane(ctx, LaneBulk, bulkTick, bulkBatch, false)
	w.runLane(ctx, LaneLive, outboxTick, outboxBatch, true)
}

func (w *OutboxWorker) runLane(ctx context.Context, lane string, every time.Duration, batch int, doCleanup bool) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	lastCleanup := time.Now()

	for {
		select {
		case <-ctx.Done():
			w.logger.GetLogger("outbox").LogInfo("[outbox] worker %s encerrado", lane)
			return
		case <-tick.C:
			w.drainOnce(ctx, lane, batch)
			if doCleanup && time.Since(lastCleanup) >= cleanupEvery {
				if n, err := w.repo.Cleanup(time.Now().Add(-keepDelivered), time.Now().Add(-keepDead)); err != nil {
					w.logger.GetLogger("outbox").LogWarn("[outbox] cleanup falhou: %v", err)
				} else if n > 0 {
					w.logger.GetLogger("outbox").LogInfo("[outbox] cleanup: %d linhas removidas", n)
				}
				lastCleanup = time.Now()
			}
		}
	}
}

func (w *OutboxWorker) drainOnce(ctx context.Context, lane string, batch int) {
	rows, err := w.repo.ClaimDue(lane, batch)
	if err != nil {
		w.logger.GetLogger("outbox").LogWarn("[outbox] claim %s falhou: %v", lane, err)
		return
	}
	if len(rows) == 0 {
		return
	}

	// Preserva ordem POR INSTÂNCIA dentro da faixa: se uma linha da instância falha de forma retentável, as
	// posteriores dela (na mesma faixa) esperam o próximo ciclo. Um `dead` NÃO segura a instância — um evento
	// permanentemente inentregável (413) não pode travar a fila atrás dele.
	held := make(map[string]bool)

	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if held[row.InstanceID] {
			continue
		}
		url, ok := w.resolveURL(row.InstanceID)
		if !ok || url == "" {
			// instância sem webhook agora (pode voltar) → adia sem contar como erro permanente.
			w.repo.Reschedule(row.ID, 0, "webhook não resolvido", time.Now().Add(backoffFor(row.Attempts+1)))
			held[row.InstanceID] = true
			continue
		}

		status, err := w.client.post(url, row.Payload, row.InstanceID)
		switch classifyResult(status, err) {
		case resultDelivered:
			w.repo.MarkDelivered(row.ID, status, row.Lane == LaneBulk)
		case resultDead:
			w.repo.MarkDead(row.ID, status, errText(err))
			w.logger.GetLogger(row.InstanceID).LogWarn("[outbox] dead-letter %s status=%d event=%s id=%d", row.InstanceID, status, row.Event, row.ID)
			// não segura a instância — segue pra próxima linha dela.
		default: // resultRetry
			if time.Since(row.CreatedAt) > outboxGiveUp {
				w.repo.MarkDead(row.ID, status, "expirou 48h sem entregar")
				w.logger.GetLogger(row.InstanceID).LogWarn("[outbox] desisti (48h) event=%s id=%d", row.Event, row.ID)
			} else {
				w.repo.Reschedule(row.ID, status, errText(err), time.Now().Add(backoffFor(row.Attempts+1)))
			}
			held[row.InstanceID] = true
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
