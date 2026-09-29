package webhook_producer

import (
	"context"
	"sync"
	"time"

	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

const (
	// 1s: o worker é a REDE (evento cuja vez não chegou no envio imediato, retry). Com 5s, tudo que caía
	// nele esperava até 5s + a fila das outras contas.
	outboxTick  = time.Second
	outboxBatch = 200
	// Instâncias drenadas AO MESMO TEMPO por ciclo (dentro de cada uma, em ordem). Sequencial, uma conta
	// com POST lento atrasava todas as outras do lote.
	outboxParalelo = 8
	// Rodízio: por ciclo, no máximo isto de cada instância (ao vivo e sync). Ver OutboxRepository.ClaimDue.
	perInstanceQuota = 20
	// Faixa sync (app-state em massa): eventos pequenos, sem cadência; só não disputa com o ao vivo.
	syncTick  = 2 * time.Second
	syncBatch = 100
	// Faixa bulk (HistorySync): UM chunk em voo por vez, somando TODOS os tenants. Um cliente grande
	// pareando vira um fluxo contínuo e previsível pro CRM (e pro banco dele), em vez de uma rajada. Tick
	// curto porque o gargalo é o tempo de ingestão de cada chunk, não o intervalo.
	bulkTick      = 2 * time.Second
	bulkBatch     = 5
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
	// Teto de vazão da faixa bulk, em bytes/s (0 = sem teto). Ver paceFor.
	bulkBytesPerSec int
}

func newOutboxWorker(
	repo *OutboxRepository,
	resolveURL func(instanceID string) (string, bool),
	client *httpSender,
	logger *logger_wrapper.LoggerManager,
	bulkBytesPerSec int,
) *OutboxWorker {
	return &OutboxWorker{repo: repo, resolveURL: resolveURL, client: client, logger: logger, bulkBytesPerSec: bulkBytesPerSec}
}

// paceFor diz quanto esperar DEPOIS de entregar um chunk de histórico pra faixa bulk não passar de
// `bytesPerSec`: o chunk "custa" bytes/bytesPerSec segundos, descontado o tempo que o POST já levou.
//
// POR QUÊ (22/09, re-pareamento de uma conta com 200k mensagens): sem teto, a faixa entregava um chunk a
// cada ~2s — ~150 mensagens/s gravadas no CRM. Cada linha gravada vira um evento do Realtime do Supabase,
// que processa as mudanças do projeto numa fila ÚNICA (checando permissão de cada uma): o tempo real de
// TODOS os tenants atrasou/perdeu evento — mensagem ao vivo sem aparecer na lista nem notificar. O
// histórico continua vindo inteiro; só vem num ritmo que o tempo real aguenta.
func paceFor(bytes, bytesPerSec int, elapsed time.Duration) time.Duration {
	if bytesPerSec <= 0 || bytes <= 0 {
		return 0
	}
	custo := time.Duration(float64(bytes) / float64(bytesPerSec) * float64(time.Second))
	if custo <= elapsed {
		return 0
	}
	return custo - elapsed
}

// StartOutboxWorker sobe o worker de drain numa goroutine. Entrypoint exportado pro main.go — esconde o
// httpSender (interno do pacote). resolveURL devolve o webhook ATUAL da instância (nil/"" = adia sem contar
// como erro permanente).
func StartOutboxWorker(
	ctx context.Context,
	repo *OutboxRepository,
	resolveURL func(instanceID string) (string, bool),
	logger *logger_wrapper.LoggerManager,
	bulkBytesPerSec int,
) {
	w := newOutboxWorker(repo, resolveURL, newHTTPSender(logger), logger, bulkBytesPerSec)
	go w.Run(ctx)
}

// Run sobe um loop por faixa. Elas nunca disputam: o ao vivo segue com a latência de sempre enquanto o
// bulk despeja histórico no ritmo que o CRM aguenta.
func (w *OutboxWorker) Run(ctx context.Context) {
	w.logger.GetLogger("outbox").LogInfo("[outbox] worker iniciado (ao vivo: tick %s, rodízio %d/instância, %d instâncias em paralelo; sync: tick %s; histórico: 1 em voo, teto %d bytes/s)", outboxTick, perInstanceQuota, outboxParalelo, syncTick, w.bulkBytesPerSec)
	go w.runLane(ctx, LaneBulk, bulkTick, bulkBatch, false)
	go w.runLane(ctx, LaneSync, syncTick, syncBatch, false)
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
	rows, err := w.repo.ClaimDue(lane, batch, perInstanceQuota)
	if err != nil {
		w.logger.GetLogger("outbox").LogWarn("[outbox] claim %s falhou: %v", lane, err)
		return
	}
	if len(rows) == 0 {
		return
	}
	// Histórico: um em voo por vez, cadenciado (é o objetivo da faixa).
	if lane == LaneBulk {
		w.drenarInstancia(ctx, lane, rows)
		return
	}

	// Ao vivo e sync: cada instância na sua goroutine (até outboxParalelo ao mesmo tempo), em ordem de id
	// dentro dela. O ClaimDue já intercala as instâncias; aqui só se separa o que é de quem.
	ordem := make([]string, 0)
	porInstancia := make(map[string][]WebhookOutbox)
	for _, r := range rows {
		if _, ok := porInstancia[r.InstanceID]; !ok {
			ordem = append(ordem, r.InstanceID)
		}
		porInstancia[r.InstanceID] = append(porInstancia[r.InstanceID], r)
	}
	vagas := make(chan struct{}, outboxParalelo)
	var wg sync.WaitGroup
	for _, inst := range ordem {
		linhas := porInstancia[inst]
		wg.Add(1)
		vagas <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-vagas }()
			w.drenarInstancia(ctx, lane, linhas)
		}()
	}
	wg.Wait()
}

// drenarInstancia entrega linhas em ordem. Preserva a ordem POR INSTÂNCIA: se uma linha da instância falha
// de forma retentável, as posteriores dela (na mesma faixa) esperam o próximo ciclo. Um `dead` NÃO segura a
// instância — um evento permanentemente inentregável (413) não pode travar a fila atrás dele.
func (w *OutboxWorker) drenarInstancia(ctx context.Context, lane string, rows []WebhookOutbox) {
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

		inicio := time.Now()
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
		// Cadência da faixa de histórico, DEPOIS de gravar o resultado (um restart no meio da espera não
		// reentrega o que já foi). Vale pra sucesso E falha: re-tentar também grava no CRM.
		if lane == LaneBulk {
			if espera := paceFor(len(row.Payload), w.bulkBytesPerSec, time.Since(inicio)); espera > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(espera):
				}
			}
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
