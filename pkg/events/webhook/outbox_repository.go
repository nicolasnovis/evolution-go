package webhook_producer

import (
	"time"

	"gorm.io/gorm"
)

// OutboxRepository — acesso à tabela webhook_outbox. Um único worker faz o drain, então não é preciso
// FOR UPDATE SKIP LOCKED (nada compete pelo claim); mantém compatível com sqlite no dev.
type OutboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// reservaImediata — quanto tempo uma linha AO VIVO recém-gravada fica reservada pro envio imediato
// (espera de vez + POST com teto de 30s). O worker não a vê nesse tempo, então os dois nunca entregam a
// mesma linha. Se o envio imediato desistir, ele LIBERA a linha na hora (Liberar); a reserva só expira
// sozinha se o processo morrer no meio — aí o worker assume depois dela.
const reservaImediata = 40 * time.Second

// Enqueue grava o evento como `pending` na faixa `lane`. Ao vivo nasce reservado pro envio imediato;
// as outras faixas nascem vencidas (o worker é quem entrega).
func (r *OutboxRepository) Enqueue(instanceID, event, lane string, payload []byte) (uint64, error) {
	proxima := time.Now()
	if lane == LaneLive {
		proxima = proxima.Add(reservaImediata)
	}
	row := WebhookOutbox{
		InstanceID:    instanceID,
		Event:         event,
		Lane:          lane,
		Payload:       payload,
		Status:        OutboxPending,
		Attempts:      0,
		NextAttemptAt: proxima,
		CreatedAt:     time.Now(),
	}
	if err := r.db.Create(&row).Error; err != nil {
		return 0, err
	}
	return row.ID, nil
}

// HasOlderPending diz se a instância tem alguma linha `pending` ANTERIOR a beforeID NA MESMA FAIXA. Usado
// pela tentativa imediata: se já há pendente mais antiga, não envia na hora (deixa o worker drenar em ordem
// por instância), pra Receipt não passar na frente de Message. Só olha a própria faixa: um histórico
// pendente NÃO segura a mensagem ao vivo.
func (r *OutboxRepository) HasOlderPending(instanceID string, beforeID uint64, lane string) (bool, error) {
	// Existência (LIMIT 1), não count: só importa se há ALGUMA — com a instância atolada, contar tudo é caro.
	var ids []uint64
	err := r.db.Model(&WebhookOutbox{}).
		Where("instance_id = ? AND lane = ? AND status = ? AND id < ?", instanceID, lane, OutboxPending, beforeID).
		Limit(1).
		Pluck("id", &ids).Error
	return len(ids) > 0, err
}

// ClaimDue devolve até `limit` linhas pending vencidas DE UMA FAIXA.
//
// Bulk sai por id (FIFO global): dois clientes pareando ao mesmo tempo dividem a vazão por ordem de chegada.
//
// Ao vivo, sync e recibos saem em RODÍZIO entre instâncias: no máximo `perInstance` linhas de cada,
// intercaladas (a 1ª de cada instância, depois a 2ª de cada…), sempre em ordem de id dentro da instância.
// POR QUÊ (28/09): o claim era `ORDER BY instance_id, id LIMIT 200` — uma instância com milhares de
// pendentes enchia o lote sozinha e todas as de instance_id MAIOR ficavam fora de todos os ciclos até ela
// esvaziar (~20 min sem mensagem nos clientes "depois" dela).
func (r *OutboxRepository) ClaimDue(lane string, limit, perInstance int) ([]WebhookOutbox, error) {
	var rows []WebhookOutbox
	if lane == LaneBulk {
		err := r.db.
			Where("lane = ? AND status = ? AND next_attempt_at <= ?", lane, OutboxPending, time.Now()).
			Order("id asc").
			Limit(limit).
			Find(&rows).Error
		return rows, err
	}
	err := r.db.Raw(`
		SELECT * FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY id) AS rodada
			FROM webhook_outbox
			WHERE lane = ? AND status = ? AND next_attempt_at <= ?
		) fila
		WHERE rodada <= ?
		ORDER BY rodada, id
		LIMIT ?`, lane, OutboxPending, time.Now(), perInstance, limit).
		Scan(&rows).Error
	return rows, err
}

// Liberar tira a reserva do envio imediato: a linha fica vencida agora e o worker a entrega na ordem.
func (r *OutboxRepository) Liberar(id uint64) error {
	return r.db.Model(&WebhookOutbox{}).Where("id = ? AND status = ?", id, OutboxPending).
		Update("next_attempt_at", time.Now()).Error
}

// MarkDelivered marca 2xx. dropPayload zera o corpo (faixa bulk): um histórico grande são GBs de JSON, e a
// linha entregue só fica pra observabilidade — guardar o corpo 24h encheria o disco do VPS.
func (r *OutboxRepository) MarkDelivered(id uint64, status int, dropPayload bool) error {
	now := time.Now()
	upd := map[string]interface{}{
		"status":       OutboxDelivered,
		"last_status":  status,
		"last_error":   "",
		"delivered_at": now,
	}
	if dropPayload {
		upd["payload"] = nil
	}
	return r.db.Model(&WebhookOutbox{}).Where("id = ?", id).Updates(upd).Error
}

// Reschedule incrementa attempts e adia a próxima tentativa (backoff), mantendo `pending`.
func (r *OutboxRepository) Reschedule(id uint64, status int, errMsg string, next time.Time) error {
	return r.db.Model(&WebhookOutbox{}).Where("id = ?", id).Updates(map[string]interface{}{
		"attempts":        gorm.Expr("attempts + 1"),
		"last_status":     status,
		"last_error":      truncate(errMsg, 500),
		"next_attempt_at": next,
	}).Error
}

// MarkDead marca erro permanente (4xx) ou esgotamento da janela — não re-tenta.
func (r *OutboxRepository) MarkDead(id uint64, status int, errMsg string) error {
	return r.db.Model(&WebhookOutbox{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":      OutboxDead,
		"attempts":    gorm.Expr("attempts + 1"),
		"last_status": status,
		"last_error":  truncate(errMsg, 500),
	}).Error
}

// TrimReceipts caps the receipts lane: for each instance it keeps the `keep` NEWEST pending rows and marks
// the older ones dead, with `reason` in last_error. Returns how many rows it dropped.
//
// WHY (24/11): its own lane stops a receipt flood from holding live, not from growing. ~63k receipts at ~2/s
// per instance is ~9h of draining, a claim that sorts the whole backlog on every tick and receipts hours
// late that only feed a group tick / "N/M viewed" count. The newest are kept because they are the closest
// to what people see now. The lane is fixed in the SQL on purpose: this can never touch live, sync or bulk.
//
// WHY three plain statements and not one `UPDATE … WHERE id IN (SELECT … ROW_NUMBER() …)`: statistics are
// always taken while pending rows are rare (almost every row is delivered), so in the very flood this cap
// exists for the planner estimates ~1 pending receipt, joins that IN with a nested loop and re-runs the
// window for every pending row. Quadratic: with pre-flood statistics, 20k pending receipts took 6m50s on a
// real Postgres (19, via PGlite) and kept this lane's loop stuck the whole time. Here each statement is a
// single index scan (idx_outbox_lane_due, then idx_outbox_inst_pending), linear whatever the statistics say:
// the same 20k took ~0.4s. The loop that delivers this lane is the one calling this, so the rows at or below
// the cutoff cannot change state in between; rows enqueued meanwhile have a higher id and are kept.
func (r *OutboxRepository) TrimReceipts(keep int, reason string) (int64, error) {
	var instancias []string
	if err := r.db.Raw(`
		SELECT instance_id FROM webhook_outbox
		WHERE lane = ? AND status = ?
		GROUP BY instance_id
		HAVING count(*) > ?`, LaneReceipts, OutboxPending, keep).Scan(&instancias).Error; err != nil {
		return 0, err
	}
	var total int64
	for _, inst := range instancias {
		// cutoff = the newest row to drop: skip the instance's `keep` newest pending receipts
		var corte []uint64
		if err := r.db.Raw(`
			SELECT id FROM webhook_outbox
			WHERE instance_id = ? AND lane = ? AND status = ?
			ORDER BY id DESC
			LIMIT 1 OFFSET ?`, inst, LaneReceipts, OutboxPending, keep).Scan(&corte).Error; err != nil {
			return total, err
		}
		if len(corte) == 0 {
			continue
		}
		res := r.db.Exec(`
			UPDATE webhook_outbox SET status = ?, last_error = ?
			WHERE instance_id = ? AND lane = ? AND status = ? AND id <= ?`,
			OutboxDead, truncate(reason, 500), inst, LaneReceipts, OutboxPending, corte[0])
		if res.Error != nil {
			return total, res.Error
		}
		total += res.RowsAffected
	}
	return total, nil
}

// Cleanup apaga `delivered` antigos e `dead` bem antigos, pra a tabela não crescer sem limite.
func (r *OutboxRepository) Cleanup(deliveredBefore, deadBefore time.Time) (int64, error) {
	res := r.db.Where(
		"(status = ? AND delivered_at < ?) OR (status = ? AND created_at < ?)",
		OutboxDelivered, deliveredBefore, OutboxDead, deadBefore,
	).Delete(&WebhookOutbox{})
	return res.RowsAffected, res.Error
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
