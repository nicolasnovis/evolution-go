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

// Enqueue grava o evento como `pending` com next_attempt_at = agora (tentativa imediata logo em seguida).
func (r *OutboxRepository) Enqueue(instanceID, event string, payload []byte) (uint64, error) {
	row := WebhookOutbox{
		InstanceID:    instanceID,
		Event:         event,
		Lane:          laneFor(event),
		Payload:       payload,
		Status:        OutboxPending,
		Attempts:      0,
		NextAttemptAt: time.Now(),
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
	var n int64
	err := r.db.Model(&WebhookOutbox{}).
		Where("instance_id = ? AND lane = ? AND status = ? AND id < ?", instanceID, lane, OutboxPending, beforeID).
		Count(&n).Error
	return n > 0, err
}

// ClaimDue devolve até `limit` linhas pending vencidas DE UMA FAIXA. Ao vivo sai ordenado por (instance_id,
// id). Bulk sai por id (FIFO global): dois clientes pareando ao mesmo tempo dividem a vazão por ordem de
// chegada, em vez de o de instance_id menor passar na frente do outro até terminar.
func (r *OutboxRepository) ClaimDue(lane string, limit int) ([]WebhookOutbox, error) {
	order := "instance_id asc, id asc"
	if lane == LaneBulk {
		order = "id asc"
	}
	var rows []WebhookOutbox
	err := r.db.
		Where("lane = ? AND status = ? AND next_attempt_at <= ?", lane, OutboxPending, time.Now()).
		Order(order).
		Limit(limit).
		Find(&rows).Error
	return rows, err
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
