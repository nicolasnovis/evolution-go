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

// HasOlderPending diz se a instância tem alguma linha `pending` ANTERIOR a beforeID. Usado pela tentativa
// imediata: se já há pendente mais antiga, não envia na hora (deixa o worker drenar em ordem por instância),
// pra Receipt não passar na frente de Message.
func (r *OutboxRepository) HasOlderPending(instanceID string, beforeID uint64) (bool, error) {
	var n int64
	err := r.db.Model(&WebhookOutbox{}).
		Where("instance_id = ? AND status = ? AND id < ?", instanceID, OutboxPending, beforeID).
		Count(&n).Error
	return n > 0, err
}

// ClaimDue devolve até `limit` linhas pending vencidas, ORDENADAS por (instance_id, id) — o worker processa
// cada instância em ordem de id.
func (r *OutboxRepository) ClaimDue(limit int) ([]WebhookOutbox, error) {
	var rows []WebhookOutbox
	err := r.db.
		Where("status = ? AND next_attempt_at <= ?", OutboxPending, time.Now()).
		Order("instance_id asc, id asc").
		Limit(limit).
		Find(&rows).Error
	return rows, err
}

func (r *OutboxRepository) MarkDelivered(id uint64, status int) error {
	now := time.Now()
	return r.db.Model(&WebhookOutbox{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":       OutboxDelivered,
		"last_status":  status,
		"last_error":   "",
		"delivered_at": now,
	}).Error
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
