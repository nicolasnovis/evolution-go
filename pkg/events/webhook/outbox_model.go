package webhook_producer

import "time"

// Status de uma linha do outbox.
const (
	OutboxPending   = "pending"   // ainda não entregue; o worker re-tenta em next_attempt_at
	OutboxDelivered = "delivered" // 2xx recebido; mantido por 24h só pra observabilidade, depois limpo
	OutboxDead      = "dead"      // erro permanente (4xx) OU esgotou a janela de 48h; não re-tenta
)

// WebhookOutbox — fila write-ahead de eventos engine→webhook, no Postgres do PRÓPRIO engine (evogo_users).
//
// POR QUÊ: hoje um evento (mensagem, recibo, chunk de histórico…) que falha a entrega vive só numa goroutine
// com 5 tentativas em memória (webhook_producer.sendWebhookWithRetry) — some quando o Novi fica >2,5 min fora
// do ar OU quando o container reinicia. Ontem (21/09) o retry 5× amplificou a carga e 32 eventos morreram.
// Com a linha persistida ANTES da tentativa, ela sobrevive a restart e o worker re-tenta com backoff até o
// Novi ACKar (2xx). 4xx permanente (413, 401…) vira `dead` na hora — não gasta retry em erro que não passa.
//
// NÃO guarda a URL de propósito: o worker resolve o webhook ATUAL da instância na hora de enviar (ver
// OutboxWorker.resolveURL), pra troca de webhook/domínio valer e pro teste T2 (apontar pro buraco negro e
// voltar) funcionar.
type WebhookOutbox struct {
	ID            uint64     `gorm:"primaryKey;autoIncrement"`
	InstanceID    string     `gorm:"column:instance_id;index:idx_outbox_inst,priority:1"`
	Event         string     `gorm:"column:event"`
	Payload       []byte     `gorm:"column:payload"` // o envelope JSON já serializado (event/data/instanceId…)
	Status        string     `gorm:"column:status;index:idx_outbox_due,priority:1"`
	Attempts      int        `gorm:"column:attempts"`
	NextAttemptAt time.Time  `gorm:"column:next_attempt_at;index:idx_outbox_due,priority:2"`
	LastStatus    int        `gorm:"column:last_status"` // último HTTP status observado (0 = erro de rede/timeout)
	LastError     string     `gorm:"column:last_error"`
	CreatedAt     time.Time  `gorm:"column:created_at;index:idx_outbox_inst,priority:2"`
	DeliveredAt   *time.Time `gorm:"column:delivered_at"`
}

func (WebhookOutbox) TableName() string { return "webhook_outbox" }
