package webhook_producer

import "time"

// Status de uma linha do outbox.
const (
	OutboxPending   = "pending"   // ainda não entregue; o worker re-tenta em next_attempt_at
	OutboxDelivered = "delivered" // 2xx recebido; mantido por 24h só pra observabilidade, depois limpo
	OutboxDead      = "dead"      // erro permanente (4xx) OU esgotou a janela de 48h; não re-tenta
)

// Faixas (lanes) do outbox. Cada faixa tem ordem própria por instância e worker próprio.
//
// POR QUÊ: a ordem por instância é estrita — se um evento da instância está pendente, tudo que vem depois
// dela espera atrás. Um cliente grande pareando despeja CENTENAS/MILHARES de chunks de HistorySync, e cada
// um leva segundos pra ingerir no CRM. Na faixa única, a mensagem AO VIVO desse cliente ficava horas atrás
// do histórico, e o worker único (que serve todos os tenants) ficava ocupado com ele. Histórico não tem
// relação de ordem com o ao vivo (o CRM deduplica por id), então vai pra uma faixa separada e cadenciada.
const (
	LaneLive = "live" // mensagens, recibos, conexão… — latência importa
	LaneBulk = "bulk" // HistorySync — volume alto, vazão cadenciada, nunca na frente do ao vivo
)

// laneFor decide a faixa pelo tipo do evento.
func laneFor(event string) string {
	if event == "HistorySync" {
		return LaneBulk
	}
	return LaneLive
}

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
	Lane          string     `gorm:"column:lane;not null;default:'live';index:idx_outbox_lane_due,priority:1"`
	Payload       []byte     `gorm:"column:payload"` // o envelope JSON já serializado; na faixa bulk é zerado ao entregar
	Status        string     `gorm:"column:status;index:idx_outbox_due,priority:1;index:idx_outbox_lane_due,priority:2"`
	Attempts      int        `gorm:"column:attempts"`
	NextAttemptAt time.Time  `gorm:"column:next_attempt_at;index:idx_outbox_due,priority:2;index:idx_outbox_lane_due,priority:3"`
	LastStatus    int        `gorm:"column:last_status"` // último HTTP status observado (0 = erro de rede/timeout)
	LastError     string     `gorm:"column:last_error"`
	CreatedAt     time.Time  `gorm:"column:created_at;index:idx_outbox_inst,priority:2"`
	DeliveredAt   *time.Time `gorm:"column:delivered_at"`
}

func (WebhookOutbox) TableName() string { return "webhook_outbox" }
