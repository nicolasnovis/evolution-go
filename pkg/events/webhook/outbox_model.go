package webhook_producer

import (
	"strings"
	"time"
)

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
	LaneSync = "sync" // app-state em massa (agenda, full sync) — volume alto, sem cadência, nunca na frente do ao vivo
	// Receipts OTHER people send for my messages in a group or on my status: one per participant per message,
	// no latency need, never ahead of live, capped per instance (see crowdReceipt and TrimReceipts).
	LaneReceipts = "receipts"
)

// eventInfo is what the outbox reads from a webhook envelope to pick the lane (see eventMeta).
type eventInfo struct {
	Event        string // envelope "event"
	FromFullSync bool   // data.FromFullSync (app-state)
	Chat         string // data.Chat when it is a string (Receipt: the chat the receipt belongs to)
	ReceiptType  string // data.Type when it is a string (Receipt: "" = delivered, "read", "played", "read-self"…)
}

// laneFor decide a faixa pelo tipo do evento e pela flag FromFullSync do app-state.
//
// POR QUÊ a faixa sync (28/09): o celular de UM cliente empurrou a agenda inteira via app-state — 3.041
// eventos Contact em 2s, depois mais 3.044. Na faixa ao vivo eles ficaram na frente das mensagens e, com o
// claim ordenado por instance_id, as instâncias "depois" dela ficaram ~20 min sem receber mensagem. Contact
// só serve pro nome do contato no CRM (sem relação de ordem com Message), e um full sync de app-state
// (arquivar/fixar/silenciar em massa, inclusive o que a nossa auto-cura dispara) tem o mesmo perfil.
//
// WHY the receipts lane (24/11): a client in 169 groups (~35k participants, the biggest with 1,234) sent a
// message to dozens of them. Each participant answers with its own delivered AND read receipt, so ~63k
// Receipt events piled up in that instance's live lane. Live order per instance is strict and the worker
// delivers ~2 events/s per instance: 2,417 Message and 194 SendMessage waited behind them, 2h30 late and
// growing. Those receipts only feed the group tick and the "N/M viewed" count in the CRM, so nothing about
// them is worth holding a live message. Not sync: there they would hold Contact and app-state of the same
// instance for hours instead.
// Accepted trade-off: when live lags, a group receipt may now reach the CRM before the message it cites;
// the CRM then ignores it (that participant is missing from the count / the tick may not go up). Cosmetic.
func laneFor(ev eventInfo) string {
	switch {
	case ev.Event == "HistorySync":
		return LaneBulk
	case ev.Event == "Contact", ev.FromFullSync:
		return LaneSync
	case crowdReceipt(ev):
		return LaneReceipts
	}
	return LaneLive
}

// crowdReceipt says whether the event is a receipt that OTHER people send for my message in a group (@g.us)
// or on my status (status@broadcast): delivered (""), read or played. A single message to a big group is
// thousands of them.
//
// Everything else stays live, exactly as before:
//   - 1:1 receipts (@lid, @s.whatsapp.net): few, and they drive the ✓✓ of the conversations;
//   - read-self / played-self (I read or played it on my own phone), even in a group: the CRM uses them to
//     clear the unread badge and to flag messages it is missing, so they must keep arriving AFTER the
//     incoming Message they cite, which is in the live lane;
//   - any other receipt type, newsletters and broadcast lists.
func crowdReceipt(ev eventInfo) bool {
	if ev.Event != "Receipt" {
		return false
	}
	if !strings.HasSuffix(ev.Chat, "@g.us") && ev.Chat != "status@broadcast" {
		return false
	}
	switch ev.ReceiptType {
	case "", "read", "played":
		return true
	}
	return false
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
	// idx_outbox_inst_pending cobre o HasOlderPending (instance_id, lane, status, id): sem ele, cada evento
	// ao vivo fazia um count sequencial — na rajada de 28/09 foram 3 mil queries de 1–7s ao mesmo tempo.
	ID            uint64     `gorm:"primaryKey;autoIncrement;index:idx_outbox_inst_pending,priority:4"`
	InstanceID    string     `gorm:"column:instance_id;index:idx_outbox_inst,priority:1;index:idx_outbox_inst_pending,priority:1"`
	Event         string     `gorm:"column:event"`
	Lane          string     `gorm:"column:lane;not null;default:'live';index:idx_outbox_lane_due,priority:1;index:idx_outbox_inst_pending,priority:2"`
	Payload       []byte     `gorm:"column:payload"` // o envelope JSON já serializado; na faixa bulk é zerado ao entregar
	Status        string     `gorm:"column:status;index:idx_outbox_due,priority:1;index:idx_outbox_lane_due,priority:2;index:idx_outbox_inst_pending,priority:3"`
	Attempts      int        `gorm:"column:attempts"`
	NextAttemptAt time.Time  `gorm:"column:next_attempt_at;index:idx_outbox_due,priority:2;index:idx_outbox_lane_due,priority:3"`
	LastStatus    int        `gorm:"column:last_status"` // último HTTP status observado (0 = erro de rede/timeout)
	LastError     string     `gorm:"column:last_error"`
	CreatedAt     time.Time  `gorm:"column:created_at;index:idx_outbox_inst,priority:2"`
	DeliveredAt   *time.Time `gorm:"column:delivered_at"`
}

func (WebhookOutbox) TableName() string { return "webhook_outbox" }
