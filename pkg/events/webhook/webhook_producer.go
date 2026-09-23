package webhook_producer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

// webhookHTTPTimeout — teto por requisição. Antes o http.Client não tinha Timeout: uma conexão pendurada
// (Novi lento, Vercel 504) travava a goroutine indefinidamente. Com o teto, a tentativa falha e o retry/
// outbox assume.
const webhookHTTPTimeout = 30 * time.Second

// Eventos EFÊMEROS não entram no outbox (alto volume e sem valor de reentrega): presença e QR. Todo o
// resto (Message, Receipt, HistorySync, CONNECTION, Picture, PushName, Archive, Pin, Label, Group, Call,
// PollVote…) é durável. Denylist (não allowlist) de propósito: um tipo novo nasce durável por segurança.
var ephemeralEvents = map[string]bool{
	"ChatPresence": true,
	"Presence":     true,
	"QRCode":       true,
	"QRTimeout":    true,
	"QRSuccess":    true,
}

func durableEvent(event string) bool {
	if event == "" {
		return false // sem tipo legível → cai no envio legado (não deveria acontecer)
	}
	return !ephemeralEvents[event]
}

// Resultado da classificação de uma tentativa de entrega.
const (
	resultDelivered = iota
	resultRetry
	resultDead
)

// classifyResult decide o destino de uma tentativa a partir do HTTP status (a decisão manda; o err é só log).
//   - 2xx           → entregue.
//   - status 0      → sem resposta (rede/timeout/DNS) → re-tenta.
//   - 408/429/5xx   → transiente → re-tenta.
//   - demais 4xx    → permanente (400/401/403/404/413) → dead (não gasta retry).
func classifyResult(status int, _ error) int {
	if status >= 200 && status < 300 {
		return resultDelivered
	}
	if status == 0 {
		return resultRetry
	}
	if status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 {
		return resultRetry
	}
	return resultDead
}

// httpSender faz o POST com timeout. Compartilhado entre o envio imediato (producer) e o worker de drain.
type httpSender struct {
	client *http.Client
	logger *logger_wrapper.LoggerManager
}

func newHTTPSender(logger *logger_wrapper.LoggerManager) *httpSender {
	return &httpSender{client: &http.Client{Timeout: webhookHTTPTimeout}, logger: logger}
}

// post devolve o status HTTP (0 = sem resposta) e um erro em qualquer caso não-2xx (pra log).
func (h *httpSender) post(url string, body []byte, userID string) (int, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drena o corpo pra reaproveitar a conexão

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("non-2xx response: %s", resp.Status)
	}
	return resp.StatusCode, nil
}

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager
	sender        *httpSender
	outbox        *OutboxRepository // nil = kill-switch OFF → comportamento antigo (fire-and-retry em memória)
}

// NewWebhookProducer — `outbox` nil desliga a durabilidade (mesmo comportamento de antes, agora com timeout).
func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
	outbox *OutboxRepository,
) producer_interfaces.Producer {
	return &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		sender:        newHTTPSender(loggerWrapper),
		outbox:        outbox,
	}
}

func (p *webhookProducer) Produce(
	queueName string,
	payload []byte,
	webhookUrl string,
	userID string,
) error {
	splitQueue := strings.Split(queueName, ".")
	if len(splitQueue) < 2 {
		return nil
	}

	// Webhook GLOBAL (raro nesta operação; não é por-instância) → caminho legado, agora com timeout.
	if p.url != "" {
		go p.sendWebhookWithRetry(p.url, payload, 5, 30*time.Second, userID)
	}

	if webhookUrl == "" {
		return nil
	}

	// Webhook POR INSTÂNCIA → durável via outbox quando ligado; senão legado.
	event := eventName(payload)
	if p.outbox != nil && durableEvent(event) {
		p.enqueueAndTryImmediate(userID, event, payload, webhookUrl)
	} else {
		go p.sendWebhookWithRetry(webhookUrl, payload, 5, 30*time.Second, userID)
	}
	return nil
}

// enqueueAndTryImmediate grava a linha ANTES de tentar (write-ahead) e faz a entrega imediata pra não
// regredir a latência do caminho saudável. O que falhar fica pending → o worker drena com backoff.
func (p *webhookProducer) enqueueAndTryImmediate(instanceID, event string, payload []byte, url string) {
	id, err := p.outbox.Enqueue(instanceID, event, payload)
	if err != nil {
		// Não perde o evento: cai no envio legado (sem durabilidade, mas entrega).
		p.loggerWrapper.GetLogger(instanceID).LogWarn("[%s] outbox enqueue falhou (%v) — envio direto sem durabilidade", instanceID, err)
		go p.sendWebhookWithRetry(url, payload, 5, 30*time.Second, instanceID)
		return
	}
	// Histórico (faixa bulk) NUNCA vai na hora: o worker da faixa entrega um por vez, cadenciado. Um
	// despejo de milhares de chunks não pode virar milhares de POSTs simultâneos no CRM.
	lane := laneFor(event)
	if lane == LaneBulk {
		return
	}
	go func() {
		// Se já há pendente mais antiga da mesma instância NA MESMA FAIXA, não envia na hora — deixa o
		// worker drenar em ordem de id (Receipt não passa na frente de Message).
		if older, _ := p.outbox.HasOlderPending(instanceID, id, lane); older {
			return
		}
		status, sendErr := p.sender.post(url, payload, instanceID)
		switch classifyResult(status, sendErr) {
		case resultDelivered:
			p.outbox.MarkDelivered(id, status, false)
			p.loggerWrapper.GetLogger(instanceID).LogInfo("[%s] webhook entregue (outbox id=%d, status=%d)", instanceID, id, status)
		case resultDead:
			p.outbox.MarkDead(id, status, errText(sendErr))
			p.loggerWrapper.GetLogger(instanceID).LogWarn("[%s] webhook dead-letter (outbox id=%d, status=%d, event=%s)", instanceID, id, status, event)
		default:
			p.outbox.Reschedule(id, status, errText(sendErr), time.Now().Add(backoffFor(1)))
			p.loggerWrapper.GetLogger(instanceID).LogWarn("[%s] webhook adiado p/ retry (outbox id=%d, status=%d): %v", instanceID, id, status, sendErr)
		}
	}()
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, maxRetries int, retryInterval time.Duration, userID string) {
	for i := 0; i < maxRetries; i++ {
		status, err := p.sender.post(url, body, userID)
		if err == nil {
			p.loggerWrapper.GetLogger(userID).LogInfo("[%s] webhook sent successfully - url: %s, status: %d", userID, url, status)
			return
		}
		// 4xx permanente não adianta re-tentar (413, 401…) — desiste na hora, como o outbox faria.
		if classifyResult(status, err) == resultDead {
			p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook permanent failure - url: %s, status: %d — não re-tenta", userID, url, status)
			return
		}
		p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook failed - url: %s, attempt: %d, status: %d, error: %v", userID, url, i+1, status, err)
		time.Sleep(retryInterval)
	}
	p.loggerWrapper.GetLogger(userID).LogError("[%s] webhook failed after maximum retries - url: %s", userID, url)
}

// eventName lê só o campo "event" do envelope JSON.
func eventName(payload []byte) string {
	var env struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return ""
	}
	return env.Event
}

// CreateGlobalQueues não faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
