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
	event, fromFullSync := eventMeta(payload)
	if p.outbox != nil && durableEvent(event) {
		p.enqueueAndTryImmediate(userID, event, laneFor(event, fromFullSync), payload, webhookUrl)
	} else {
		go p.sendWebhookWithRetry(webhookUrl, payload, 5, 30*time.Second, userID)
	}
	return nil
}

// enqueueAndTryImmediate grava a linha ANTES de tentar (write-ahead) e faz a entrega imediata pra não
// regredir a latência do caminho saudável. O que falhar fica pending → o worker drena com backoff.
func (p *webhookProducer) enqueueAndTryImmediate(instanceID, event, lane string, payload []byte, url string) {
	id, err := p.outbox.Enqueue(instanceID, event, lane, payload)
	if err != nil {
		// Não perde o evento: cai no envio legado (sem durabilidade, mas entrega).
		p.loggerWrapper.GetLogger(instanceID).LogWarn("[%s] outbox enqueue falhou (%v) — envio direto sem durabilidade", instanceID, err)
		go p.sendWebhookWithRetry(url, payload, 5, 30*time.Second, instanceID)
		return
	}
	// Histórico (bulk) e app-state em massa (sync) NUNCA vão na hora: o worker da faixa entrega. Um despejo
	// de milhares de eventos não pode virar milhares de POSTs (e de queries) simultâneos.
	if lane != LaneLive {
		return
	}
	go func() {
		// Ordem por instância: só envia quando não há pendente MAIS ANTIGA da mesma instância na faixa
		// (Receipt não passa na frente de Message). Quase sempre a mais antiga é o envio imediato do evento
		// anterior, ainda em voo — então ESPERA a vez um pouco em vez de desistir.
		// POR QUÊ (29/09): desistir na hora mandava ~1/3 das mensagens pro worker (tick de 5s, fila de
		// todas as contas) → 10–30s de "digitando…" no CRM com a mensagem já entregue no WhatsApp.
		if !p.esperarVez(instanceID, id, lane) {
			// a mais antiga está presa (retry/backoff) → o worker entrega na ordem, atrás dela
			p.outbox.Liberar(id)
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

// esperaDeVez — até quanto o envio imediato espera a pendente mais antiga da instância sair.
const (
	esperaDeVez      = 3 * time.Second
	esperaDeVezPasso = 100 * time.Millisecond
)

// esperarVez devolve true quando não há mais pendente anterior da instância na faixa (pode enviar), ou
// false se ela continuar lá depois da espera (falhou e está em backoff — quem entrega é o worker).
func (p *webhookProducer) esperarVez(instanceID string, id uint64, lane string) bool {
	limite := time.Now().Add(esperaDeVez)
	for {
		older, err := p.outbox.HasOlderPending(instanceID, id, lane)
		if err != nil {
			return false
		}
		if !older {
			return true
		}
		if time.Now().After(limite) {
			return false
		}
		time.Sleep(esperaDeVezPasso)
	}
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

// eventMeta lê do envelope JSON o campo "event" e, se houver, data.FromFullSync (app-state). O `data` vem
// cru e é lido à parte: um data que não seja objeto não pode apagar o nome do evento.
func eventMeta(payload []byte) (event string, fromFullSync bool) {
	var env struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return "", false
	}
	var data struct {
		FromFullSync bool `json:"FromFullSync"`
	}
	if len(env.Data) > 0 && json.Unmarshal(env.Data, &data) == nil {
		fromFullSync = data.FromFullSync
	}
	return env.Event, fromFullSync
}

// CreateGlobalQueues não faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
