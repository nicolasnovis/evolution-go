package webhook_producer

import (
	"errors"
	"testing"
	"time"
)

func TestClassifyResult(t *testing.T) {
	cases := []struct {
		name   string
		status int
		err    error
		want   int
	}{
		{"2xx entregue", 200, nil, resultDelivered},
		{"204 entregue", 204, nil, resultDelivered},
		{"sem resposta (timeout/rede)", 0, errors.New("dial timeout"), resultRetry},
		{"500 re-tenta", 500, errors.New("non-2xx"), resultRetry},
		{"503 re-tenta", 503, errors.New("non-2xx"), resultRetry},
		{"429 re-tenta", 429, errors.New("non-2xx"), resultRetry},
		{"408 re-tenta", 408, errors.New("non-2xx"), resultRetry},
		{"413 dead (payload grande)", 413, errors.New("non-2xx"), resultDead},
		{"401 dead", 401, errors.New("non-2xx"), resultDead},
		{"404 dead", 404, errors.New("non-2xx"), resultDead},
		{"400 dead", 400, errors.New("non-2xx"), resultDead},
	}
	for _, c := range cases {
		if got := classifyResult(c.status, c.err); got != c.want {
			t.Errorf("%s: classifyResult(%d) = %d, quero %d", c.name, c.status, got, c.want)
		}
	}
}

func TestDurableEvent(t *testing.T) {
	durables := []string{"Message", "SendMessage", "Receipt", "HistorySync", "Connected", "Disconnected", "LoggedOut", "Picture", "PushName", "Archive", "Pin", "GroupInfo", "Call", "PollVote"}
	for _, e := range durables {
		if !durableEvent(e) {
			t.Errorf("%q deveria ser durável", e)
		}
	}
	ephemeral := []string{"ChatPresence", "Presence", "QRCode", "QRTimeout", "QRSuccess"}
	for _, e := range ephemeral {
		if durableEvent(e) {
			t.Errorf("%q deveria ser efêmero (fora do outbox)", e)
		}
	}
	if durableEvent("") {
		t.Error("evento vazio não deve ser durável (cai no legado)")
	}
}

func TestBackoffFor(t *testing.T) {
	// monotônico não-decrescente e com teto de 1h.
	want := []struct {
		attempts int
		dur      time.Duration
	}{
		{1, 5 * time.Second},
		{2, 15 * time.Second},
		{3, time.Minute},
		{4, 5 * time.Minute},
		{5, 15 * time.Minute},
		{6, 30 * time.Minute},
		{7, time.Hour},
		{20, time.Hour}, // teto
	}
	for _, w := range want {
		if got := backoffFor(w.attempts); got != w.dur {
			t.Errorf("backoffFor(%d) = %s, quero %s", w.attempts, got, w.dur)
		}
	}
}

func TestEventMeta(t *testing.T) {
	if m := eventMeta([]byte(`{"event":"Message","data":{}}`)); m.Event != "Message" || m.FromFullSync {
		t.Errorf("eventMeta = %q/%v, quero Message/false", m.Event, m.FromFullSync)
	}
	if m := eventMeta([]byte(`{"event":"Archive","data":{"FromFullSync":true}}`)); m.Event != "Archive" || !m.FromFullSync {
		t.Errorf("eventMeta = %q/%v, quero Archive/true", m.Event, m.FromFullSync)
	}
	// data que não é objeto não pode apagar o nome do evento
	if m := eventMeta([]byte(`{"event":"Receipt","data":["x"]}`)); m.Event != "Receipt" || m.FromFullSync || m.Chat != "" {
		t.Errorf("eventMeta com data não-objeto = %+v, quero só Receipt", m)
	}
	if m := eventMeta([]byte(`não é json`)); m.Event != "" {
		t.Errorf("eventMeta de lixo = %q, quero vazio", m.Event)
	}
	// Receipt in the real envelope (events.Receipt with MessageSource embedded): chat and type come out.
	m := eventMeta([]byte(`{"event":"Receipt","state":"Read","data":{"Chat":"120363041234567890@g.us","Sender":"123456789012345@lid","IsFromMe":false,"IsGroup":true,"MessageIDs":["3EB0A1B2C3"],"Timestamp":"2026-11-24T10:00:00-03:00","Type":"read","MessageSender":"5521999999999@s.whatsapp.net"}}`))
	if m.Event != "Receipt" || m.Chat != "120363041234567890@g.us" || m.ReceiptType != "read" {
		t.Errorf("eventMeta do recibo de grupo = %+v, quero chat do grupo e tipo read", m)
	}
	// A Chat/Type of another type must not wipe FromFullSync (it would send an app-state flood back to live).
	if m := eventMeta([]byte(`{"event":"Archive","data":{"FromFullSync":true,"Chat":{"User":"x"},"Type":7}}`)); !m.FromFullSync || m.Chat != "" || m.ReceiptType != "" {
		t.Errorf("Chat/Type de outro tipo = %+v, quero FromFullSync mantido e chat/tipo vazios", m)
	}
}

// Histórico e app-state em massa vão pra faixas próprias: um despejo de milhares de eventos não pode
// segurar a mensagem ao vivo.
func TestLaneFor(t *testing.T) {
	if got := laneFor(eventInfo{Event: "HistorySync"}); got != LaneBulk {
		t.Errorf("HistorySync deveria ir pra %q, foi pra %q", LaneBulk, got)
	}
	if got := laneFor(eventInfo{Event: "Contact"}); got != LaneSync {
		t.Errorf("Contact deveria ir pra %q, foi pra %q", LaneSync, got)
	}
	for _, e := range []string{"Archive", "Pin", "LabelAssociationChat", "DeleteForMe", "MarkChatAsRead"} {
		if got := laneFor(eventInfo{Event: e, FromFullSync: true}); got != LaneSync {
			t.Errorf("%q de full sync deveria ir pra %q, foi pra %q", e, LaneSync, got)
		}
		if got := laneFor(eventInfo{Event: e}); got != LaneLive {
			t.Errorf("%q avulso deveria ir pra %q, foi pra %q", e, LaneLive, got)
		}
	}
	for _, e := range []string{"Message", "SendMessage", "Receipt", "Connected", "Disconnected", "LoggedOut", "OfflineSyncPreview", "OfflineSyncCompleted", "PollVote", "PushName", ""} {
		if got := laneFor(eventInfo{Event: e}); got != LaneLive {
			t.Errorf("%q deveria ir pra %q, foi pra %q", e, LaneLive, got)
		}
	}
}

// 24/11: ~63k group receipts held 2,417 live messages for 2h30. Receipts that other people send in a group
// or on my status go to their own lane; 1:1 receipts, my own read-self/played-self and every other event
// stay live.
func TestLaneForReceipts(t *testing.T) {
	const grupo = "120363041234567890@g.us"
	cases := []struct {
		name string
		ev   eventInfo
		want string
	}{
		{"entrega em grupo", eventInfo{Event: "Receipt", Chat: grupo, ReceiptType: ""}, LaneReceipts},
		{"leitura em grupo", eventInfo{Event: "Receipt", Chat: grupo, ReceiptType: "read"}, LaneReceipts},
		{"áudio tocado em grupo", eventInfo{Event: "Receipt", Chat: grupo, ReceiptType: "played"}, LaneReceipts},
		{"grupo no formato antigo", eventInfo{Event: "Receipt", Chat: "5521999999999-1600000000@g.us", ReceiptType: "read"}, LaneReceipts},
		{"quem viu meu status", eventInfo{Event: "Receipt", Chat: "status@broadcast", ReceiptType: "read"}, LaneReceipts},
		{"entrega do meu status", eventInfo{Event: "Receipt", Chat: "status@broadcast", ReceiptType: ""}, LaneReceipts},

		{"leitura individual (@lid)", eventInfo{Event: "Receipt", Chat: "123456789012345@lid", ReceiptType: "read"}, LaneLive},
		{"entrega individual (número)", eventInfo{Event: "Receipt", Chat: "5521999999999@s.whatsapp.net", ReceiptType: ""}, LaneLive},
		{"áudio tocado individual", eventInfo{Event: "Receipt", Chat: "123456789012345@lid", ReceiptType: "played"}, LaneLive},
		{"eu li o grupo no celular", eventInfo{Event: "Receipt", Chat: grupo, ReceiptType: "read-self"}, LaneLive},
		{"eu ouvi o áudio do grupo no celular", eventInfo{Event: "Receipt", Chat: grupo, ReceiptType: "played-self"}, LaneLive},
		{"eu vi um status no celular", eventInfo{Event: "Receipt", Chat: "status@broadcast", ReceiptType: "read-self"}, LaneLive},
		{"tipo desconhecido em grupo", eventInfo{Event: "Receipt", Chat: grupo, ReceiptType: "inactive"}, LaneLive},
		{"canal", eventInfo{Event: "Receipt", Chat: "120363000000000000@newsletter", ReceiptType: "read"}, LaneLive},
		{"lista de transmissão", eventInfo{Event: "Receipt", Chat: "1700000000@broadcast", ReceiptType: "read"}, LaneLive},
		{"recibo sem chat", eventInfo{Event: "Receipt"}, LaneLive},
		{"mensagem de grupo", eventInfo{Event: "Message", Chat: grupo}, LaneLive},
		{"envio pra grupo", eventInfo{Event: "SendMessage", Chat: grupo}, LaneLive},
		{"info do grupo", eventInfo{Event: "GroupInfo", Chat: grupo}, LaneLive},
	}
	for _, c := range cases {
		if got := laneFor(c.ev); got != c.want {
			t.Errorf("%s: laneFor(%+v) = %q, quero %q", c.name, c.ev, got, c.want)
		}
	}
}

// The lane comes out of the real envelope, the way Produce reads it. A group Message (and SendMessage)
// carries its chat inside data.Info, so it can never be mistaken for a group receipt.
func TestLaneFromEnvelope(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"leitura de grupo", `{"event":"Receipt","state":"Read","data":{"Chat":"120363041234567890@g.us","Sender":"111111111111111@lid","IsFromMe":false,"IsGroup":true,"MessageIDs":["3EB0A"],"Type":"read","MessageSender":"5521999999999@s.whatsapp.net"}}`, LaneReceipts},
		{"entrega de grupo", `{"event":"Receipt","state":"Delivered","data":{"Chat":"120363041234567890@g.us","Sender":"111111111111111@lid","IsFromMe":false,"IsGroup":true,"MessageIDs":["3EB0A"],"Type":""}}`, LaneReceipts},
		{"leitura do meu status", `{"event":"Receipt","state":"Read","data":{"Chat":"status@broadcast","Sender":"222222222222222@lid","IsGroup":false,"MessageIDs":["3EB0S"],"Type":"read"}}`, LaneReceipts},
		{"read-self de grupo", `{"event":"Receipt","state":"ReadSelf","data":{"Chat":"120363041234567890@g.us","Sender":"5521999999999@s.whatsapp.net","IsFromMe":true,"IsGroup":true,"MessageIDs":["3EB0B"],"Type":"read-self"}}`, LaneLive},
		{"leitura individual", `{"event":"Receipt","state":"Read","data":{"Chat":"123456789012345@lid","Sender":"123456789012345@lid","IsGroup":false,"MessageIDs":["3EB0C"],"Type":"read"}}`, LaneLive},
		{"mensagem de grupo", `{"event":"Message","data":{"Info":{"Chat":"120363041234567890@g.us","Sender":"111111111111111@lid","IsGroup":true,"ID":"3EB0D","Type":"text"},"Message":{"conversation":"oi"}}}`, LaneLive},
		{"envio pra grupo", `{"event":"SendMessage","data":{"Info":{"Chat":"120363041234567890@g.us","IsFromMe":true,"IsGroup":true,"ID":"3EB0E","Type":"text"},"Message":{"conversation":"oi"}}}`, LaneLive},
	}
	for _, c := range cases {
		if got := laneFor(eventMeta([]byte(c.payload))); got != c.want {
			t.Errorf("%s: faixa = %q, quero %q", c.name, got, c.want)
		}
	}
}

// O histórico entra num ritmo que o tempo real do CRM aguenta: o chunk "custa" bytes/teto segundos.
func TestPaceFor(t *testing.T) {
	cases := []struct {
		name    string
		bytes   int
		rate    int
		elapsed time.Duration
		want    time.Duration
	}{
		{"1,5MB a 50KB/s = 30s", 1_500_000, 50_000, 0, 30 * time.Second},
		{"desconta o tempo do POST", 1_500_000, 50_000, 4 * time.Second, 26 * time.Second},
		{"POST mais lento que o teto → não espera", 100_000, 50_000, 5 * time.Second, 0},
		{"sem teto (0) → não espera", 1_500_000, 0, 0, 0},
		{"chunk vazio → não espera", 0, 50_000, 0, 0},
	}
	for _, c := range cases {
		if got := paceFor(c.bytes, c.rate, c.elapsed); got != c.want {
			t.Errorf("%s: paceFor = %s, quero %s", c.name, got, c.want)
		}
	}
}
