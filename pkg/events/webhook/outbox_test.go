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

func TestEventName(t *testing.T) {
	if got := eventName([]byte(`{"event":"Message","data":{}}`)); got != "Message" {
		t.Errorf("eventName = %q, quero Message", got)
	}
	if got := eventName([]byte(`não é json`)); got != "" {
		t.Errorf("eventName de lixo = %q, quero vazio", got)
	}
}

// Histórico vai pra faixa própria: um despejo de milhares de chunks não pode segurar a mensagem ao vivo.
func TestLaneFor(t *testing.T) {
	if got := laneFor("HistorySync"); got != LaneBulk {
		t.Errorf("HistorySync deveria ir pra %q, foi pra %q", LaneBulk, got)
	}
	for _, e := range []string{"Message", "SendMessage", "Receipt", "Connected", "Disconnected", "LoggedOut", "OfflineSyncPreview", "OfflineSyncCompleted", "PollVote", ""} {
		if got := laneFor(e); got != LaneLive {
			t.Errorf("%q deveria ir pra %q, foi pra %q", e, LaneLive, got)
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
