//go:build integration

package webhook_producer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// The real worker, end to end, against Postgres + a fake CRM (incident 24/11: group receipts held the live lane).
// Instance A has receiptsMaxPending+50 pending group receipts and ONE live message behind them.
// Expected: the live message arrives on the first live ticks (receipts never hold it); the cap runs
// BEFORE the first receipts drain (the 50 oldest are dead and never POSTed, the first receipt
// delivered is #51); instance B's receipt is delivered too.
func TestOutboxReceiptsWorkerIntegration(t *testing.T) {
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN não definido")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1) // serialize on the client side (PGlite, used to validate this, is a single session)
	db.Exec(`drop table if exists webhook_outbox`)
	if err := db.AutoMigrate(&WebhookOutbox{}); err != nil {
		t.Fatal(err)
	}
	repo := NewOutboxRepository(db)

	total := receiptsMaxPending + 50
	if err := db.Exec(`INSERT INTO webhook_outbox (instance_id, event, lane, payload, status, attempts, next_attempt_at, last_status, last_error, created_at)
		SELECT 'A', 'Receipt', 'receipts', convert_to('{"seq":"' || g || '"}', 'UTF8'), 'pending', 0, now(), 0, '', now()
		FROM generate_series(1, ?) g`, total).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Enqueue("B", "Receipt", LaneReceipts, []byte(`{"seq":"B1"}`)); err != nil {
		t.Fatal(err)
	}
	msg, err := repo.Enqueue("A", "Message", LaneLive, []byte(`{"seq":"msgA"}`))
	if err != nil {
		t.Fatal(err)
	}
	repo.Liberar(msg) // as if the immediate send had given up: only the worker can deliver it now

	var mu sync.Mutex
	var got []string
	arrived := map[string]time.Duration{}
	start := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Seq string `json:"seq"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		got = append(got, body.Seq)
		arrived[body.Seq] = time.Since(start)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lm := logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: t.TempDir()})
	StartOutboxWorker(ctx, repo, func(string) (string, bool) { return srv.URL, true }, lm, 0)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		_, okMsg := arrived["msgA"]
		_, okB := arrived["B1"]
		n := len(got)
		mu.Unlock()
		if okMsg && okB && n >= 22 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	t.Logf("POSTs recebidos: %d; msgA em %v; B1 em %v; primeiros: %v", len(got), arrived["msgA"], arrived["B1"], got[:min(len(got), 8)])

	if d, ok := arrived["msgA"]; !ok || d > 3*time.Second {
		t.Errorf("mensagem ao vivo de A deveria chegar nos primeiros ticks mesmo com %d recibos pendentes, chegou=%v em %v", total, ok, d)
	}
	if _, ok := arrived["B1"]; !ok {
		t.Errorf("recibo da instância B deveria ser entregue pela faixa receipts")
	}
	for i := 1; i <= 50; i++ {
		if _, ok := arrived[fmt.Sprint(i)]; ok {
			t.Errorf("recibo #%d estava acima do teto e foi entregue (o teto deveria rodar antes do 1º drain)", i)
			break
		}
	}
	primeiroA := ""
	for _, s := range got {
		if s != "msgA" && s != "B1" {
			primeiroA = s
			break
		}
	}
	if primeiroA != "51" {
		t.Errorf("o 1º recibo de A entregue deveria ser o #51 (mais antigo que sobrou), foi %q", primeiroA)
	}
	var dead int64
	db.Model(&WebhookOutbox{}).Where("instance_id = ? AND lane = ? AND status = ? AND last_error LIKE ?", "A", LaneReceipts, OutboxDead, "descartado: teto%").Count(&dead)
	if dead != 50 {
		t.Errorf("deveriam ser 50 recibos de A descartados pelo teto, foram %d", dead)
	}
}
