//go:build integration

package webhook_producer

import (
	"os"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Roda contra um Postgres de verdade (OUTBOX_TEST_DSN). Prova o que o teste unitário não alcança: o
// AutoMigrate acrescenta `lane` numa tabela de PRODUÇÃO que já tem linhas (formato do 202608u), e as
// linhas antigas caem na faixa ao vivo.
func TestOutboxLanesIntegration(t *testing.T) {
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN não definido")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	// tabela no formato do 202608u (sem lane), com uma linha pendente de antes do swap
	db.Exec(`drop table if exists webhook_outbox`)
	if err := db.Exec(`create table webhook_outbox (id bigserial primary key, instance_id text, event text, payload bytea,
		status text, attempts bigint, next_attempt_at timestamptz, last_status bigint, last_error text,
		created_at timestamptz, delivered_at timestamptz)`).Error; err != nil {
		t.Fatal(err)
	}
	db.Exec(`insert into webhook_outbox (instance_id,event,payload,status,attempts,next_attempt_at,created_at)
		values ('A','Message','{}','pending',0,now(),now())`)

	if err := db.AutoMigrate(&WebhookOutbox{}); err != nil {
		t.Fatalf("AutoMigrate sobre a tabela antiga: %v", err)
	}
	var antiga WebhookOutbox
	db.First(&antiga, 1)
	if antiga.Lane != LaneLive {
		t.Fatalf("linha de antes do swap deveria cair em %q, caiu em %q", LaneLive, antiga.Lane)
	}

	repo := NewOutboxRepository(db)
	h1, _ := repo.Enqueue("A", "HistorySync", []byte(`{"big":1}`))
	h2, _ := repo.Enqueue("B", "HistorySync", []byte(`{"big":2}`))
	msg, _ := repo.Enqueue("A", "Message", []byte(`{}`))

	// histórico pendente da A NÃO segura a mensagem ao vivo da A…
	if older, _ := repo.HasOlderPending("A", msg, LaneLive); !older {
		t.Fatal("a linha ao vivo antiga (id 1) deveria contar como pendente mais antiga da faixa ao vivo")
	}
	db.Model(&WebhookOutbox{}).Where("id = 1").Update("status", OutboxDelivered)
	if older, _ := repo.HasOlderPending("A", msg, LaneLive); older {
		t.Fatal("histórico pendente não pode segurar a mensagem ao vivo")
	}
	// …e cada faixa só enxerga o que é dela
	live, _ := repo.ClaimDue(LaneLive, 10)
	if len(live) != 1 || live[0].ID != msg {
		t.Fatalf("faixa ao vivo deveria trazer só a mensagem, trouxe %+v", live)
	}
	bulk, _ := repo.ClaimDue(LaneBulk, 10)
	if len(bulk) != 2 || bulk[0].ID != h1 || bulk[1].ID != h2 {
		t.Fatalf("faixa bulk deveria trazer os 2 históricos em FIFO global, trouxe %d", len(bulk))
	}
	// entregue na bulk → corpo zerado (disco)
	repo.MarkDelivered(h1, 200, true)
	var entregue WebhookOutbox
	db.First(&entregue, h1)
	if entregue.Status != OutboxDelivered || entregue.Payload != nil {
		t.Fatalf("histórico entregue deveria ficar sem corpo: status=%s len=%d", entregue.Status, len(entregue.Payload))
	}
}
