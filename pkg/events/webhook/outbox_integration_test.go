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
	h1, _ := repo.Enqueue("A", "HistorySync", LaneBulk, []byte(`{"big":1}`))
	h2, _ := repo.Enqueue("B", "HistorySync", LaneBulk, []byte(`{"big":2}`))
	msg, _ := repo.Enqueue("A", "Message", LaneLive, []byte(`{}`))

	// histórico pendente da A NÃO segura a mensagem ao vivo da A…
	if older, _ := repo.HasOlderPending("A", msg, LaneLive); !older {
		t.Fatal("a linha ao vivo antiga (id 1) deveria contar como pendente mais antiga da faixa ao vivo")
	}
	db.Model(&WebhookOutbox{}).Where("id = 1").Update("status", OutboxDelivered)
	if older, _ := repo.HasOlderPending("A", msg, LaneLive); older {
		t.Fatal("histórico pendente não pode segurar a mensagem ao vivo")
	}
	// linha ao vivo nasce RESERVADA pro envio imediato: o worker não a vê…
	if cedo, _ := repo.ClaimDue(LaneLive, 10, 20); len(cedo) != 0 {
		t.Fatalf("linha ao vivo reservada não pode aparecer pro worker, apareceram %d", len(cedo))
	}
	// …até o envio imediato desistir e liberar
	repo.Liberar(msg)
	// …e cada faixa só enxerga o que é dela
	live, _ := repo.ClaimDue(LaneLive, 10, 20)
	if len(live) != 1 || live[0].ID != msg {
		t.Fatalf("faixa ao vivo deveria trazer só a mensagem, trouxe %+v", live)
	}
	bulk, _ := repo.ClaimDue(LaneBulk, 10, 20)
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

// Reproduz o incidente de 28/09: uma instância com centenas de pendentes na faixa ao vivo não pode deixar
// as de instance_id maior fora do lote; e a agenda em massa (Contact) nem entra na faixa ao vivo.
func TestOutboxRodizioIntegration(t *testing.T) {
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN não definido")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`drop table if exists webhook_outbox`)
	if err := db.AutoMigrate(&WebhookOutbox{}); err != nil {
		t.Fatal(err)
	}
	var idx int64
	db.Raw(`select count(*) from pg_indexes where tablename = 'webhook_outbox' and indexname = 'idx_outbox_inst_pending'`).Scan(&idx)
	if idx != 1 {
		t.Fatal("AutoMigrate deveria criar idx_outbox_inst_pending")
	}

	repo := NewOutboxRepository(db)
	// "8018" = a instância atolada; "b42a" e "ea6e" vêm depois dela na ordem de instance_id.
	for i := 0; i < 300; i++ {
		repo.Enqueue("8018", "Receipt", LaneLive, []byte(`{}`))
	}
	b1, _ := repo.Enqueue("b42a", "Message", LaneLive, []byte(`{}`))
	b2, _ := repo.Enqueue("b42a", "Receipt", LaneLive, []byte(`{}`))
	e1, _ := repo.Enqueue("ea6e", "Message", LaneLive, []byte(`{}`))
	// simula o envio imediato tendo desistido de todas (o que as faz cair no worker)
	db.Exec(`update webhook_outbox set next_attempt_at = now() where lane = 'live'`)

	lote, err := repo.ClaimDue(LaneLive, 200, 20)
	if err != nil {
		t.Fatal(err)
	}
	porInstancia := map[string][]uint64{}
	for _, r := range lote {
		porInstancia[r.InstanceID] = append(porInstancia[r.InstanceID], r.ID)
	}
	if len(porInstancia["8018"]) != 20 {
		t.Fatalf("a instância atolada deveria ter 20 no lote (cota), teve %d", len(porInstancia["8018"]))
	}
	if got := porInstancia["b42a"]; len(got) != 2 || got[0] != b1 || got[1] != b2 {
		t.Fatalf("b42a deveria vir inteira e em ordem de id, veio %v", got)
	}
	if got := porInstancia["ea6e"]; len(got) != 1 || got[0] != e1 {
		t.Fatalf("ea6e deveria vir no lote, veio %v", got)
	}
	// rodízio: a 1ª linha de cada instância vem antes da 2ª de qualquer uma
	if len(lote) < 3 || lote[0].InstanceID == lote[1].InstanceID {
		t.Fatalf("o lote deveria começar intercalando instâncias, começou %s,%s", lote[0].InstanceID, lote[1].InstanceID)
	}

	// Contact vai pra sync: não segura a mensagem ao vivo da própria instância
	repo.Enqueue("c0de", "Contact", LaneSync, []byte(`{}`))
	m, _ := repo.Enqueue("c0de", "Message", LaneLive, []byte(`{}`))
	if older, _ := repo.HasOlderPending("c0de", m, LaneLive); older {
		t.Fatal("Contact pendente (sync) não pode segurar a mensagem ao vivo")
	}
	sync, _ := repo.ClaimDue(LaneSync, 100, 20)
	if len(sync) != 1 || sync[0].Event != "Contact" {
		t.Fatalf("faixa sync deveria trazer o Contact, trouxe %d", len(sync))
	}
}

// 24/11: ~63k group receipts in the live lane held 2,417 messages for 2h30. In their own lane they never
// hold a live message, they drain in round-robin like the other lanes, and the cap drops the OLDEST pending
// ones of an instance above it, without touching delivered rows, other instances or other lanes.
func TestOutboxReceiptsLaneIntegration(t *testing.T) {
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN não definido")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`drop table if exists webhook_outbox`)
	if err := db.AutoMigrate(&WebhookOutbox{}); err != nil {
		t.Fatal(err)
	}
	repo := NewOutboxRepository(db)

	// "A" = the flooded instance: 30 group receipts, then a live message
	var recibos []uint64
	for i := 0; i < 30; i++ {
		id, err := repo.Enqueue("A", "Receipt", LaneReceipts, []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		recibos = append(recibos, id)
	}
	msg, _ := repo.Enqueue("A", "Message", LaneLive, []byte(`{}`))
	if older, _ := repo.HasOlderPending("A", msg, LaneLive); older {
		t.Fatal("recibo de grupo pendente não pode segurar a mensagem ao vivo")
	}
	b1, _ := repo.Enqueue("B", "Receipt", LaneReceipts, []byte(`{}`))
	repo.Enqueue("A", "Contact", LaneSync, []byte(`{}`))

	// receipts are born due (no immediate send) and drain in round-robin, in id order per instance
	lote, err := repo.ClaimDue(LaneReceipts, 100, 20)
	if err != nil {
		t.Fatal(err)
	}
	porInstancia := map[string][]uint64{}
	for _, r := range lote {
		porInstancia[r.InstanceID] = append(porInstancia[r.InstanceID], r.ID)
	}
	if got := porInstancia["A"]; len(got) != 20 || got[0] != recibos[0] {
		t.Fatalf("A deveria vir com 20 (cota), a partir do mais antigo, veio %v", got)
	}
	if got := porInstancia["B"]; len(got) != 1 || got[0] != b1 {
		t.Fatalf("B deveria vir no lote, veio %v", got)
	}
	// the live lane never sees receipts (and the message is still reserved for the immediate send)
	if live, _ := repo.ClaimDue(LaneLive, 100, 20); len(live) != 0 {
		t.Fatalf("a faixa ao vivo não pode ver recibos, viu %d linhas", len(live))
	}

	// cap: the 5 oldest of A were already delivered (they neither count nor change); of the 25 pending,
	// the 10 newest stay and the 15 oldest become dead
	for _, id := range recibos[:5] {
		repo.MarkDelivered(id, 200, false)
	}
	n, err := repo.TrimReceipts(10, "teto de teste")
	if err != nil {
		t.Fatal(err)
	}
	if n != 15 {
		t.Fatalf("deveria descartar os 15 pendentes mais antigos de A, descartou %d", n)
	}
	var sobraram []uint64
	db.Model(&WebhookOutbox{}).Where("instance_id = ? AND lane = ? AND status = ?", "A", LaneReceipts, OutboxPending).
		Order("id").Pluck("id", &sobraram)
	if len(sobraram) != 10 || sobraram[0] != recibos[20] || sobraram[9] != recibos[29] {
		t.Fatalf("deveriam sobrar os 10 recibos mais novos de A, sobraram %v", sobraram)
	}
	var descartado, entregue WebhookOutbox
	db.First(&descartado, recibos[5])
	if descartado.Status != OutboxDead || descartado.LastError != "teto de teste" {
		t.Fatalf("o mais antigo pendente deveria virar dead com o motivo, ficou %s/%q", descartado.Status, descartado.LastError)
	}
	db.First(&entregue, recibos[0])
	if entregue.Status != OutboxDelivered {
		t.Fatalf("recibo já entregue não pode ser tocado, ficou %s", entregue.Status)
	}
	// B (below the cap), A's live message and A's Contact (sync) are untouched
	var intactos int64
	db.Model(&WebhookOutbox{}).Where("status = ? AND id IN ?", OutboxPending, []uint64{b1, msg}).Count(&intactos)
	var contato int64
	db.Model(&WebhookOutbox{}).Where("status = ? AND lane = ?", OutboxPending, LaneSync).Count(&contato)
	if intactos != 2 || contato != 1 {
		t.Fatalf("o teto só pode mexer nos recibos acima do teto (intactos=%d/2, sync=%d/1)", intactos, contato)
	}
	// under the cap, nothing changes
	if n, _ := repo.TrimReceipts(10, "teto de teste"); n != 0 {
		t.Fatalf("abaixo do teto não pode descartar nada, descartou %d", n)
	}
}
