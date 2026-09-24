package appstate

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestBuildDeleteForMe(t *testing.T) {
	chat := types.NewJID("5500000000001", types.DefaultUserServer)
	ts := time.Unix(1700000000, 0)

	t.Run("mensagem minha em 1:1: sender vira 0, fromMe 1", func(t *testing.T) {
		p := BuildDeleteForMe(chat, types.EmptyJID, "ABC123", true, false, ts)
		if p.Type != WAPatchRegularHigh {
			t.Fatalf("coleção %s, queria regular_high", p.Type)
		}
		m := p.Mutations[0]
		want := []string{IndexDeleteMessageForMe, chat.String(), "ABC123", "1", "0"}
		for i := range want {
			if m.Index[i] != want[i] {
				t.Fatalf("index[%d]=%q, queria %q", i, m.Index[i], want[i])
			}
		}
		if m.Version != 3 {
			t.Fatalf("version %d", m.Version)
		}
		a := m.Value.GetDeleteMessageForMeAction()
		if a.GetMessageTimestamp() != 1700000000 || a.GetDeleteMedia() {
			t.Fatalf("action %+v", a)
		}
	})

	t.Run("recebida em 1:1: sender = o próprio chat vira 0", func(t *testing.T) {
		m := BuildDeleteForMe(chat, chat, "X", false, true, ts).Mutations[0]
		if m.Index[3] != "0" || m.Index[4] != "0" {
			t.Fatalf("index %v", m.Index)
		}
		if !m.Value.GetDeleteMessageForMeAction().GetDeleteMedia() {
			t.Fatal("deleteMedia perdido")
		}
	})

	t.Run("grupo, mensagem de outra pessoa: sender é o participante", func(t *testing.T) {
		grupo := types.NewJID("120363000000000001", types.GroupServer)
		part := types.NewJID("5500000000002", types.DefaultUserServer)
		m := BuildDeleteForMe(grupo, part, "Y", false, false, ts).Mutations[0]
		if m.Index[1] != grupo.String() || m.Index[4] != part.String() {
			t.Fatalf("index %v", m.Index)
		}
	})
}
