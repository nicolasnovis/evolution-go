package user_service

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestResolverJIDdoCheck(t *testing.T) {
	lid := types.NewJID("146102623969530", types.HiddenUserServer)
	pn := types.NewJID("554100000003", types.DefaultUserServer)
	store := func(l types.JID) (types.JID, bool) {
		if l == lid {
			return pn, true
		}
		return types.EmptyJID, false
	}

	t.Run("@lid com par no store vira o número e o @lid vai pro LID", func(t *testing.T) {
		got, gotLid := resolverJIDdoCheck(lid, store)
		if got != pn {
			t.Fatalf("JID = %v, quer %v", got, pn)
		}
		if gotLid == nil || *gotLid != "146102623969530@lid" {
			t.Fatalf("LID = %v", gotLid)
		}
	})

	t.Run("@lid sem par no store fica como veio", func(t *testing.T) {
		outro := types.NewJID("999", types.HiddenUserServer)
		got, gotLid := resolverJIDdoCheck(outro, store)
		if got != outro || gotLid != nil {
			t.Fatalf("mudou: %v %v", got, gotLid)
		}
	})

	t.Run("número já canônico não é tocado", func(t *testing.T) {
		got, gotLid := resolverJIDdoCheck(pn, store)
		if got != pn || gotLid != nil {
			t.Fatalf("mudou: %v %v", got, gotLid)
		}
	})

	t.Run("store que devolve algo que não é número é ignorado", func(t *testing.T) {
		ruim := func(types.JID) (types.JID, bool) { return lid, true }
		got, gotLid := resolverJIDdoCheck(lid, ruim)
		if got != lid || gotLid != nil {
			t.Fatalf("mudou: %v %v", got, gotLid)
		}
	})
}
