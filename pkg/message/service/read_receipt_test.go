package message_service

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestRecibosPorAutor_GrupoUmLotePorAutor(t *testing.T) {
	grupo := types.NewJID("120363167870687836", types.GroupServer)
	lotes := recibosPorAutor(
		[]string{"A1", "B1", "A2", "SEM"},
		map[string]string{"A1": "5521999990001@s.whatsapp.net", "A2": "5521999990001:7@s.whatsapp.net", "B1": "100000000000001@lid"},
		grupo,
	)
	if len(lotes) != 3 {
		t.Fatalf("esperava 3 lotes (autor A, autor B, sem autor), veio %d: %+v", len(lotes), lotes)
	}
	if lotes[0].sender.String() != "5521999990001@s.whatsapp.net" || len(lotes[0].ids) != 2 {
		t.Errorf("lote do autor A errado: %+v", lotes[0])
	}
	if lotes[1].sender.String() != "100000000000001@lid" || lotes[1].ids[0] != "B1" {
		t.Errorf("lote do autor B errado: %+v", lotes[1])
	}
	if lotes[2].sender != grupo || lotes[2].ids[0] != "SEM" {
		t.Errorf("id sem autor devia manter sender = chat: %+v", lotes[2])
	}
}

func TestRecibosPorAutor_UmPraUmSemSenders(t *testing.T) {
	chat := types.NewJID("5521900000007", types.DefaultUserServer)
	lotes := recibosPorAutor([]string{"X", "Y"}, nil, chat)
	if len(lotes) != 1 || lotes[0].sender != chat || len(lotes[0].ids) != 2 {
		t.Fatalf("1:1 sem senders devia ser um lote só com sender = chat: %+v", lotes)
	}
}
