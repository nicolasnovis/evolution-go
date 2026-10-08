package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestRotearListaDeTransmissao(t *testing.T) {
	lista := types.NewJID("1700000001", types.BroadcastServer)
	daniel := types.JID{User: "100000000000001", Server: types.HiddenUserServer, Device: 0}
	danielComAparelho := types.JID{User: "100000000000001", Server: types.HiddenUserServer, Device: 12}

	t.Run("recebida por lista vira 1:1 com o remetente", func(t *testing.T) {
		info := &types.MessageInfo{MessageSource: types.MessageSource{Chat: lista, Sender: danielComAparelho}}
		got, ok := rotearListaDeTransmissao(info, false)
		if !ok || got != "1700000001@broadcast" {
			t.Fatalf("esperava rotear a lista, got=%q ok=%v", got, ok)
		}
		if info.Chat != daniel {
			t.Fatalf("Chat deveria ser o remetente sem aparelho, veio %s", info.Chat)
		}
		if info.Sender != danielComAparelho {
			t.Fatalf("Sender não pode mudar, veio %s", info.Sender)
		}
	})

	casos := []struct {
		nome string
		info types.MessageInfo
		poll bool
	}{
		{"status não é lista", types.MessageInfo{MessageSource: types.MessageSource{Chat: types.StatusBroadcastJID, Sender: daniel}}, false},
		{"envio do próprio dono por lista", types.MessageInfo{MessageSource: types.MessageSource{Chat: lista, Sender: daniel, IsFromMe: true}}, false},
		{"voto de enquete", types.MessageInfo{MessageSource: types.MessageSource{Chat: lista, Sender: daniel}}, true},
		{"conversa 1:1 normal", types.MessageInfo{MessageSource: types.MessageSource{Chat: daniel, Sender: daniel}}, false},
		{"grupo", types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("120363423759190455", types.GroupServer), Sender: daniel, IsGroup: true}}, false},
		{"lista sem remetente", types.MessageInfo{MessageSource: types.MessageSource{Chat: lista}}, false},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			info := c.info
			antes := info.Chat
			if _, ok := rotearListaDeTransmissao(&info, c.poll); ok {
				t.Fatalf("não deveria rotear")
			}
			if info.Chat != antes {
				t.Fatalf("Chat não pode mudar, veio %s", info.Chat)
			}
		})
	}
}
