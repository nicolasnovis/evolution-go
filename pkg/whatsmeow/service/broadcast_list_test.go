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

func TestPreencherAltDoRemetente(t *testing.T) {
	lid := types.JID{User: "100000000000001", Server: types.HiddenUserServer}
	lidComAparelho := types.JID{User: "100000000000001", Server: types.HiddenUserServer, Device: 4}
	pn := "5521900000007@s.whatsapp.net"
	lookup := lookupFixo(map[string]string{"100000000000001@lid": pn})

	t.Run("reenvio 1:1 @lid sem alt ganha o número", func(t *testing.T) {
		info := &types.MessageInfo{MessageSource: types.MessageSource{Chat: lid, Sender: lidComAparelho}}
		preencherAltDoRemetente(info, true, false, lookup)
		if info.SenderAlt.String() != pn {
			t.Fatalf("SenderAlt=%s", info.SenderAlt)
		}
		if info.Sender != lidComAparelho || info.Chat != lid {
			t.Fatalf("Sender/Chat não podem mudar aqui: %s %s", info.Sender, info.Chat)
		}
	})

	casos := []struct {
		nome    string
		info    types.MessageInfo
		aplicar bool
		poll    bool
	}{
		{"mensagem viva comum (não é reenvio nem lista)", types.MessageInfo{MessageSource: types.MessageSource{Chat: lid, Sender: lid}}, false, false},
		{"saída do dono", types.MessageInfo{MessageSource: types.MessageSource{Chat: lid, Sender: lid, IsFromMe: true}}, true, false},
		{"grupo", types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("1203", types.GroupServer), Sender: lid, IsGroup: true}}, true, false},
		{"voto de enquete", types.MessageInfo{MessageSource: types.MessageSource{Chat: lid, Sender: lid}}, true, true},
		{"já tem alt", types.MessageInfo{MessageSource: types.MessageSource{Chat: lid, Sender: lid, SenderAlt: types.NewJID("5521900000001", types.DefaultUserServer)}}, true, false},
		{"remetente já é número", types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("5521900000007", types.DefaultUserServer), Sender: types.NewJID("5521900000007", types.DefaultUserServer)}}, true, false},
		{"lid desconhecido no store", types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("777", types.HiddenUserServer), Sender: types.NewJID("777", types.HiddenUserServer)}}, true, false},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			info := c.info
			antes := info.SenderAlt
			preencherAltDoRemetente(&info, c.aplicar, c.poll, lookup)
			if info.SenderAlt != antes {
				t.Fatalf("SenderAlt não deveria mudar, veio %s", info.SenderAlt)
			}
		})
	}
}
