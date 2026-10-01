package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// Lids e números fictícios.
func lookupFixo(m map[string]string) func(string) (string, bool) {
	return func(lid string) (string, bool) { pn, ok := m[lid]; return pn, ok }
}

func TestPreencherAltDoLID_saidaSemNumero(t *testing.T) {
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.NewJID("111", types.HiddenUserServer), Sender: types.NewJID("999", types.HiddenUserServer), IsFromMe: true,
	}}
	preencherAltDoLID(info, lookupFixo(map[string]string{"111@lid": "5521900000001@s.whatsapp.net"}))
	if got := info.RecipientAlt.String(); got != "5521900000001@s.whatsapp.net" {
		t.Fatalf("RecipientAlt = %q", got)
	}
	if !info.SenderAlt.IsEmpty() {
		t.Fatalf("saída não pode mexer no SenderAlt (o remetente sou eu)")
	}
}

func TestPreencherAltDoLID_entradaSemNumero(t *testing.T) {
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.NewJID("222", types.HiddenUserServer), Sender: types.NewJID("222", types.HiddenUserServer),
	}}
	preencherAltDoLID(info, lookupFixo(map[string]string{"222@lid": "5521900000002@s.whatsapp.net"}))
	if got := info.SenderAlt.String(); got != "5521900000002@s.whatsapp.net" {
		t.Fatalf("SenderAlt = %q", got)
	}
}

func TestPreencherAltDoLID_naoMexe(t *testing.T) {
	ja := types.NewJID("5521900000003", types.DefaultUserServer)
	casos := []*types.MessageInfo{
		// já veio com o número
		{MessageSource: types.MessageSource{Chat: types.NewJID("333", types.HiddenUserServer), IsFromMe: true, RecipientAlt: ja}},
		// conversa já em número
		{MessageSource: types.MessageSource{Chat: ja, IsFromMe: true}},
		// grupo
		{MessageSource: types.MessageSource{Chat: types.NewJID("444", types.HiddenUserServer), IsGroup: true, IsFromMe: true}},
		// par desconhecido no store
		{MessageSource: types.MessageSource{Chat: types.NewJID("555", types.HiddenUserServer), IsFromMe: true}},
	}
	for i, info := range casos {
		antes := info.RecipientAlt
		preencherAltDoLID(info, lookupFixo(map[string]string{"333@lid": "5521900000009@s.whatsapp.net", "444@lid": "5521900000009@s.whatsapp.net"}))
		if info.RecipientAlt != antes {
			t.Fatalf("caso %d: RecipientAlt mudou pra %q", i, info.RecipientAlt.String())
		}
	}
}
