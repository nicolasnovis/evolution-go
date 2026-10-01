package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// Lids e números fictícios.
func lookupFixo(m map[string]string) func(string) (string, bool) {
	return func(lid string) (string, bool) { pn, ok := m[lid]; return pn, ok }
}

// retrato = os campos comparáveis do MessageSource (BroadcastRecipients é slice → struct não compara).
type fotoDaFonte struct {
	chat, sender, senderAlt, recipientAlt types.JID
	fromMe, group                         bool
}

func retrato(i *types.MessageInfo) fotoDaFonte {
	return fotoDaFonte{i.Chat, i.Sender, i.SenderAlt, i.RecipientAlt, i.IsFromMe, i.IsGroup}
}

var mapaFixo = map[string]string{
	"111@lid": "5521900000001@s.whatsapp.net",
	"222@lid": "5521900000002@s.whatsapp.net",
	"333@lid": "5521900000009@s.whatsapp.net",
	"444@lid": "5521900000009@s.whatsapp.net",
	"999@lid": "5521900000099@s.whatsapp.net",
}

func TestPreencherAltDoLID_saidaSemNumero(t *testing.T) {
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.NewJID("111", types.HiddenUserServer), Sender: types.JID{User: "999", Device: 3, Server: types.HiddenUserServer}, IsFromMe: true,
	}}
	chatAntes, senderAntes := info.Chat, info.Sender
	preencherAltDoLID(info, lookupFixo(mapaFixo))
	if got := info.RecipientAlt.String(); got != "5521900000001@s.whatsapp.net" {
		t.Fatalf("RecipientAlt = %q", got)
	}
	if !info.SenderAlt.IsEmpty() {
		t.Fatalf("saída não pode mexer no SenderAlt (o remetente sou eu)")
	}
	if info.Chat != chatAntes || info.Sender != senderAntes {
		t.Fatalf("Chat/Sender não podem mudar: %s / %s", info.Chat, info.Sender)
	}
}

func TestPreencherAltDoLID_saidaChatComDevice(t *testing.T) {
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.JID{User: "111", Device: 7, Server: types.HiddenUserServer}, IsFromMe: true,
	}}
	preencherAltDoLID(info, lookupFixo(mapaFixo))
	if got := info.RecipientAlt.String(); got != "5521900000001@s.whatsapp.net" {
		t.Fatalf("RecipientAlt = %q", got)
	}
}

// Entrada NUNCA é tocada (o swap/decrypt de enquete dependem do Sender @lid intacto).
func TestPreencherAltDoLID_entradaSemSenderAltContinuaVazio(t *testing.T) {
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.NewJID("222", types.HiddenUserServer), Sender: types.NewJID("222", types.HiddenUserServer),
	}}
	preencherAltDoLID(info, lookupFixo(mapaFixo))
	if !info.SenderAlt.IsEmpty() || !info.RecipientAlt.IsEmpty() {
		t.Fatalf("entrada mudou: SenderAlt=%q RecipientAlt=%q", info.SenderAlt.String(), info.RecipientAlt.String())
	}
	if info.Chat.Server != types.HiddenUserServer || info.Sender.Server != types.HiddenUserServer {
		t.Fatalf("entrada: Chat/Sender mudaram")
	}
}

func TestPreencherAltDoLID_entradaComSenderAltNaoMuda(t *testing.T) {
	alt := types.NewJID("5521900000077", types.DefaultUserServer)
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.NewJID("222", types.HiddenUserServer), Sender: types.NewJID("222", types.HiddenUserServer), SenderAlt: alt,
	}}
	antes := retrato(info)
	preencherAltDoLID(info, lookupFixo(mapaFixo))
	if retrato(info) != antes {
		t.Fatalf("entrada com SenderAlt mudou: %+v", info.MessageSource)
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
		// grupo de verdade (@g.us)
		{MessageSource: types.MessageSource{Chat: types.NewJID("1203630", types.GroupServer), IsGroup: true, IsFromMe: true}},
		// status/broadcast
		{MessageSource: types.MessageSource{Chat: types.StatusBroadcastJID, IsFromMe: true}},
		// newsletter
		{MessageSource: types.MessageSource{Chat: types.NewJID("12036", types.NewsletterServer), IsFromMe: true}},
	}
	for i, info := range casos {
		antes := retrato(info)
		preencherAltDoLID(info, lookupFixo(mapaFixo))
		if retrato(info) != antes {
			t.Fatalf("caso %d mudou: RecipientAlt=%q SenderAlt=%q", i, info.RecipientAlt.String(), info.SenderAlt.String())
		}
	}
	preencherAltDoLID(nil, lookupFixo(mapaFixo)) // nil não pode panicar
}
