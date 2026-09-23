package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/proto"
)

// Números e lids fictícios.
func conv(id string, pn string, remoteJIDs ...string) *waHistorySync.Conversation {
	c := &waHistorySync.Conversation{ID: proto.String(id)}
	if pn != "" {
		c.PnJID = proto.String(pn)
	}
	for _, r := range remoteJIDs {
		c.Messages = append(c.Messages, &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{RemoteJID: proto.String(r)},
		}})
	}
	return c
}

func TestBuildLIDMap_fontesNaOrdem(t *testing.T) {
	hs := &waHistorySync.HistorySync{
		PhoneNumberToLidMappings: []*waHistorySync.PhoneNumberToLIDMapping{
			{LidJID: proto.String("111@lid"), PnJID: proto.String("5521900000001@s.whatsapp.net")},
		},
		Conversations: []*waHistorySync.Conversation{
			conv("111@lid", "", "111@lid"),                             // resolve pelo par do chunk
			conv("222@lid", "5521900000002@s.whatsapp.net", "222@lid"), // resolve pelo PnJID da conversa
			conv("333@lid", ""),                                        // resolve pelo store
			conv("444@lid", ""),                                        // ninguém sabe → fica de fora
			conv("5521900000005@s.whatsapp.net", "", "5521900000005@s.whatsapp.net"), // já é número
			conv("120363000000000001@g.us", "", "120363000000000001@g.us"),           // grupo
		},
	}
	storeCalls := 0
	got := buildLIDMap(hs, func(lid string) (string, bool) {
		storeCalls++
		if lid == "333@lid" {
			return "5521900000003@s.whatsapp.net", true
		}
		return "", false
	})
	want := map[string]string{
		"111@lid": "5521900000001@s.whatsapp.net",
		"222@lid": "5521900000002@s.whatsapp.net",
		"333@lid": "5521900000003@s.whatsapp.net",
	}
	if len(got) != len(want) {
		t.Fatalf("mapa = %v, quero %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s → %q, quero %q", k, got[k], v)
		}
	}
	// o store só é consultado pro que o chunk não resolveu (333 e 444)
	if storeCalls != 2 {
		t.Errorf("store consultado %d vezes, quero 2", storeCalls)
	}
}

func TestBuildLIDMap_semLid(t *testing.T) {
	hs := &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{conv("5521900000005@s.whatsapp.net", "")}}
	if m := buildLIDMap(hs, func(string) (string, bool) { t.Fatal("não devia consultar o store"); return "", false }); m != nil {
		t.Errorf("sem @lid o mapa deveria ser nil, veio %v", m)
	}
}

func TestBuildLIDMap_storeDevolveLixo(t *testing.T) {
	hs := &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{conv("111@lid", "")}}
	if m := buildLIDMap(hs, func(string) (string, bool) { return "111@lid", true }); m != nil {
		t.Errorf("só número (@s.whatsapp.net) pode entrar no mapa, veio %v", m)
	}
}

func TestLidMapForPart_recortaPorParte(t *testing.T) {
	full := map[string]string{"111@lid": "5521900000001@s.whatsapp.net", "222@lid": "5521900000002@s.whatsapp.net"}
	part := &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{conv("222@lid", "")}}
	got := lidMapForPart(full, part)
	if len(got) != 1 || got["222@lid"] != "5521900000002@s.whatsapp.net" {
		t.Errorf("a parte só deveria levar o 222, levou %v", got)
	}
	if lidMapForPart(full, &waHistorySync.HistorySync{}) != nil {
		t.Error("parte sem @lid não leva mapa")
	}
}
