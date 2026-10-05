package send_service

import (
	"encoding/json"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestQuotedContextInfoPrivateReplyCarriesGroup(t *testing.T) {
	var q QuotedStruct
	body := `{"messageId":"3EB0ABC","participant":"5511999990000@s.whatsapp.net","remoteJid":"120363000000000001@g.us","type":"text","text":"oi grupo"}`
	if err := json.Unmarshal([]byte(body), &q); err != nil {
		t.Fatal(err)
	}
	chat := types.NewJID("5511888880000", types.DefaultUserServer)
	ci := q.ContextInfo(chat)
	if ci.GetRemoteJID() != "120363000000000001@g.us" {
		t.Fatalf("want group remoteJid, got %q", ci.GetRemoteJID())
	}
	if ci.GetStanzaID() != "3EB0ABC" || ci.GetParticipant() != "5511999990000@s.whatsapp.net" {
		t.Fatalf("stanza/participant lost: %v", ci)
	}
	if ci.GetQuotedMessage().GetConversation() != "oi grupo" {
		t.Fatalf("quoted snapshot lost: %v", ci.GetQuotedMessage())
	}
}

func TestQuotedContextInfoWithoutRemoteJidIsUnchanged(t *testing.T) {
	q := QuotedStruct{MessageID: "ABC", Participant: "5511@s.whatsapp.net"}
	ci := q.ContextInfo(types.NewJID("5511888880000", types.DefaultUserServer))
	if ci.RemoteJID != nil {
		t.Fatalf("no remoteJid sent must keep RemoteJID nil, got %q", ci.GetRemoteJID())
	}
	if ci.StanzaID == nil || ci.GetStanzaID() != "ABC" || ci.Participant == nil || ci.GetParticipant() != "5511@s.whatsapp.net" {
		t.Fatalf("legacy fields changed: %v", ci)
	}
	if ci.QuotedMessage == nil || ci.QuotedMessage.Conversation == nil || ci.GetQuotedMessage().GetConversation() != "" {
		t.Fatalf("legacy quoted snapshot changed: %v", ci.GetQuotedMessage())
	}
	// whitespace-only behaves like absent
	if (QuotedStruct{MessageID: "ABC", RemoteJid: "  "}).ContextInfo(types.JID{}).RemoteJID != nil {
		t.Fatal("blank remoteJid must be ignored")
	}
}

func TestQuotedContextInfoSameChatOmitsRemoteJid(t *testing.T) {
	group := types.NewJID("120363000000000001", types.GroupServer)
	q := QuotedStruct{MessageID: "ABC", Participant: "5511@s.whatsapp.net", RemoteJid: "120363000000000001@g.us"}
	if ci := q.ContextInfo(group); ci.RemoteJID != nil {
		t.Fatalf("in-chat reply must not carry remoteJid, got %q", ci.GetRemoteJID())
	}
	// unknown destination (carousel path): keep what the caller sent
	if got := q.quotedRemoteJID(types.JID{}); got != "120363000000000001@g.us" {
		t.Fatalf("want remoteJid kept with unknown chat, got %q", got)
	}
}

func TestQuotedRemoteJidOptionalInJSON(t *testing.T) {
	var q QuotedStruct
	if err := json.Unmarshal([]byte(`{"messageId":"X","participant":"p"}`), &q); err != nil {
		t.Fatal(err)
	}
	if q.RemoteJid != "" {
		t.Fatalf("absent remoteJid must stay empty, got %q", q.RemoteJid)
	}
	out, _ := json.Marshal(q)
	if string(out) != `{"messageId":"X","participant":"p"}` {
		t.Fatalf("omitempty broke the JSON shape: %s", out)
	}
}
