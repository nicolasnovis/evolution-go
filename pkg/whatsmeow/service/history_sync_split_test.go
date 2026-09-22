package whatsmeow_service

import (
	"encoding/json"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/proto"
)

// --- fixture builders -------------------------------------------------------

func mkMsg(id string, pad int) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{
		Message: &waWeb.WebMessageInfo{
			Key:     &waCommon.MessageKey{ID: proto.String(id)},
			Message: &waE2E.Message{Conversation: proto.String(strings.Repeat("x", pad))},
		},
	}
}

func mkConv(jid string, nMsgs, pad int) *waHistorySync.Conversation {
	eoh := waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY
	c := &waHistorySync.Conversation{
		ID:                       proto.String(jid),
		EndOfHistoryTransferType: &eoh,
	}
	for i := 0; i < nMsgs; i++ {
		c.Messages = append(c.Messages, mkMsg(jid+"-m"+string(rune('a'+i)), pad))
	}
	return c
}

func baseSync() *waHistorySync.HistorySync {
	st := waHistorySync.HistorySync_FULL
	return &waHistorySync.HistorySync{
		SyncType:   &st,
		Progress:   proto.Uint32(50),
		ChunkOrder: proto.Uint32(3),
	}
}

// collect all messages across the returned parts, in order.
func allMessages(parts []*waHistorySync.HistorySync) []string {
	var ids []string
	for _, p := range parts {
		for _, c := range p.GetConversations() {
			for _, m := range c.GetMessages() {
				ids = append(ids, m.GetMessage().GetKey().GetID())
			}
		}
	}
	return ids
}

// --- tests ------------------------------------------------------------------

func TestSplitHistorySync_NilOrDisabled(t *testing.T) {
	if got := splitHistorySync(nil, 1000); len(got) != 1 || got[0] != nil {
		t.Fatalf("nil payload should pass through, got %d parts", len(got))
	}
	full := baseSync()
	full.Conversations = []*waHistorySync.Conversation{mkConv("a@s.whatsapp.net", 2, 100)}
	if got := splitHistorySync(full, 0); len(got) != 1 || got[0] != full {
		t.Fatalf("maxBytes<=0 should disable splitting, got %d parts", len(got))
	}
}

func TestSplitHistorySync_NoSplitWhenUnderThreshold(t *testing.T) {
	full := baseSync()
	full.Conversations = []*waHistorySync.Conversation{mkConv("a@s.whatsapp.net", 2, 50)}
	got := splitHistorySync(full, 10_000_000)
	if len(got) != 1 || got[0] != full {
		t.Fatalf("under-threshold payload must pass through unchanged, got %d parts", len(got))
	}
}

func TestSplitHistorySync_ByConversations(t *testing.T) {
	full := baseSync()
	full.Pushnames = []*waHistorySync.Pushname{{ID: proto.String("a@s.whatsapp.net"), Pushname: proto.String("Alice")}}
	// 6 conversations, each ~600 bytes of messages; threshold small → must split into several parts.
	for i := 0; i < 6; i++ {
		full.Conversations = append(full.Conversations, mkConv(string(rune('a'+i))+"@s.whatsapp.net", 1, 500))
	}
	maxBytes := 1500
	parts := splitHistorySync(full, maxBytes)

	if len(parts) < 2 {
		t.Fatalf("expected multiple parts, got %d", len(parts))
	}

	// Every part stays under the threshold and keeps the per-chunk identity fields.
	for i, p := range parts {
		if sz := jsonSize(p); sz > maxBytes {
			t.Errorf("part %d is %d bytes, over the %d threshold", i, sz, maxBytes)
		}
		if p.GetSyncType() != waHistorySync.HistorySync_FULL || p.GetProgress() != 50 || p.GetChunkOrder() != 3 {
			t.Errorf("part %d lost identity fields: syncType=%v progress=%d chunkOrder=%d", i, p.GetSyncType(), p.GetProgress(), p.GetChunkOrder())
		}
	}

	// No conversation lost, order preserved.
	var jids []string
	for _, p := range parts {
		for _, c := range p.GetConversations() {
			jids = append(jids, c.GetID())
		}
	}
	want := []string{"a@s.whatsapp.net", "b@s.whatsapp.net", "c@s.whatsapp.net", "d@s.whatsapp.net", "e@s.whatsapp.net", "f@s.whatsapp.net"}
	if strings.Join(jids, ",") != strings.Join(want, ",") {
		t.Errorf("conversations lost/reordered:\n got %v\nwant %v", jids, want)
	}

	// Pushnames ride only on the first part (never duplicated).
	if len(parts[0].GetPushnames()) != 1 {
		t.Errorf("first part should carry the pushnames, has %d", len(parts[0].GetPushnames()))
	}
	for i := 1; i < len(parts); i++ {
		if len(parts[i].GetPushnames()) != 0 {
			t.Errorf("part %d should not carry pushnames (duplication), has %d", i, len(parts[i].GetPushnames()))
		}
	}
}

func TestSplitHistorySync_BigConversationByMessages(t *testing.T) {
	full := baseSync()
	// One conversation, 8 messages ~400 bytes each → ~3.2KB, well over the small threshold.
	full.Conversations = []*waHistorySync.Conversation{mkConv("big@s.whatsapp.net", 8, 400)}
	maxBytes := 1200
	parts := splitHistorySync(full, maxBytes)

	if len(parts) < 2 {
		t.Fatalf("a big single conversation must split by messages, got %d parts", len(parts))
	}

	// All 8 messages preserved in order.
	got := allMessages(parts)
	if len(got) != 8 {
		t.Fatalf("expected 8 messages preserved, got %d (%v)", len(got), got)
	}
	for i := 0; i < 8; i++ {
		want := "big@s.whatsapp.net-m" + string(rune('a'+i))
		if got[i] != want {
			t.Errorf("message %d out of order: got %s want %s", i, got[i], want)
		}
	}

	// Every piece is the same chat, with the conversation metadata stamped on it.
	for i, p := range parts {
		convs := p.GetConversations()
		if len(convs) != 1 {
			t.Fatalf("piece %d should carry exactly one conversation, has %d", i, len(convs))
		}
		c := convs[0]
		if c.GetID() != "big@s.whatsapp.net" {
			t.Errorf("piece %d lost conversation ID: %s", i, c.GetID())
		}
		if c.GetEndOfHistoryTransferType() != waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY {
			t.Errorf("piece %d lost endOfHistoryTransferType", i)
		}
		if p.GetSyncType() != waHistorySync.HistorySync_FULL {
			t.Errorf("piece %d lost syncType", i)
		}
	}
}

// The sliced payloads must marshal to the SAME lowercase JSON shape the CRM parses
// (data.Data.conversations[].messages[], syncType, pushnames) — proof the split stays wire-compatible.
func TestSplitHistorySync_WireShape(t *testing.T) {
	full := baseSync()
	for i := 0; i < 4; i++ {
		full.Conversations = append(full.Conversations, mkConv(string(rune('a'+i))+"@s.whatsapp.net", 1, 500))
	}
	parts := splitHistorySync(full, 1200)
	if len(parts) < 2 {
		t.Fatalf("expected split, got %d", len(parts))
	}
	b, err := json.Marshal(parts[0])
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if _, ok := m["syncType"]; !ok {
		t.Errorf("expected lowercase 'syncType' key, got keys %v", keysOf(m))
	}
	if _, ok := m["conversations"]; !ok {
		t.Errorf("expected lowercase 'conversations' key, got keys %v", keysOf(m))
	}
}

func keysOf(m map[string]interface{}) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
