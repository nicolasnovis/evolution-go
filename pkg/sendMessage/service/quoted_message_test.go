package send_service

import "testing"

func TestQuotedMessageVoiceNoteKeepsDuration(t *testing.T) {
	m := QuotedStruct{MessageID: "ABC", Type: "audio", Seconds: 20}.QuotedMessage()
	if m.GetAudioMessage() == nil || !m.GetAudioMessage().GetPTT() || m.GetAudioMessage().GetSeconds() != 20 {
		t.Fatalf("want PTT audio with 20s, got %v", m)
	}
	if m.Conversation != nil {
		t.Fatalf("voice note quote must not be an empty text")
	}
}

func TestQuotedMessageImageCarriesCaption(t *testing.T) {
	m := QuotedStruct{Type: "image", Text: "look at this"}.QuotedMessage()
	if m.GetImageMessage().GetCaption() != "look at this" {
		t.Fatalf("want caption, got %v", m)
	}
}

func TestQuotedMessageTextAndLegacyCallers(t *testing.T) {
	if got := (QuotedStruct{Type: "text", Text: "hello"}).QuotedMessage().GetConversation(); got != "hello" {
		t.Fatalf("want text, got %q", got)
	}
	// callers that only send messageId/participant keep working (same as before)
	m := QuotedStruct{MessageID: "ABC"}.QuotedMessage()
	if m.Conversation == nil || m.GetConversation() != "" {
		t.Fatalf("legacy caller should still get an empty Conversation, got %v", m)
	}
}
