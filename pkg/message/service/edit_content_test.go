package message_service

import "testing"

func TestEditContent(t *testing.T) {
	if m := editContent("", "oi"); m.GetExtendedTextMessage().GetText() != "oi" {
		t.Fatalf("texto: %v", m)
	}
	if m := editContent("image", "legenda"); m.GetImageMessage().GetCaption() != "legenda" || m.GetExtendedTextMessage() != nil {
		t.Fatalf("foto: %v", m)
	}
	if m := editContent("video", "v"); m.GetVideoMessage().GetCaption() != "v" {
		t.Fatalf("vídeo: %v", m)
	}
	if m := editContent("document", "d"); m.GetDocumentMessage().GetCaption() != "d" {
		t.Fatalf("documento: %v", m)
	}
}
