package send_service

import (
	"encoding/base64"
	"testing"
)

// Prévia pronta (como o Novi manda): usa título/descrição/miniatura dados, sem buscar nada na rede.
func TestBuildLinkMessageUsesReadyPreview(t *testing.T) {
	thumb := []byte{0xFF, 0xD8, 0xFF, 0xE0, 1, 2, 3}
	ext := buildLinkMessage(&LinkStruct{
		Text:            "olha https://www.instagram.com/p/abc/",
		Url:             "https://www.instagram.com/p/abc/",
		Title:           "Leandro no Instagram",
		Description:     "Oque tem no carro?",
		JpegThumbnail:   "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(thumb),
		ThumbnailWidth:  160,
		ThumbnailHeight: 160,
	})
	if ext.GetText() != "olha https://www.instagram.com/p/abc/" || ext.GetMatchedText() != "https://www.instagram.com/p/abc/" {
		t.Fatalf("text/matched wrong: %q %q", ext.GetText(), ext.GetMatchedText())
	}
	if ext.GetTitle() != "Leandro no Instagram" || ext.GetDescription() != "Oque tem no carro?" {
		t.Fatalf("title/description wrong: %q %q", ext.GetTitle(), ext.GetDescription())
	}
	if string(ext.GetJPEGThumbnail()) != string(thumb) || ext.GetThumbnailWidth() != 160 || ext.GetThumbnailHeight() != 160 {
		t.Fatalf("thumbnail wrong: %d bytes %dx%d", len(ext.GetJPEGThumbnail()), ext.GetThumbnailWidth(), ext.GetThumbnailHeight())
	}
	if ext.GetPreviewType().String() != "NONE" {
		t.Fatalf("preview type must be NONE (not VIDEO), got %v", ext.GetPreviewType())
	}
}

// Sem link no texto: texto puro, sem prévia (e sem rede).
func TestBuildLinkMessageNoURLIsPlainText(t *testing.T) {
	ext := buildLinkMessage(&LinkStruct{Text: "sem link aqui"})
	if ext.GetText() != "sem link aqui" || ext.Title != nil || ext.MatchedText != nil {
		t.Fatalf("expected plain text, got %+v", ext)
	}
}

// Busca falhou (host inexistente): a mensagem SAI mesmo assim, só sem prévia.
func TestBuildLinkMessageFetchFailureStillSends(t *testing.T) {
	ext := buildLinkMessage(&LinkStruct{Text: "veja http://nao-existe.invalid/x"})
	if ext.GetText() != "veja http://nao-existe.invalid/x" || ext.Title != nil {
		t.Fatalf("expected text without preview, got %+v", ext)
	}
}
