package send_service

import (
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// QuotedMessage builds the quoted-message snapshot embedded in a reply's ContextInfo.
//
// WhatsApp clients render the reply preview from this embedded copy, not by looking the original
// message up. Sending an empty `Conversation: ""` (what every reply used to carry) makes the
// recipient's app show a blank quote — e.g. a reply to a voice note appeared as an empty box instead
// of "🎤 Voice message (0:20)". The caller now sends the quoted message's type, text/caption and
// audio duration, and this rebuilds a minimal message of the right kind.
func (q QuotedStruct) QuotedMessage() *waE2E.Message {
	text := strings.TrimSpace(q.Text)
	switch strings.ToLower(q.Type) {
	case "audio", "ptt":
		m := &waE2E.AudioMessage{PTT: proto.Bool(true)}
		if q.Seconds > 0 {
			m.Seconds = proto.Uint32(q.Seconds)
		}
		return &waE2E.Message{AudioMessage: m}
	case "image":
		m := &waE2E.ImageMessage{}
		if text != "" {
			m.Caption = proto.String(text)
		}
		return &waE2E.Message{ImageMessage: m}
	case "video":
		m := &waE2E.VideoMessage{}
		if text != "" {
			m.Caption = proto.String(text)
		}
		return &waE2E.Message{VideoMessage: m}
	case "document":
		m := &waE2E.DocumentMessage{}
		if text != "" {
			m.Caption = proto.String(text)
		}
		return &waE2E.Message{DocumentMessage: m}
	case "sticker":
		return &waE2E.Message{StickerMessage: &waE2E.StickerMessage{}}
	default:
		// text, or an older caller that only sends messageId/participant (Text stays "")
		return &waE2E.Message{Conversation: proto.String(text)}
	}
}
