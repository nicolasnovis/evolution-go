package send_service

import (
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
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

// ContextInfo builds the reply context for a message sent to chat: StanzaID, Participant and the
// quoted snapshot (exactly what every send path used to inline), plus RemoteJID when the quoted
// message lives in ANOTHER chat — the "Reply privately" case, where a 1:1 message quotes a group
// message. With RemoteJID the recipient's app tags the quote with the group and tapping it opens the
// original; without it the quote looks like it came from the 1:1 chat itself.
func (q QuotedStruct) ContextInfo(chat types.JID) *waE2E.ContextInfo {
	ci := &waE2E.ContextInfo{
		StanzaID:      proto.String(q.MessageID),
		Participant:   proto.String(q.Participant),
		QuotedMessage: q.QuotedMessage(),
	}
	if rj := q.quotedRemoteJID(chat); rj != "" {
		ci.RemoteJID = proto.String(rj)
	}
	return ci
}

// quotedRemoteJID returns the chat to put in ContextInfo.RemoteJID, or "" when the quote belongs to
// the destination chat itself (WhatsApp omits remoteJid for in-chat replies). chat may be the zero
// JID when the caller does not know the destination yet.
func (q QuotedStruct) quotedRemoteJID(chat types.JID) string {
	rj := strings.TrimSpace(q.RemoteJid)
	if rj == "" {
		return ""
	}
	if !chat.IsEmpty() && (rj == chat.String() || rj == chat.ToNonAD().String()) {
		return ""
	}
	return rj
}
