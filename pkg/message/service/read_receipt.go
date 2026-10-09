package message_service

import (
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	"go.mau.fi/whatsmeow/types"
)

type loteDeRecibo struct {
	sender types.JID
	ids    []types.MessageID
}

// recibosPorAutor splits a read/played receipt into one batch per message author.
//
// whatsmeow's MarkRead takes a single sender. In a 1:1 chat that's the chat itself, but in a group
// the receipt must carry the participant who wrote the messages: sending the group JID as sender
// made WhatsApp drop the receipt, so reading a group in the CRM never cleared it on the phone.
// Ids without a known author keep the old behaviour (sender = chat), in their original order.
func recibosPorAutor(ids []string, senders map[string]string, chat types.JID) []loteDeRecibo {
	var lotes []loteDeRecibo
	pos := map[types.JID]int{}
	for _, id := range ids {
		sender := chat
		if raw, ok := senders[id]; ok {
			if parsed, valid := utils.ParseJID(raw); valid {
				sender = utils.CanonicalJID(parsed).ToNonAD()
			}
		}
		i, ok := pos[sender]
		if !ok {
			i = len(lotes)
			pos[sender] = i
			lotes = append(lotes, loteDeRecibo{sender: sender})
		}
		lotes[i].ids = append(lotes[i].ids, types.MessageID(id))
	}
	return lotes
}
