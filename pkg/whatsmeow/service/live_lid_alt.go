package whatsmeow_service

import (
	"go.mau.fi/whatsmeow/types"
)

// N1 PATCH — número real no evento AO VIVO de conversa @lid.
//
// O WhatsApp vem endereçando conversa 1-a-1 por @lid. Quando o PRÓPRIO dono manda mensagem pelo
// celular/WhatsApp Web pra um contato @lid, o evento chega com Chat=@lid e, às vezes, SEM o
// RecipientAlt (o número) — o WhatsApp não mandou o `peer_recipient_pn` no nó. O store do whatsmeow
// quase sempre JÁ conhece o par (whatsmeow_lid_map), mas o webhook não levava: o Novi recebia um
// @lid que nunca viu e descartava a mensagem ("lid-drop"). 01/10: 12 mensagens do um contato em
// 3 conversas sumiram assim.
//
// Aqui o par vai junto: saída → RecipientAlt; entrada sem SenderAlt → SenderAlt (o swap logo depois
// já põe o número no Chat/Sender, como sempre fez quando o WhatsApp manda o alt). Sem o par no store,
// nada muda (o comportamento de antes).
func preencherAltDoLID(info *types.MessageInfo, lookup func(lid string) (string, bool)) {
	if info == nil || info.IsGroup || info.Chat.Server != types.HiddenUserServer {
		return
	}
	if info.IsFromMe {
		if info.RecipientAlt.IsEmpty() {
			if pn, ok := lookup(info.Chat.ToNonAD().String()); ok {
				if jid, err := types.ParseJID(pn); err == nil {
					info.RecipientAlt = jid
				}
			}
		}
		return
	}
	if info.SenderAlt.IsEmpty() && info.Sender.Server == types.HiddenUserServer {
		if pn, ok := lookup(info.Sender.ToNonAD().String()); ok {
			if jid, err := types.ParseJID(pn); err == nil {
				info.SenderAlt = jid
			}
		}
	}
}
