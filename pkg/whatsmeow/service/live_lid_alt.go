package whatsmeow_service

import (
	"go.mau.fi/whatsmeow/types"
)

// N1 PATCH — número real no evento AO VIVO de SAÍDA pra conversa @lid.
//
// O WhatsApp vem endereçando conversa 1-a-1 por @lid. Quando o PRÓPRIO dono manda mensagem pelo
// celular/WhatsApp Web pra um contato @lid, o evento chega com Chat=@lid e, às vezes, SEM o
// RecipientAlt (o número) — o WhatsApp não mandou o `peer_recipient_pn` no nó. O store do whatsmeow
// quase sempre JÁ conhece o par (whatsmeow_lid_map), mas o webhook não levava: o Novi recebia um
// @lid que nunca viu e descartava a mensagem ("lid-drop"). 01/10: 12 mensagens do um contato em
// 3 conversas sumiram assim.
//
// SÓ SAÍDA (fromMe), de propósito: aqui só o RecipientAlt é preenchido — Chat/Sender ficam como
// vieram, então o swap LID/JID logo depois e os decrypts (edição/enquete) não mudam em nada.
// A ENTRADA sem SenderAlt NÃO é tocada: preencher o SenderAlt dispararia o swap (Chat/Sender viram
// número) ANTES do DecryptPollVote, que deriva a chave com o Sender @lid → voto de enquete quebraria
// (mesma classe do bug de MAC da edição). Do lado do Novi essa entrada já resolve por contacts.lid.
// Sem o par no store, nada muda (o comportamento de antes).
func preencherAltDoLID(info *types.MessageInfo, lookup func(lid string) (string, bool)) {
	if info == nil || info.IsGroup || !info.IsFromMe || info.Chat.Server != types.HiddenUserServer || !info.RecipientAlt.IsEmpty() {
		return
	}
	if pn, ok := lookup(info.Chat.ToNonAD().String()); ok {
		if jid, err := types.ParseJID(pn); err == nil {
			info.RecipientAlt = jid
		}
	}
}
