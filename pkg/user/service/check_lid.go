package user_service

import (
	"go.mau.fi/whatsmeow/types"
)

// N1 PATCH — /user/check devolve o NÚMERO, não o @lid.
//
// Para conta já migrada pro endereçamento por @lid, o IsOnWhatsApp responde `JID = <id>@lid` e o
// /user/check repassava isso como JID/RemoteJID (com LID null, porque a consulta GetLIDForPN feita
// sobre um @lid não acha nada). Quem chama o check quer o JID CANÔNICO do número pra criar o
// contato — e recebia o @lid. No Novi isso virava contato "fantasma" (wa_jid=@lid) ao lado do
// contato real, que é por onde a resposta do lead chega: histórico partido em dois (01/10: 176
// fantasmas em 6 contas).
//
// O store do whatsmeow já conhece o par (whatsmeow_lid_map). Aqui: JID @lid com par conhecido →
// JID vira o número (@s.whatsapp.net) e o @lid vai pro campo LID. Sem par no store, nada muda.
func resolverJIDdoCheck(jid types.JID, lookupPN func(lid types.JID) (types.JID, bool)) (types.JID, *string) {
	if jid.Server != types.HiddenUserServer || lookupPN == nil {
		return jid, nil
	}
	pn, ok := lookupPN(jid.ToNonAD())
	if !ok || pn.IsEmpty() || pn.Server != types.DefaultUserServer {
		return jid, nil
	}
	lid := jid.ToNonAD().String()
	return pn.ToNonAD(), &lid
}
