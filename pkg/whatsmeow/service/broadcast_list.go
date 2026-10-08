package whatsmeow_service

import (
	"go.mau.fi/whatsmeow/types"
)

// N1 PATCH — mensagem recebida por LISTA DE TRANSMISSÃO entra como 1:1.
//
// Quando um contato manda algo pela lista de transmissão dele, o evento chega com
// Chat=<n>@broadcast e Sender=quem mandou. No celular do dono ela aparece na conversa 1:1 com o
// remetente — e o CRM tem que mostrar igual. Antes, o filtro de "ignorar status" testava
// Contains("@broadcast") e descartava essas mensagens em silêncio (incidente cliente A, 08/10: 42
// mensagens de 13 contatos nunca chegaram ao Novi).
//
// Só troca o Chat pelo remetente (sem aparelho). Sender/SenderAlt ficam como vieram, então o swap
// @lid→número logo depois trata exatamente como uma 1:1 recebida normal. Fica de fora:
//   - status@broadcast (não é lista);
//   - envio do PRÓPRIO dono por lista (IsFromMe): o destino são vários contatos, não um;
//   - voto de enquete (pollUpdate): a chave do voto é derivada com o Chat da enquete (a lista);
//     trocar o Chat quebraria o decrypt.
//
// Devolve o JID da lista original e true quando roteou.
func rotearListaDeTransmissao(info *types.MessageInfo, isPollUpdate bool) (string, bool) {
	if info == nil || !info.Chat.IsBroadcastList() || info.IsFromMe || isPollUpdate || info.Sender.IsEmpty() {
		return "", false
	}
	list := info.Chat.String()
	info.Chat = info.Sender.ToNonAD()
	return list, true
}
