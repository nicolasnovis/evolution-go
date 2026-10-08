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

// N1 PATCH — número do remetente na mensagem REENVIADA pelo celular / recebida por lista.
//
// A mensagem que volta por reenvio (PLACEHOLDER_MESSAGE_RESEND, UnavailableRequestID) é remontada pelo
// whatsmeow a partir do WebMessageInfo (ParseWebMessage) e NÃO traz SenderAlt. Sem ele o swap
// @lid→número não acontece e o CRM recebe Chat=@lid: se o contato nunca teve mensagem viva (justamente
// o caso de "a 1ª mensagem se perdeu"), o Novi não acha o número e criava um contato fantasma @lid.
// A lista de transmissão já roteada pra 1:1 pode vir sem o alt também. Aqui completa o SenderAlt pelo
// store (whatsmeow_lid_map) — o swap logo depois faz o resto, igual a uma 1:1 viva.
//
// Fica de fora: saída do dono (fromMe — tem caminho próprio), grupo (o chat é o grupo) e voto de
// enquete (a chave do voto é derivada com o Sender @lid; o swap antes do decrypt quebraria o voto).
// Sem o par no store, nada muda.
func preencherAltDoRemetente(info *types.MessageInfo, aplicar, isPollUpdate bool, lookup func(lid string) (string, bool)) {
	if !aplicar || info == nil || info.IsFromMe || info.IsGroup || isPollUpdate || !info.SenderAlt.IsEmpty() || info.Sender.Server != types.HiddenUserServer {
		return
	}
	if pn, ok := lookup(info.Sender.ToNonAD().String()); ok {
		if jid, err := types.ParseJID(pn); err == nil {
			info.SenderAlt = jid
		}
	}
}
