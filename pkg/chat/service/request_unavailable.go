package chat_service

import (
	"errors"
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow/types"
)

// ErrPedidoDeReenvioInvalido = corpo do /chat/request-unavailable fora do contrato (400, não 500).
var ErrPedidoDeReenvioInvalido = errors.New("pedido de reenvio inválido")

// MaxIDsPorPedidoDeReenvio limita quantas mensagens um único POST pede ao celular. O Novi manda
// poucas por vez (as de uma leitura); o teto só impede um corpo absurdo de virar rajada no aparelho.
const MaxIDsPorPedidoDeReenvio = 10

// montarPedidoDeReenvio valida e normaliza o corpo do /chat/request-unavailable. O chat vai como o
// WhatsApp mandou no recibo (@lid, número, grupo ou lista <n>@broadcast) — é a chave com que o celular
// guardou a mensagem; NÃO normaliza número aqui. sender = quem mandou a mensagem (vazio em 1:1 = o
// próprio chat). IDs: sem vazios nem repetidos, no máximo MaxIDsPorPedidoDeReenvio.
func montarPedidoDeReenvio(data *RequestUnavailableStruct) (types.JID, types.JID, []string, error) {
	if data == nil {
		return types.EmptyJID, types.EmptyJID, nil, fmt.Errorf("%w: corpo vazio", ErrPedidoDeReenvioInvalido)
	}
	chat, err := types.ParseJID(strings.TrimSpace(data.Chat))
	if err != nil || chat.IsEmpty() || chat.User == "" {
		return types.EmptyJID, types.EmptyJID, nil, fmt.Errorf("%w: chat %q", ErrPedidoDeReenvioInvalido, data.Chat)
	}
	if chat == types.StatusBroadcastJID || chat.Server == types.NewsletterServer {
		return types.EmptyJID, types.EmptyJID, nil, fmt.Errorf("%w: chat %q não aceita reenvio", ErrPedidoDeReenvioInvalido, data.Chat)
	}
	sender := chat.ToNonAD()
	if s := strings.TrimSpace(data.Sender); s != "" {
		parsed, err := types.ParseJID(s)
		if err != nil || parsed.User == "" {
			return types.EmptyJID, types.EmptyJID, nil, fmt.Errorf("%w: sender %q", ErrPedidoDeReenvioInvalido, data.Sender)
		}
		sender = parsed.ToNonAD()
	}
	vistos := make(map[string]bool, len(data.IDs))
	ids := make([]string, 0, len(data.IDs))
	for _, raw := range data.IDs {
		id := strings.TrimSpace(raw)
		if id == "" || vistos[id] {
			continue
		}
		vistos[id] = true
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return types.EmptyJID, types.EmptyJID, nil, fmt.Errorf("%w: nenhum id", ErrPedidoDeReenvioInvalido)
	}
	if len(ids) > MaxIDsPorPedidoDeReenvio {
		return types.EmptyJID, types.EmptyJID, nil, fmt.Errorf("%w: %d ids (máximo %d)", ErrPedidoDeReenvioInvalido, len(ids), MaxIDsPorPedidoDeReenvio)
	}
	return chat.ToNonAD(), sender, ids, nil
}
