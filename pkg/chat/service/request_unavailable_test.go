package chat_service

import (
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestMontarPedidoDeReenvio(t *testing.T) {
	t.Run("1:1 @lid sem sender usa o próprio chat", func(t *testing.T) {
		chat, sender, ids, err := montarPedidoDeReenvio(&RequestUnavailableStruct{Chat: "100000000000001@lid", IDs: []string{"A", " A ", "B", ""}})
		if err != nil {
			t.Fatal(err)
		}
		if chat.String() != "100000000000001@lid" || sender != chat {
			t.Fatalf("chat=%s sender=%s", chat, sender)
		}
		if len(ids) != 2 || ids[0] != "A" || ids[1] != "B" {
			t.Fatalf("ids=%v", ids)
		}
	})
	t.Run("lista de transmissão mantém o chat e usa o sender sem aparelho", func(t *testing.T) {
		chat, sender, _, err := montarPedidoDeReenvio(&RequestUnavailableStruct{Chat: "1700000001@broadcast", Sender: "100000000000001:12@lid", IDs: []string{"X"}})
		if err != nil {
			t.Fatal(err)
		}
		if chat.Server != types.BroadcastServer || sender.String() != "100000000000001@lid" {
			t.Fatalf("chat=%s sender=%s", chat, sender)
		}
	})
	invalidos := map[string]*RequestUnavailableStruct{
		"nil":         nil,
		"sem chat":    {IDs: []string{"A"}},
		"status":      {Chat: "status@broadcast", IDs: []string{"A"}},
		"newsletter":  {Chat: "1203630@newsletter", IDs: []string{"A"}},
		"sem ids":     {Chat: "5521900000007@s.whatsapp.net", IDs: []string{" ", ""}},
		"ids demais":  {Chat: "5521900000007@s.whatsapp.net", IDs: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"}},
		"sender ruim": {Chat: "5521900000007@s.whatsapp.net", Sender: "@lid", IDs: []string{"A"}},
	}
	for nome, body := range invalidos {
		t.Run(nome, func(t *testing.T) {
			if _, _, _, err := montarPedidoDeReenvio(body); !errors.Is(err, ErrPedidoDeReenvioInvalido) {
				t.Fatalf("esperava ErrPedidoDeReenvioInvalido, veio %v", err)
			}
		})
	}
}
