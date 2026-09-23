package whatsmeow_service

import (
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"google.golang.org/protobuf/proto"
)

// Janela do histórico que o celular manda no PAREAMENTO (FULL sync).
//
// O whatsmeow manda por padrão FullSyncDaysLimit/FullSyncSizeMbLimit vazios e StorageQuotaMb=10240: o
// celular decide sozinho, e na prática veio ~1 ano (pareamento do maior cliente em 04/09). Pedimos o teto:
// o WhatsApp entrega o que ele aceitar — a comunidade (mautrix-whatsapp, bridge do autor do whatsmeow)
// observa ~3 anos e diz que valor maior não quebra.
//
// Isto SÓ vai no registro do device (getRegistrationPayload). Sessões já pareadas não mudam; o volume
// maior só chega em pareamento novo — e entra pela faixa bulk do outbox, cadenciado, sem segurar o ao vivo.
const (
	fullSyncDaysLimit   = 3650   // 10 anos: "o máximo que o celular tiver"
	fullSyncSizeMbLimit = 102400 // 100 GB
	storageQuotaMb      = 102400
)

func applyFullHistoryConfig(cfg *waCompanionReg.DeviceProps_HistorySyncConfig) {
	if cfg == nil {
		return
	}
	cfg.FullSyncDaysLimit = proto.Uint32(fullSyncDaysLimit)
	cfg.FullSyncSizeMbLimit = proto.Uint32(fullSyncSizeMbLimit)
	cfg.StorageQuotaMb = proto.Uint32(storageQuotaMb)
}
