package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"google.golang.org/protobuf/proto"
)

func TestApplyFullHistoryConfig(t *testing.T) {
	cfg := &waCompanionReg.DeviceProps_HistorySyncConfig{
		StorageQuotaMb:                proto.Uint32(10240),
		InlineInitialPayloadInE2EeMsg: proto.Bool(true),
	}
	applyFullHistoryConfig(cfg)
	if cfg.GetFullSyncDaysLimit() != 3650 || cfg.GetFullSyncSizeMbLimit() != 102400 || cfg.GetStorageQuotaMb() != 102400 {
		t.Fatalf("janela não aplicada: %+v", cfg)
	}
	// não mexe no resto do que o whatsmeow anuncia
	if !cfg.GetInlineInitialPayloadInE2EeMsg() {
		t.Error("apagou uma flag que não era dele")
	}
	applyFullHistoryConfig(nil) // não pode panicar
}
