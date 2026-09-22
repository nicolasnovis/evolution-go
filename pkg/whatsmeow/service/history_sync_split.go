package whatsmeow_service

import (
	"encoding/json"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/proto"
)

// HistorySync slicing (item 2b da janela F3 do engine).
//
// Problema: um chunk de HistorySync grande estoura o limite de corpo da plataforma que recebe o
// webhook (413 na Vercel acima de ~4,5MB — medido: p90 5,2MB, máx 6,5MB, 12% dos chunks). O evento
// morre e o reconciliador do Novi fica sem a conversa. O outbox (item 2) não resolve: 413 é erro
// permanente → vira dead-letter. A cura é fatiar o payload NO engine, antes de despachar.
//
// Como: se o JSON do HistorySync passa do teto (WEBHOOK_MAX_PAYLOAD_BYTES, default 3.500.000), ele é
// quebrado em N eventos HistorySync menores — cada um com um subconjunto de Conversations, e, pra uma
// conversa gigante sozinha, um subconjunto de Messages. Todos preservam SyncType/Progress/ChunkOrder
// (os campos que o CRM lê por chunk); os campos não-conversa (pushnames, statusV3, globalSettings, …)
// vão SÓ no primeiro chunk, pra não re-enviar em cada parte. O Novi já ingere dezenas de chunks por
// sync e é idempotente por chunk (registrarChunkDeHistorico), então o fatiamento é transparente.

// jsonSize returns the encoding/json byte length of v — the SAME marshaler the webhook dispatch
// uses, so the measure matches the real body. On marshal error returns 0, which makes the caller
// treat the payload as "fits" and fall back to a single dispatch: we never silently emit an
// oversized split we couldn't measure.
func jsonSize(v interface{}) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// identitySkeleton builds an empty HistorySync carrying only the per-chunk identity fields
// (SyncType/Progress/ChunkOrder). Those pointers are shared read-only across parts — never mutated.
func identitySkeleton(full *waHistorySync.HistorySync) *waHistorySync.HistorySync {
	return &waHistorySync.HistorySync{
		SyncType:   full.SyncType,
		Progress:   full.Progress,
		ChunkOrder: full.ChunkOrder,
	}
}

// splitHistorySync divides a HistorySync payload whose JSON exceeds maxBytes into an ordered list of
// smaller HistorySync payloads, each aiming to stay under maxBytes. Returns a single-element slice
// (the original) when no split is needed — or when there are no conversations to slice (a lone
// oversized status/settings blob is left whole: the platform rejects it, we never drop it).
func splitHistorySync(full *waHistorySync.HistorySync, maxBytes int) []*waHistorySync.HistorySync {
	if full == nil || maxBytes <= 0 {
		return []*waHistorySync.HistorySync{full}
	}
	if jsonSize(full) <= maxBytes {
		return []*waHistorySync.HistorySync{full}
	}
	convs := full.GetConversations()
	if len(convs) == 0 {
		return []*waHistorySync.HistorySync{full}
	}

	// Part 0 keeps every non-conversation field. Build it by detaching Conversations from `full`
	// before cloning (so we don't deep-clone megabytes of messages just to drop them), then restore.
	// Safe: this runs on the single event-handler goroutine, and `full` is restored before dispatch.
	savedConvs := full.Conversations
	full.Conversations = nil
	meta := proto.Clone(full).(*waHistorySync.HistorySync)
	full.Conversations = savedConvs
	meta.Conversations = nil

	lightBase := jsonSize(identitySkeleton(full))

	parts := make([]*waHistorySync.HistorySync, 0, 4)
	current := meta
	currentSize := jsonSize(current)

	// A part is worth emitting if it carries conversations, or (part 0) non-conversation payload
	// beyond the bare identity fields (pushnames/status/settings that must not be dropped).
	partHasContent := func(p *waHistorySync.HistorySync) bool {
		if len(p.Conversations) > 0 {
			return true
		}
		return jsonSize(p) > lightBase
	}
	flush := func() {
		if partHasContent(current) {
			parts = append(parts, current)
		}
	}

	for _, conv := range convs {
		convSize := jsonSize(conv)

		// This conversation can't fit even in an empty part → split it by its Messages.
		if lightBase+convSize > maxBytes {
			flush()
			parts = append(parts, splitConversation(conv, full, maxBytes)...)
			current = identitySkeleton(full)
			currentSize = lightBase
			continue
		}

		// Adding it would overflow the current part → close it and open a fresh light part.
		if partHasContent(current) && currentSize+convSize > maxBytes {
			flush()
			current = identitySkeleton(full)
			currentSize = lightBase
		}
		current.Conversations = append(current.Conversations, conv)
		currentSize += convSize
	}
	flush()

	if len(parts) == 0 {
		return []*waHistorySync.HistorySync{full}
	}
	return parts
}

// splitConversation breaks one oversized conversation into ordered HistorySync pieces, each a copy of
// the conversation carrying a subset of its Messages plus all of its own metadata (ID,
// endOfHistoryTransferType, …) so the CRM ingests every piece as the same chat. Message order is
// preserved. If a single message still overflows a piece on its own, it goes out alone (we never drop
// a message; an oversized lone message is the platform's to reject).
func splitConversation(conv *waHistorySync.Conversation, full *waHistorySync.HistorySync, maxBytes int) []*waHistorySync.HistorySync {
	msgs := conv.GetMessages()

	// Conversation metadata WITHOUT messages, cloned once — both to measure its base cost and to
	// stamp every piece with the same ID/endOfHistoryTransferType/etc.
	savedMsgs := conv.Messages
	conv.Messages = nil
	metaConv := proto.Clone(conv).(*waHistorySync.Conversation)
	conv.Messages = savedMsgs
	metaConv.Messages = nil

	lightBase := jsonSize(identitySkeleton(full))
	convBase := jsonSize(metaConv)
	budget := maxBytes - lightBase - convBase // room for messages in one piece

	pieces := make([]*waHistorySync.HistorySync, 0, 2)
	flush := func(batch []*waHistorySync.HistorySyncMsg) {
		if len(batch) == 0 {
			return
		}
		c := proto.Clone(metaConv).(*waHistorySync.Conversation)
		c.Messages = batch
		h := identitySkeleton(full)
		h.Conversations = []*waHistorySync.Conversation{c}
		pieces = append(pieces, h)
	}

	batch := make([]*waHistorySync.HistorySyncMsg, 0, len(msgs))
	batchSize := 0
	for _, m := range msgs {
		ms := jsonSize(m)
		if len(batch) > 0 && (budget <= 0 || batchSize+ms > budget) {
			flush(batch)
			batch = make([]*waHistorySync.HistorySyncMsg, 0, len(msgs))
			batchSize = 0
		}
		batch = append(batch, m)
		batchSize += ms
	}
	flush(batch)

	// Conversation was oversized but carries no messages (bloated metadata) → send it whole once.
	if len(pieces) == 0 {
		h := identitySkeleton(full)
		h.Conversations = []*waHistorySync.Conversation{conv}
		pieces = append(pieces, h)
	}
	return pieces
}
