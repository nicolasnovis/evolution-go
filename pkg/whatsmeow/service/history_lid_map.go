package whatsmeow_service

import (
	"strings"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
)

// Mapa @lid → número que viaja JUNTO com cada chunk de HistorySync (campo `lidMap` do envelope).
//
// Problema: o WhatsApp vem migrando conversa 1-a-1 pra @lid, e no histórico de um pareamento 40–80% das
// conversas chegam com ID @lid, sem o número. O CRM só consegue gravar a conversa quando sabe o número —
// sem ele a conversa inteira cai fora. O engine SABE o número: o próprio chunk traz os pares
// (PhoneNumberToLidMappings), a conversa às vezes traz PnJID, e o store do whatsmeow tem o mapa acumulado.
//
// Por que um mapa ao lado e NÃO reescrever o ID dentro do chunk: o whatsmeow grava os dados do chunk
// (segredos de mensagem por chat, pares lid/pn) numa goroutine PARALELA ao despacho do evento
// (DownloadHistorySync com synchronousStorage=false). Mexer no proto aqui seria corrida de dados com essa
// gravação — e pelo mesmo motivo o par deste chunk pode ainda não estar no store; por isso o chunk é a
// primeira fonte, o store só a última.

const lidServer = "@lid"

// historyLIDs lista (sem repetir) os @lid de conversa 1-a-1 que aparecem no chunk: ID da conversa e
// remoteJID das mensagens. Autor de mensagem de grupo (participant) fica de fora — o CRM guarda o autor
// de grupo como veio, e o jid do grupo é @g.us.
func historyLIDs(hs *waHistorySync.HistorySync) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(j string) {
		if strings.HasSuffix(j, lidServer) && !seen[j] {
			seen[j] = true
			out = append(out, j)
		}
	}
	for _, c := range hs.GetConversations() {
		add(c.GetID())
		for _, m := range c.GetMessages() {
			add(m.GetMessage().GetKey().GetRemoteJID())
		}
	}
	return out
}

// buildLIDMap resolve os @lid do chunk, nesta ordem: pares do próprio chunk → PnJID da conversa → store
// (lookup). Só entra quem resolveu pra um número (@s.whatsapp.net); o resto o CRM trata como hoje.
func buildLIDMap(hs *waHistorySync.HistorySync, lookup func(lid string) (string, bool)) map[string]string {
	lids := historyLIDs(hs)
	if len(lids) == 0 {
		return nil
	}
	known := make(map[string]string)
	for _, p := range hs.GetPhoneNumberToLidMappings() {
		if l, pn := p.GetLidJID(), p.GetPnJID(); l != "" && pn != "" {
			known[l] = pn
		}
	}
	for _, c := range hs.GetConversations() {
		if id, pn := c.GetID(), c.GetPnJID(); strings.HasSuffix(id, lidServer) && pn != "" {
			known[id] = pn
		}
	}

	out := make(map[string]string, len(lids))
	for _, l := range lids {
		pn, ok := known[l]
		if !ok && lookup != nil {
			pn, ok = lookup(l)
		}
		if ok && strings.HasSuffix(pn, "@s.whatsapp.net") {
			out[l] = pn
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// lidMapForPart recorta o mapa do chunk inteiro pros @lid de UMA parte fatiada — cada envelope leva só o
// que usa (o mapa de um FULL grande tem milhares de pares).
func lidMapForPart(full map[string]string, part *waHistorySync.HistorySync) map[string]string {
	if len(full) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, l := range historyLIDs(part) {
		if pn, ok := full[l]; ok {
			out[l] = pn
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
