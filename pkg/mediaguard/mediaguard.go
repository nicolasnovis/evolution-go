// Package mediaguard bounds media-download memory across the whole process.
//
// Motivo (incidente 2026-09-21): baixar mídia materializa o arquivo inteiro em RAM várias vezes
// (ciphertext + plaintext + base64/JSON, e sticker soma decode RGBA + PNG). O whatsmeow serializa
// o download POR INSTÂNCIA, mas com N instâncias os downloads acontecem em PARALELO, e um arquivo
// grande (ex.: vídeo de 145 MB) vira um pico de ~700 MB. Vários ao mesmo tempo estouram a RAM e o
// kernel/cgroup mata o processo (OOM).
//
// Este pacote dá dois freios GLOBAIS (um processo inteiro, não por instância):
//   - um semáforo que limita quantos downloads acontecem ao mesmo tempo (teto de pico de RAM);
//   - um teto de tamanho no auto-download de recebimento (pula o grandão; o metadado do proto segue
//     no payload, então o app materializa sob demanda se alguém abrir).
//
// Ambos são configuráveis por env e NASCEM INERTES (concurrency 0 = ilimitado, cap 0 = sem teto),
// pra a troca da imagem não mudar comportamento nenhum até serem ligados. Init é chamado uma vez no
// startup (main), antes de qualquer download.
package mediaguard

import "context"

var (
	sem          chan struct{} // nil = sem limite de concorrência
	maxAutoBytes int64         // 0 = sem teto de auto-download
)

// Init configura os freios globais. Chamado uma vez no startup. concurrency <= 0 desliga o semáforo
// (ilimitado, comportamento antigo); maxAutoDownloadBytes <= 0 desliga o teto de auto-download.
func Init(concurrency int, maxAutoDownloadBytes int64) {
	if concurrency > 0 {
		sem = make(chan struct{}, concurrency)
	} else {
		sem = nil
	}
	if maxAutoDownloadBytes > 0 {
		maxAutoBytes = maxAutoDownloadBytes
	} else {
		maxAutoBytes = 0
	}
}

// Acquire pega uma vaga de download, bloqueando até uma liberar ou o ctx cancelar. Devolve uma função
// release que SEMPRE deve ser chamada (idealmente via defer). Sem semáforo (não-inicializado ou
// concurrency 0) é no-op — nunca bloqueia, nunca falha. Um ctx já cancelado devolve o erro do ctx.
func Acquire(ctx context.Context) (release func(), err error) {
	if sem == nil {
		return func() {}, nil
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}

// SkipAutoDownload diz se uma mídia com o tamanho declarado (fileLength do proto, em bytes) deve ser
// PULADA pelo auto-download de recebimento. fileLength <= 0 (desconhecido) nunca pula — o timeout
// cobre a rede. Teto 0 nunca pula (comportamento antigo).
func SkipAutoDownload(fileLength int64) bool {
	return maxAutoBytes > 0 && fileLength > 0 && fileLength > maxAutoBytes
}
