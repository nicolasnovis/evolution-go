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
//
// Mídia pesada em STREAMING (plano arquivos-pesados, 11/2026) soma a este pacote, também global e
// também inerte por default:
//   - AcquireWithin: a mesma vaga de download, mas com prazo (503 busy em vez de fila infinita);
//   - um semáforo separado pra ENVIO grande em streaming (default 1 por vez);
//   - guarda de disco (statfs) antes de criar o temporário: livre >= arquivo + reserva;
//   - pasta de temporários EM DISCO (nunca tmpfs — tmpfs é RAM) e limpeza de órfãos no boot;
//   - lista de hosts aceitos nas URLs do Storage (anti-SSRF: o motor não vira proxy pra qualquer URL);
//   - desligar o auto-download de recebimento (MEDIA_AUTODOWNLOAD=off).
package mediaguard

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	// ErrBusy: no download/send slot became free within the wait window. Callers answer 503 busy.
	ErrBusy = errors.New("media guard: busy, no slot available within the wait window")
	// ErrDiskLow: the temp dir does not have (file size + reserve) free. Callers answer 503 disk_low.
	ErrDiskLow = errors.New("media guard: not enough free disk")
	// ErrNotConfigured: MEDIA_UPLOAD_ALLOWED_HOSTS is empty, so no Storage URL can be accepted yet.
	ErrNotConfigured = errors.New("media guard: MEDIA_UPLOAD_ALLOWED_HOSTS is not configured")
	// ErrURLNotAllowed: the URL is not https or its host is not in MEDIA_UPLOAD_ALLOWED_HOSTS.
	ErrURLNotAllowed = errors.New("media guard: url not allowed")
)

// TempPrefixReceive / TempPrefixSend name the temporary files so the boot cleanup can tell them apart
// from anything else living in the same directory.
const (
	TempPrefixReceive = "rx-"
	TempPrefixSend    = "tx-"
)

var (
	sem          chan struct{} // nil = sem limite de concorrência
	maxAutoBytes int64         // 0 = sem teto de auto-download

	autoOff      bool          // MEDIA_AUTODOWNLOAD=off
	sendSem      chan struct{} // envio grande em streaming; nil = InitStreaming não chamado (1 vaga implícita)
	sendSemOnce  sync.Once
	tmpDir       string // "" = os.TempDir()
	minFreeBytes int64
	allowedHosts []string // lower-case; vazio = endpoint de URL desligado

	// diskProbe is swapped by tests. Production uses statfs on the temp dir.
	diskProbe = freeBytes
)

// Settings is everything the streaming paths need, set once from main (InitStreaming).
type Settings struct {
	// TmpDir holds the rx-*/tx-* temporaries. Must be on disk, never tmpfs. "" = os.TempDir().
	TmpDir string
	// MinFreeBytes is the reserve the disk guard demands on top of the file size.
	MinFreeBytes int64
	// AllowedHosts are the Storage hosts (lower-case, no scheme) accepted in upload/source URLs.
	AllowedHosts []string
	// SendLargeConcurrency caps simultaneous streamed sends; < 1 becomes 1.
	SendLargeConcurrency int
	// AutoDownloadOff disables the inbound auto-download entirely (SkipAutoDownload always true).
	AutoDownloadOff bool
}

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

// InitStreaming applies the streaming settings. Creates the temp dir (falling back to os.TempDir()
// when it cannot be created) and sweeps orphaned rx-*/tx-* files left by a process killed mid-way.
// The returned error is informational (temp dir fallback); the process keeps running.
func InitStreaming(s Settings) error {
	autoOff = s.AutoDownloadOff
	minFreeBytes = s.MinFreeBytes
	if minFreeBytes < 0 {
		minFreeBytes = 0
	}
	allowedHosts = allowedHosts[:0]
	for _, h := range s.AllowedHosts {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			allowedHosts = append(allowedHosts, h)
		}
	}
	n := s.SendLargeConcurrency
	if n < 1 {
		n = 1
	}
	sendSem = make(chan struct{}, n)

	var err error
	tmpDir = s.TmpDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	} else if mkErr := os.MkdirAll(tmpDir, 0o750); mkErr != nil {
		err = fmt.Errorf("media temp dir %q unusable (%v), falling back to %s", tmpDir, mkErr, os.TempDir())
		tmpDir = os.TempDir()
	}
	if _, sweepErr := CleanTempDir(); sweepErr != nil && err == nil {
		err = sweepErr
	}
	return err
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
		return releaseOnce(sem), nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}

// AcquireWithin is Acquire with a deadline: waits at most `wait` for a download slot and returns
// ErrBusy when none frees up (callers answer 503 busy instead of holding the caller's function
// hostage). ctx cancellation still wins. Without a semaphore it is a no-op.
func AcquireWithin(ctx context.Context, wait time.Duration) (release func(), err error) {
	return acquireWithin(ctx, sem, wait)
}

// AcquireSendWithin takes a slot of the streamed-send semaphore (MEDIA_SEND_LARGE_CONCURRENCY) within
// `wait`, or returns ErrBusy. Before InitStreaming a single implicit slot is used.
func AcquireSendWithin(ctx context.Context, wait time.Duration) (release func(), err error) {
	sendSemOnce.Do(func() {
		if sendSem == nil {
			sendSem = make(chan struct{}, 1)
		}
	})
	return acquireWithin(ctx, sendSem, wait)
}

func acquireWithin(ctx context.Context, ch chan struct{}, wait time.Duration) (release func(), err error) {
	if ch == nil {
		return func() {}, nil
	}
	// Fast path: a free slot right now never pays the timer.
	select {
	case ch <- struct{}{}:
		return releaseOnce(ch), nil
	default:
	}
	if wait <= 0 {
		return func() {}, ErrBusy
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		return releaseOnce(ch), nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	case <-timer.C:
		return func() {}, ErrBusy
	}
}

// releaseOnce makes a double release (defer + explicit) harmless.
func releaseOnce(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { <-ch }) }
}

// SkipAutoDownload diz se uma mídia com o tamanho declarado (fileLength do proto, em bytes) deve ser
// PULADA pelo auto-download de recebimento. fileLength <= 0 (desconhecido) nunca pula — o timeout
// cobre a rede. Teto 0 nunca pula (comportamento antigo). Com MEDIA_AUTODOWNLOAD=off pula TUDO,
// inclusive tamanho desconhecido: o webhook segue com os metadados do proto e a miniatura inline, e o
// app materializa pelo downloadmedia-to-url.
func SkipAutoDownload(fileLength int64) bool {
	if autoOff {
		return true
	}
	return maxAutoBytes > 0 && fileLength > 0 && fileLength > maxAutoBytes
}

// AutoDownloadOff reports whether MEDIA_AUTODOWNLOAD=off is active.
func AutoDownloadOff() bool { return autoOff }

// TmpDir is the directory for media temporaries (set by InitStreaming; os.TempDir() before that).
func TmpDir() string {
	if tmpDir == "" {
		return os.TempDir()
	}
	return tmpDir
}

// CreateTemp opens a new temporary file in TmpDir with the given prefix (TempPrefixReceive/Send).
// The caller owns it: close AND remove it in a defer, including on error.
func CreateTemp(prefix string) (*os.File, error) {
	return os.CreateTemp(TmpDir(), prefix+"*")
}

// RemoveTemp closes and deletes a temp file. Safe on nil and on already-removed files.
func RemoveTemp(f *os.File) {
	if f == nil {
		return
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
}

// CleanTempDir removes orphaned rx-*/tx-* files in TmpDir (a process killed mid-transfer leaves
// them behind and they would eat the disk forever). Returns how many were removed.
func CleanTempDir() (int, error) {
	removed := 0
	var firstErr error
	for _, prefix := range []string{TempPrefixReceive, TempPrefixSend} {
		matches, err := filepath.Glob(filepath.Join(TmpDir(), prefix+"*"))
		if err != nil {
			return removed, err
		}
		for _, m := range matches {
			info, err := os.Lstat(m)
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			if err := os.Remove(m); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			removed++
		}
	}
	return removed, firstErr
}

// EnsureDiskFor checks that TmpDir has at least need + MinFreeBytes available. need < 0 is treated
// as 0. A failing probe counts as "low" (fail closed: better a 503 than a full disk).
func EnsureDiskFor(need int64) error {
	if need < 0 {
		need = 0
	}
	free, err := diskProbe(TmpDir())
	if err != nil {
		return fmt.Errorf("%w: statfs %s: %v", ErrDiskLow, TmpDir(), err)
	}
	required := uint64(need) + uint64(minFreeBytes)
	if free < required {
		return fmt.Errorf("%w: %d free, %d required (%d file + %d reserve)", ErrDiskLow, free, required, need, minFreeBytes)
	}
	return nil
}

var (
	diskMu       sync.Mutex
	diskReserved uint64 // bytes promised to in-flight operations (ReserveDisk), not yet released
)

// ReserveDisk is EnsureDiskFor made safe for concurrent operations: the check counts what the other
// in-flight operations were promised (two downloads that each see "enough" can no longer both eat the
// same free space), and the promise lasts until release is called — call it AFTER the temp file is
// removed. need < 0 is treated as 0.
//
// headroom is how much the caller may write without eating into the reserve or another operation's
// promise (>= need on success). Callers cap their writes with it, so a size that lies (a proto that
// declares 1 KB for a 2 GB file, a source without Content-Length) cannot fill the disk.
func ReserveDisk(need int64) (release func(), headroom int64, err error) {
	if need < 0 {
		need = 0
	}
	diskMu.Lock()
	defer diskMu.Unlock()
	free, perr := diskProbe(TmpDir())
	if perr != nil {
		return func() {}, 0, fmt.Errorf("%w: statfs %s: %v", ErrDiskLow, TmpDir(), perr)
	}
	required := uint64(need) + uint64(minFreeBytes) + diskReserved
	if free < required {
		return func() {}, 0, fmt.Errorf("%w: %d free, %d required (%d file + %d reserve + %d in flight)",
			ErrDiskLow, free, required, need, minFreeBytes, diskReserved)
	}
	headroom = int64(free - uint64(minFreeBytes) - diskReserved)
	diskReserved += uint64(need)
	var once sync.Once
	release = func() {
		once.Do(func() {
			diskMu.Lock()
			diskReserved -= uint64(need)
			diskMu.Unlock()
		})
	}
	return release, headroom, nil
}

// DiskReserved reports the bytes currently promised to in-flight operations (tests and logs).
func DiskReserved() uint64 {
	diskMu.Lock()
	defer diskMu.Unlock()
	return diskReserved
}

// SetDiskProbe swaps the free-space probe (tests). Returns the previous probe so it can be restored.
func SetDiskProbe(p func(dir string) (uint64, error)) (previous func(dir string) (uint64, error)) {
	previous = diskProbe
	if p == nil {
		p = freeBytes
	}
	diskProbe = p
	return previous
}

// HostsConfigured reports whether MEDIA_UPLOAD_ALLOWED_HOSTS has at least one host.
func HostsConfigured() bool { return len(allowedHosts) > 0 }

// ValidateStorageURL enforces the anti-SSRF rule for URLs the engine will fetch from or upload to:
// https only, host in MEDIA_UPLOAD_ALLOWED_HOSTS (hostname or host:port, case-insensitive). With no
// hosts configured it returns ErrNotConfigured — the receive endpoint refuses, and the send path
// (compatibility) keeps accepting any URL as it does today.
func ValidateStorageURL(raw string) (*url.URL, error) {
	if len(allowedHosts) == 0 {
		return nil, ErrNotConfigured
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		// url.Parse errors quote the whole raw URL (token included); keep only the cause.
		return nil, fmt.Errorf("%w: %s", ErrURLNotAllowed, RedactURLError(err))
	}
	if !strings.EqualFold(u.Scheme, "https") {
		return nil, fmt.Errorf("%w: scheme %q (https only)", ErrURLNotAllowed, u.Scheme)
	}
	host := strings.ToLower(u.Host)
	hostname := strings.ToLower(u.Hostname())
	if hostname == "" {
		return nil, fmt.Errorf("%w: empty host", ErrURLNotAllowed)
	}
	for _, allowed := range allowedHosts {
		if allowed == host || allowed == hostname {
			return u, nil
		}
	}
	return nil, fmt.Errorf("%w: host %q not in MEDIA_UPLOAD_ALLOWED_HOSTS", ErrURLNotAllowed, hostname)
}

// RedactURLError renders err without the request URL. A *url.Error (anywhere in the chain) prints
// the full URL, and signed URLs carry their credential in the query string (Storage `token`,
// WhatsApp upload `auth`), which must never reach a JSON error body or a log line. For those it
// returns "<op> <host>: <cause>" (host only when the URL parses); any other error is unchanged.
func RedactURLError(err error) string {
	if err == nil {
		return ""
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		return err.Error()
	}
	cause := "unknown error"
	if ue.Err != nil {
		cause = RedactURLError(ue.Err)
	}
	prefix := ue.Op
	if u, perr := url.Parse(ue.URL); perr == nil && u.Host != "" {
		prefix += " " + u.Host
	}
	return prefix + ": " + cause
}
