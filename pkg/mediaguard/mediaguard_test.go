package mediaguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func resetForTest(t *testing.T, s Settings) {
	t.Helper()
	Init(0, 0)
	if s.TmpDir == "" {
		s.TmpDir = t.TempDir()
	}
	if err := InitStreaming(s); err != nil {
		t.Fatalf("InitStreaming: %v", err)
	}
	t.Cleanup(func() {
		Init(0, 0)
		_ = InitStreaming(Settings{TmpDir: os.TempDir()})
		SetDiskProbe(nil)
	})
}

func TestAcquireWithin_NoSemaphoreIsNoop(t *testing.T) {
	resetForTest(t, Settings{})
	release, err := AcquireWithin(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatalf("sem semáforo nunca falha: %v", err)
	}
	release()
}

func TestAcquireWithin_BusyThenReleased(t *testing.T) {
	resetForTest(t, Settings{})
	Init(1, 0)

	release1, err := AcquireWithin(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("primeira vaga: %v", err)
	}

	start := time.Now()
	_, err = AcquireWithin(context.Background(), 30*time.Millisecond)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("cheio devia dar ErrBusy, deu %v", err)
	}
	if waited := time.Since(start); waited < 25*time.Millisecond || waited > time.Second {
		t.Fatalf("espera fora do prazo: %v", waited)
	}

	release1()
	release1() // double release must be harmless
	release2, err := AcquireWithin(context.Background(), 30*time.Millisecond)
	if err != nil {
		t.Fatalf("depois de liberar devia pegar: %v", err)
	}
	release2()
}

func TestAcquireWithin_CtxCancelWins(t *testing.T) {
	resetForTest(t, Settings{})
	Init(1, 0)
	release, _ := AcquireWithin(context.Background(), time.Second)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := AcquireWithin(ctx, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx cancelado devia vencer: %v", err)
	}
}

func TestAcquireSendWithin_DefaultOneSlot(t *testing.T) {
	resetForTest(t, Settings{SendLargeConcurrency: 0}) // < 1 becomes 1
	release, err := AcquireSendWithin(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatalf("primeira vaga de envio: %v", err)
	}
	if _, err := AcquireSendWithin(context.Background(), 10*time.Millisecond); !errors.Is(err, ErrBusy) {
		t.Fatalf("segunda vaga devia dar ErrBusy: %v", err)
	}
	release()
	release2, err := AcquireSendWithin(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatalf("depois de liberar: %v", err)
	}
	release2()
}

func TestSkipAutoDownload_Off(t *testing.T) {
	resetForTest(t, Settings{})
	Init(0, 1000)
	if SkipAutoDownload(0) {
		t.Fatal("tamanho desconhecido com teto não pula (comportamento antigo)")
	}
	if !SkipAutoDownload(1001) {
		t.Fatal("acima do teto pula")
	}

	resetForTest(t, Settings{AutoDownloadOff: true})
	if !AutoDownloadOff() {
		t.Fatal("flag não ligou")
	}
	for _, n := range []int64{0, 1, 1001} {
		if !SkipAutoDownload(n) {
			t.Fatalf("com off pula tudo, inclusive %d", n)
		}
	}
}

func TestEnsureDiskFor(t *testing.T) {
	resetForTest(t, Settings{MinFreeBytes: 100})
	SetDiskProbe(func(string) (uint64, error) { return 150, nil })
	if err := EnsureDiskFor(50); err != nil {
		t.Fatalf("50 + 100 reserva = 150 livre devia passar: %v", err)
	}
	if err := EnsureDiskFor(51); !errors.Is(err, ErrDiskLow) {
		t.Fatalf("51 + 100 > 150 devia dar ErrDiskLow: %v", err)
	}
	if err := EnsureDiskFor(-5); err != nil {
		t.Fatalf("need negativo vira 0: %v", err)
	}
	SetDiskProbe(func(string) (uint64, error) { return 0, errors.New("statfs boom") })
	if err := EnsureDiskFor(0); !errors.Is(err, ErrDiskLow) {
		t.Fatalf("probe falhando fecha a porta: %v", err)
	}
	SetDiskProbe(nil)
	if _, err := freeBytes(t.TempDir()); err != nil {
		t.Fatalf("statfs real: %v", err)
	}
}

func TestValidateStorageURL(t *testing.T) {
	resetForTest(t, Settings{})
	if _, err := ValidateStorageURL("https://x.supabase.co/a"); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("sem hosts = not configured: %v", err)
	}
	if HostsConfigured() {
		t.Fatal("HostsConfigured devia ser false")
	}

	resetForTest(t, Settings{AllowedHosts: []string{" X.Supabase.co ", "127.0.0.1:8443"}})
	ok := []string{
		"https://x.supabase.co/storage/v1/object/upload/sign/b/p?token=t",
		"HTTPS://X.SUPABASE.CO/a",
		"https://127.0.0.1:8443/put",
	}
	for _, u := range ok {
		if _, err := ValidateStorageURL(u); err != nil {
			t.Errorf("%s devia passar: %v", u, err)
		}
	}
	bad := []string{
		"http://x.supabase.co/a",       // scheme
		"https://evil.com/a",           // host
		"https://x.supabase.co.evil/a", // suffix trick
		"https://127.0.0.1:9999/put",   // port differs and bare ip not listed
		"https://user@x.supabase.co@evil.com/a",
		"://broken",
		"",
	}
	for _, u := range bad {
		if _, err := ValidateStorageURL(u); !errors.Is(err, ErrURLNotAllowed) {
			t.Errorf("%q devia ser recusada com ErrURLNotAllowed: %v", u, err)
		}
	}
}

func TestCleanTempDir_RemovesOnlyOurPrefixes(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"rx-abc", "tx-def", "keep.txt", "rxnot"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "rx-dir"), 0o750); err != nil {
		t.Fatal(err)
	}
	resetForTest(t, Settings{TmpDir: dir}) // InitStreaming sweeps on boot
	entries, _ := os.ReadDir(dir)
	var left []string
	for _, e := range entries {
		left = append(left, e.Name())
	}
	want := map[string]bool{"keep.txt": true, "rxnot": true, "rx-dir": true}
	if len(left) != len(want) {
		t.Fatalf("sobrou %v, queria só %v", left, want)
	}
	for _, n := range left {
		if !want[n] {
			t.Fatalf("%s não devia ter sido apagado/sobrado", n)
		}
	}
}

func TestCreateTemp_InTmpDirAndRemoveTemp(t *testing.T) {
	dir := t.TempDir()
	resetForTest(t, Settings{TmpDir: dir})
	f, err := CreateTemp(TempPrefixReceive)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(f.Name()) != dir || filepath.Base(f.Name())[:3] != "rx-" {
		t.Fatalf("nome fora do padrão: %s", f.Name())
	}
	RemoveTemp(f)
	RemoveTemp(f) // idempotent
	RemoveTemp(nil)
	if _, err := os.Stat(f.Name()); !os.IsNotExist(err) {
		t.Fatalf("temporário devia sumir: %v", err)
	}
}

func TestInitStreaming_FallsBackWhenDirUnusable(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = InitStreaming(Settings{TmpDir: os.TempDir()}) })
	err := InitStreaming(Settings{TmpDir: filepath.Join(blocker, "sub")})
	if err == nil {
		t.Fatal("pasta impossível devia avisar")
	}
	if TmpDir() != os.TempDir() {
		t.Fatalf("devia cair no os.TempDir, ficou %s", TmpDir())
	}
}
