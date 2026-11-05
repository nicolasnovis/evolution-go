package message_service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/mediaguard"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// ---- test doubles -------------------------------------------------------------------------------

// fakeStorage plays the Supabase upload endpoint: records the PUT and answers `status`.
type fakeStorage struct {
	srv    *httptest.Server
	status atomic.Int32
	body   string

	mu       sync.Mutex
	calls    int
	method   string
	path     string
	query    string
	headers  http.Header
	length   int64
	received int64
	sha256   string
}

func newFakeStorage(t *testing.T) *fakeStorage {
	t.Helper()
	fs := &fakeStorage{body: `{"Key":"whatsapp-media/x"}`}
	fs.status.Store(http.StatusOK)
	fs.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := sha256.New()
		n, _ := io.Copy(h, r.Body)
		fs.mu.Lock()
		fs.calls++
		fs.method = r.Method
		fs.path = r.URL.Path
		fs.query = r.URL.RawQuery
		fs.headers = r.Header.Clone()
		fs.length = r.ContentLength
		fs.received = n
		fs.sha256 = hex.EncodeToString(h.Sum(nil))
		fs.mu.Unlock()
		w.WriteHeader(int(fs.status.Load()))
		_, _ = io.WriteString(w, fs.body)
	}))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *fakeStorage) host(t *testing.T) string {
	u, err := url.Parse(fs.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func (fs *fakeStorage) uploadURL() string {
	return fs.srv.URL + "/storage/v1/object/upload/sign/whatsapp-media/acc/inbox/ID.pdf?token=abc"
}

// setupGuard points mediaguard at a fresh temp dir with the fake storage allowed, and returns the dir.
func setupGuard(t *testing.T, fs *fakeStorage, concurrency int) string {
	t.Helper()
	dir := t.TempDir()
	mediaguard.Init(concurrency, 0)
	if err := mediaguard.InitStreaming(mediaguard.Settings{TmpDir: dir, MinFreeBytes: 0, AllowedHosts: []string{fs.host(t)}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mediaguard.Init(0, 0)
		_ = mediaguard.InitStreaming(mediaguard.Settings{TmpDir: os.TempDir()})
		mediaguard.SetDiskProbe(nil)
	})
	return dir
}

func docProto(size uint64) *waE2E.Message {
	return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		FileLength: proto.Uint64(size),
		Mimetype:   proto.String("application/pdf"),
		FileName:   proto.String("Contrato.pdf"),
		DirectPath: proto.String("/v/t62.7119-24/abc"),
	}}
}

// writeN is a download double that writes `n` bytes of a deterministic pattern into the file.
func writeN(n int64) downloadToFileFunc {
	return func(_ context.Context, _ whatsmeow.DownloadableMessage, f whatsmeow.File) error {
		_, err := io.CopyN(f, &patternReader{}, n)
		return err
	}
}

func failWith(err error) downloadToFileFunc {
	return func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error { return err }
}

type patternReader struct{ i byte }

func (p *patternReader) Read(b []byte) (int, error) {
	for k := range b {
		b[k] = p.i
		p.i++
	}
	return len(b), nil
}

func patternSHA(n int64) string {
	h := sha256.New()
	_, _ = io.CopyN(h, &patternReader{}, n)
	return hex.EncodeToString(h.Sum(nil))
}

func assertDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("temporário não foi apagado: %v", names)
	}
}

func asTyped(t *testing.T, err error) *MediaToUrlError {
	t.Helper()
	var typed *MediaToUrlError
	if !errors.As(err, &typed) {
		t.Fatalf("erro sem tipo: %v", err)
	}
	return typed
}

func deps(fs *fakeStorage, dl downloadToFileFunc) mediaToUrlDeps {
	return mediaToUrlDeps{download: dl, httpClient: fs.srv.Client(), timeout: 10 * time.Second, acquireWait: 50 * time.Millisecond}
}

// ---- node selection -----------------------------------------------------------------------------

func TestPickMediaNode_UnwrapsEnvelopes(t *testing.T) {
	doc := &waE2E.DocumentMessage{FileLength: proto.Uint64(7), FileName: proto.String("a.pdf"), Mimetype: proto.String("application/pdf")}
	wrap := func(m *waE2E.Message) *waE2E.FutureProofMessage { return &waE2E.FutureProofMessage{Message: m} }
	cases := map[string]*waE2E.Message{
		"plain":            {DocumentMessage: doc},
		"docWithCaption":   {DocumentWithCaptionMessage: wrap(&waE2E.Message{DocumentMessage: doc})},
		"ephemeral":        {EphemeralMessage: wrap(&waE2E.Message{DocumentMessage: doc})},
		"viewOnce":         {ViewOnceMessage: wrap(&waE2E.Message{DocumentMessage: doc})},
		"viewOnceV2":       {ViewOnceMessageV2: wrap(&waE2E.Message{DocumentMessage: doc})},
		"viewOnceV2Ext":    {ViewOnceMessageV2Extension: wrap(&waE2E.Message{DocumentMessage: doc})},
		"ephemeral+docCap": {EphemeralMessage: wrap(&waE2E.Message{DocumentWithCaptionMessage: wrap(&waE2E.Message{DocumentMessage: doc})})},
	}
	for name, msg := range cases {
		n := pickMediaNode(msg)
		if n == nil || n.kind != "document" || n.fileLength != 7 || n.fileName != "a.pdf" || n.mimetype != "application/pdf" {
			t.Errorf("%s: nó errado: %+v", name, n)
		}
	}
	if pickMediaNode(&waE2E.Message{Conversation: proto.String("oi")}) != nil {
		t.Error("texto não tem mídia")
	}
	if pickMediaNode(nil) != nil {
		t.Error("nil não tem mídia")
	}
	kinds := map[string]*waE2E.Message{
		"image":   {ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}},
		"video":   {VideoMessage: &waE2E.VideoMessage{}},
		"audio":   {AudioMessage: &waE2E.AudioMessage{}},
		"sticker": {StickerMessage: &waE2E.StickerMessage{Mimetype: proto.String("image/webp")}},
	}
	for kind, msg := range kinds {
		if n := pickMediaNode(msg); n == nil || n.kind != kind {
			t.Errorf("%s: %+v", kind, n)
		}
	}
	if n := pickMediaNode(&waE2E.Message{PtvMessage: &waE2E.VideoMessage{}}); n == nil || n.kind != "video" {
		t.Errorf("ptv vira video: %+v", n)
	}
}

// ---- happy path ---------------------------------------------------------------------------------

func TestDownloadMediaToUrl_PutHappyPath(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 4)
	const size = 300_000

	res, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{
		Message:     docProto(size),
		UploadUrl:   fs.uploadURL(),
		ContentType: "application/pdf",
		MaxBytes:    1 << 20,
		Headers:     map[string]string{"apikey": "pub", "Content-Length": "999", "Host": "evil"},
	}, deps(fs, writeN(size)))
	if err != nil {
		t.Fatalf("esperava sucesso: %v", err)
	}
	if !res.Ok || res.Bytes != size || res.Mimetype != "application/pdf" || res.FileName != "Contrato.pdf" || res.MediaType != "document" {
		t.Fatalf("resultado errado: %+v", res)
	}
	if res.SHA256 != patternSHA(size) || fs.sha256 != res.SHA256 {
		t.Fatalf("sha256 não bate: res=%s storage=%s want=%s", res.SHA256, fs.sha256, patternSHA(size))
	}
	if fs.calls != 1 || fs.method != http.MethodPut || fs.length != size || fs.received != size {
		t.Fatalf("PUT errado: calls=%d method=%s len=%d recv=%d", fs.calls, fs.method, fs.length, fs.received)
	}
	if fs.path != "/storage/v1/object/upload/sign/whatsapp-media/acc/inbox/ID.pdf" || fs.query != "token=abc" {
		t.Fatalf("URL alterada: %s?%s", fs.path, fs.query)
	}
	if got := fs.headers.Get("Content-Type"); got != "application/pdf" {
		t.Fatalf("Content-Type: %q", got)
	}
	if got := fs.headers.Get("x-upsert"); got != "true" {
		t.Fatalf("x-upsert: %q", got)
	}
	if got := fs.headers.Get("Cache-Control"); got != "max-age=3600" {
		t.Fatalf("Cache-Control: %q", got)
	}
	if got := fs.headers.Get("apikey"); got != "pub" {
		t.Fatalf("header extra não passou: %q", got)
	}
	if fs.headers.Get("Host") == "evil" {
		t.Fatal("Host não pode ser sobrescrito pelo app")
	}
	assertDirEmpty(t, dir)
}

func TestDownloadMediaToUrl_ContentTypeFallbacks(t *testing.T) {
	fs := newFakeStorage(t)
	setupGuard(t, fs, 0)

	// proto mimetype wins when the body omits contentType
	res, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(10), UploadUrl: fs.uploadURL()}, deps(fs, writeN(10)))
	if err != nil || res.Mimetype != "application/pdf" {
		t.Fatalf("mimetype do proto: %v %+v", err, res)
	}

	// no mimetype anywhere: sniffed from the bytes (a real PDF header)
	msg := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{DirectPath: proto.String("/x")}}
	pdf := func(_ context.Context, _ whatsmeow.DownloadableMessage, f whatsmeow.File) error {
		_, err := f.Write([]byte("%PDF-1.4\n%âãÏÓ\n1 0 obj\n<<>>\nendobj\n"))
		return err
	}
	res, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: msg, UploadUrl: fs.uploadURL()}, deps(fs, pdf))
	if err != nil || res.Mimetype != "application/pdf" {
		t.Fatalf("sniff: %v %+v", err, res)
	}
	if fs.headers.Get("Content-Type") != "application/pdf" {
		t.Fatalf("Content-Type do PUT: %q", fs.headers.Get("Content-Type"))
	}

	// sticker stays raw webp, no conversion
	st := &waE2E.Message{StickerMessage: &waE2E.StickerMessage{Mimetype: proto.String("image/webp"), DirectPath: proto.String("/x")}}
	res, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: st, UploadUrl: fs.uploadURL()}, deps(fs, writeN(100)))
	if err != nil || res.Mimetype != "image/webp" || res.MediaType != "sticker" || res.Bytes != 100 {
		t.Fatalf("sticker: %v %+v", err, res)
	}
}

// ---- refusals before any download ---------------------------------------------------------------

func TestDownloadMediaToUrl_TooLargeDeclared_NoDownload(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	var downloads atomic.Int32
	dl := func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error {
		downloads.Add(1)
		return nil
	}

	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(101), UploadUrl: fs.uploadURL(), MaxBytes: 100}, deps(fs, dl))
	typed := asTyped(t, err)
	if typed.Status != 413 || typed.Code != "too_large" || typed.Extra["declaredBytes"] != int64(101) || typed.Extra["maxBytes"] != int64(100) {
		t.Fatalf("413 too_large esperado: %+v", typed)
	}
	if downloads.Load() != 0 || fs.calls != 0 {
		t.Fatal("não pode baixar nem fazer PUT quando o tamanho declarado passa do teto")
	}
	// exactly at the cap is fine
	if _, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(100), UploadUrl: fs.uploadURL(), MaxBytes: 100}, deps(fs, writeN(100))); err != nil {
		t.Fatalf("igual ao teto passa: %v", err)
	}
	assertDirEmpty(t, dir)
}

func TestDownloadMediaToUrl_ActualSizeOverMax(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	// fileLength lies (says 10), the CDN sends 500
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(10), UploadUrl: fs.uploadURL(), MaxBytes: 100}, deps(fs, writeN(500)))
	typed := asTyped(t, err)
	if typed.Status != 413 || typed.Code != "too_large" {
		t.Fatalf("413 too_large pelo tamanho real: %+v", typed)
	}
	if fs.calls != 0 {
		t.Fatal("não pode fazer PUT")
	}
	assertDirEmpty(t, dir)
}

// A sender-controlled fileLength that lies must not let the download write past maxBytes: the cap
// stops io.Copy (whatsmeow fills the file with io.Copy, which prefers ReadFrom) and cancels the
// download so no other CDN host is tried.
func TestDownloadMediaToUrl_LyingSizeIsCappedWhileWriting(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	var written int64
	var ctxDone bool
	dl := func(ctx context.Context, _ whatsmeow.DownloadableMessage, f whatsmeow.File) error {
		n, err := io.Copy(f, io.LimitReader(&patternReader{}, 10<<20)) // the "CDN" sends 10 MB
		written = n
		ctxDone = ctx.Err() != nil
		return err
	}
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(10), UploadUrl: fs.uploadURL(), MaxBytes: 1000}, deps(fs, dl))
	typed := asTyped(t, err)
	if typed.Status != 413 || typed.Code != "too_large" {
		t.Fatalf("esperava 413 too_large: %+v", typed)
	}
	if written > 1000+encOverheadBytes {
		t.Fatalf("escreveu %d bytes, acima do teto %d", written, 1000+encOverheadBytes)
	}
	if !ctxDone {
		t.Fatal("o teto devia cancelar o download (sem tentar outro host do CDN)")
	}
	if fs.calls != 0 || mediaguard.DiskReserved() != 0 {
		t.Fatalf("PUT=%d reservado=%d", fs.calls, mediaguard.DiskReserved())
	}
	assertDirEmpty(t, dir)
}

// Without maxBytes the cap is the free-disk headroom (disk_low), never unbounded.
func TestDownloadMediaToUrl_NoMaxBytesCappedByDiskHeadroom(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	mediaguard.SetDiskProbe(func(string) (uint64, error) { return 1500, nil })
	_ = mediaguard.InitStreaming(mediaguard.Settings{TmpDir: dir, MinFreeBytes: 1000, AllowedHosts: []string{fs.host(t)}})
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(10), UploadUrl: fs.uploadURL()}, deps(fs, writeN(5000)))
	typed := asTyped(t, err)
	if typed.Status != 503 || typed.Code != "disk_low" {
		t.Fatalf("esperava 503 disk_low pelo teto de disco: %+v", typed)
	}
	if mediaguard.DiskReserved() != 0 {
		t.Fatalf("reserva não foi solta: %d", mediaguard.DiskReserved())
	}
	assertDirEmpty(t, dir)
}

// The PUT never follows a redirect: a 307 would replay the body to a host outside the allow-list.
func TestDownloadMediaToUrl_PutDoesNotFollowRedirect(t *testing.T) {
	var evilHits atomic.Int32
	evil := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evilHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	t.Cleanup(evil.Close)
	redir := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Redirect(w, r, evil.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redir.Close)
	u, _ := url.Parse(redir.URL)
	dir := t.TempDir()
	mediaguard.Init(0, 0)
	if err := mediaguard.InitStreaming(mediaguard.Settings{TmpDir: dir, AllowedHosts: []string{u.Host}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mediaguard.InitStreaming(mediaguard.Settings{TmpDir: os.TempDir()}) })

	client := redir.Client()
	client.CheckRedirect = storageHTTPClient.CheckRedirect
	client.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify = true // evil uses another cert
	d := mediaToUrlDeps{download: writeN(10), httpClient: client, timeout: 10 * time.Second, acquireWait: 50 * time.Millisecond}
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(10), UploadUrl: redir.URL + "/up"}, d)
	typed := asTyped(t, err)
	if typed.Status != 502 || typed.Code != "storage_rejected" || typed.Extra["status"] != 307 {
		t.Fatalf("redirect devia virar storage_rejected 307: %+v", typed)
	}
	if evilHits.Load() != 0 {
		t.Fatal("o PUT seguiu o redirect pra um host fora da lista")
	}
	assertDirEmpty(t, dir)
}

func TestDownloadMediaToUrl_SSRF(t *testing.T) {
	fs := newFakeStorage(t)
	setupGuard(t, fs, 0)
	var downloads atomic.Int32
	dl := func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error {
		downloads.Add(1)
		return nil
	}

	for _, bad := range []string{
		"http://" + fs.host(t) + "/plain",
		"https://evil.example/upload",
		"https://127.0.0.1:6060/debug/pprof/",
	} {
		_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(1), UploadUrl: bad}, deps(fs, dl))
		typed := asTyped(t, err)
		if typed.Status != 400 || typed.Code != "upload_url_not_allowed" {
			t.Errorf("%s: esperava 400 upload_url_not_allowed, veio %+v", bad, typed)
		}
	}
	if downloads.Load() != 0 || fs.calls != 0 {
		t.Fatal("URL recusada não pode baixar nem subir nada")
	}

	// no hosts configured at all → 503 not_configured (engine not switched on yet)
	mediaguard.Init(0, 0)
	_ = mediaguard.InitStreaming(mediaguard.Settings{TmpDir: t.TempDir()})
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(1), UploadUrl: fs.uploadURL()}, deps(fs, dl))
	typed := asTyped(t, err)
	if typed.Status != 503 || typed.Code != "not_configured" {
		t.Fatalf("sem hosts: %+v", typed)
	}
}

func TestDownloadMediaToUrl_BadInput(t *testing.T) {
	fs := newFakeStorage(t)
	setupGuard(t, fs, 0)
	_, err := downloadMediaToUrl(context.Background(), nil, deps(fs, writeN(1)))
	if typed := asTyped(t, err); typed.Status != 400 || typed.Code != "bad_request" {
		t.Fatalf("nil: %+v", typed)
	}
	_, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: &waE2E.Message{Conversation: proto.String("x")}, UploadUrl: fs.uploadURL()}, deps(fs, writeN(1)))
	if typed := asTyped(t, err); typed.Status != 400 || typed.Code != "invalid_media" {
		t.Fatalf("sem mídia: %+v", typed)
	}
}

// ---- error mapping -------------------------------------------------------------------------------

func TestDownloadMediaToUrl_ErrorMapping(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	cases := []struct {
		name   string
		err    error
		status int
		code   string
		cdn    any
	}{
		{"404", fmt.Errorf("wrapped: %w", whatsmeow.ErrMediaDownloadFailedWith404), 410, "expired", 404},
		{"410", whatsmeow.ErrMediaDownloadFailedWith410, 410, "expired", 410},
		{"403", fmt.Errorf("failed to download media from last host: %w", whatsmeow.ErrMediaDownloadFailedWith403), 410, "expired", 403},
		{"hmac", whatsmeow.ErrInvalidMediaHMAC, 422, "corrupt", nil},
		{"sha", fmt.Errorf("x: %w", whatsmeow.ErrInvalidMediaSHA256), 422, "corrupt", nil},
		{"encsha", whatsmeow.ErrInvalidMediaEncSHA256, 422, "corrupt", nil},
		{"short", whatsmeow.ErrTooShortFile, 422, "corrupt", nil},
		{"nourl", whatsmeow.ErrNoURLPresent, 400, "invalid_media", nil},
		{"deadline", context.DeadlineExceeded, 504, "timeout", nil},
		{"canceled", context.Canceled, 499, "canceled", nil},
		{"cdn500", whatsmeow.DownloadHTTPError{Response: &http.Response{StatusCode: 500}}, 502, "download_failed", nil},
		{"net", errors.New("dial tcp: i/o timeout"), 502, "download_failed", nil},
	}
	for _, c := range cases {
		_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(1), UploadUrl: fs.uploadURL()}, deps(fs, failWith(c.err)))
		typed := asTyped(t, err)
		if typed.Status != c.status || typed.Code != c.code {
			t.Errorf("%s: esperava %d %s, veio %d %s (%v)", c.name, c.status, c.code, typed.Status, typed.Code, typed)
		}
		if c.cdn != nil && typed.Extra["cdnStatus"] != c.cdn {
			t.Errorf("%s: cdnStatus=%v", c.name, typed.Extra["cdnStatus"])
		}
		assertDirEmpty(t, dir)
	}
	if fs.calls != 0 {
		t.Fatal("download falhando nunca faz PUT")
	}
}

func TestDownloadMediaToUrl_DeadlineDuringDownload(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	slow := func(ctx context.Context, _ whatsmeow.DownloadableMessage, _ whatsmeow.File) error {
		<-ctx.Done()
		return ctx.Err()
	}
	d := deps(fs, slow)
	d.timeout = 30 * time.Millisecond
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(1), UploadUrl: fs.uploadURL()}, d)
	if typed := asTyped(t, err); typed.Status != 504 || typed.Code != "timeout" {
		t.Fatalf("prazo: %+v", typed)
	}
	assertDirEmpty(t, dir)
}

func TestDownloadMediaToUrl_StorageRejected(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)

	fs.status.Store(400)
	fs.body = `{"statusCode":"400","error":"InvalidJWT","message":"jwt expired"}`
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(50), UploadUrl: fs.uploadURL()}, deps(fs, writeN(50)))
	typed := asTyped(t, err)
	if typed.Status != 502 || typed.Code != "storage_rejected" || typed.Extra["status"] != 400 || typed.Extra["body"] != fs.body {
		t.Fatalf("400 do Storage: %+v", typed)
	}
	assertDirEmpty(t, dir)

	fs.status.Store(413)
	fs.body = `{"error":"Payload too large"}`
	_, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(50), UploadUrl: fs.uploadURL()}, deps(fs, writeN(50)))
	typed = asTyped(t, err)
	if typed.Status != 413 || typed.Code != "storage_too_large" || typed.Extra["status"] != 413 {
		t.Fatalf("413 do Storage: %+v", typed)
	}
	assertDirEmpty(t, dir)

	// body preview is capped at 300 bytes
	fs.status.Store(500)
	fs.body = string(bytes.Repeat([]byte("x"), 2000))
	_, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(50), UploadUrl: fs.uploadURL()}, deps(fs, writeN(50)))
	typed = asTyped(t, err)
	if typed.Code != "storage_rejected" || len(typed.Extra["body"].(string)) != storageBodyPreviewBytes {
		t.Fatalf("preview: %+v", typed)
	}

	// network failure on the PUT (server gone) → storage_rejected status 0
	fs.srv.Close()
	_, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(50), UploadUrl: fs.uploadURL()}, deps(fs, writeN(50)))
	typed = asTyped(t, err)
	if typed.Status != 502 || typed.Code != "storage_rejected" || typed.Extra["status"] != 0 {
		t.Fatalf("PUT sem rede: %+v", typed)
	}
	assertDirEmpty(t, dir)
}

// ---- guards --------------------------------------------------------------------------------------

func TestDownloadMediaToUrl_Busy(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 1)
	release, err := mediaguard.AcquireWithin(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var downloads atomic.Int32
	dl := func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error {
		downloads.Add(1)
		return nil
	}

	start := time.Now()
	_, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(1), UploadUrl: fs.uploadURL()}, deps(fs, dl))
	typed := asTyped(t, err)
	if typed.Status != 503 || typed.Code != "busy" {
		t.Fatalf("cheio: %+v", typed)
	}
	if time.Since(start) > time.Second {
		t.Fatal("esperou além do prazo de aquisição")
	}
	if downloads.Load() != 0 {
		t.Fatal("sem vaga não baixa")
	}
	assertDirEmpty(t, dir)

	release()
	if _, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(5), UploadUrl: fs.uploadURL()}, deps(fs, writeN(5))); err != nil {
		t.Fatalf("com vaga livre passa: %v", err)
	}
	assertDirEmpty(t, dir)
}

func TestDownloadMediaToUrl_DiskLow(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	var seenNeed []uint64
	mediaguard.SetDiskProbe(func(string) (uint64, error) { return 1000, nil })
	_ = mediaguard.InitStreaming(mediaguard.Settings{TmpDir: dir, MinFreeBytes: 900, AllowedHosts: []string{fs.host(t)}})
	_ = seenNeed

	var downloads atomic.Int32
	dl := func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error {
		downloads.Add(1)
		return nil
	}
	// declared 200 + reserve 900 > 1000 free → disk_low, nothing downloaded
	_, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(200), UploadUrl: fs.uploadURL()}, deps(fs, dl))
	typed := asTyped(t, err)
	if typed.Status != 503 || typed.Code != "disk_low" {
		t.Fatalf("disco baixo: %+v", typed)
	}
	if downloads.Load() != 0 {
		t.Fatal("sem disco não baixa")
	}
	// declared 100 + 900 = 1000 → ok
	if _, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(100), UploadUrl: fs.uploadURL()}, deps(fs, writeN(100))); err != nil {
		t.Fatalf("no limite passa: %v", err)
	}
	// unknown size reserves maxBytes
	_, err = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(0), UploadUrl: fs.uploadURL(), MaxBytes: 500}, deps(fs, dl))
	if typed := asTyped(t, err); typed.Code != "disk_low" {
		t.Fatalf("tamanho desconhecido reserva maxBytes: %+v", typed)
	}
	assertDirEmpty(t, dir)
}

// ---- memory: a big file must not inflate the heap ----------------------------------------------

func TestDownloadMediaToUrl_MemoryBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("96 MB on disk; skipped with -short")
	}
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	const size = 96 << 20

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var peak uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peak {
					peak = ms.HeapInuse
				}
			}
		}
	}()

	res, err := downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(size), UploadUrl: fs.uploadURL(), MaxBytes: 100 << 20}, deps(fs, writeN(size)))
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Bytes != size || fs.received != size || fs.sha256 != res.SHA256 {
		t.Fatalf("transferência incompleta: %+v recv=%d", res, fs.received)
	}
	var delta uint64
	if peak > before.HeapInuse {
		delta = peak - before.HeapInuse
	}
	t.Logf("heap in use: antes=%d MiB pico=%d MiB delta=%d MiB", before.HeapInuse>>20, peak>>20, delta>>20)
	if delta > 16<<20 {
		t.Fatalf("pico de heap %d MiB acima de 16 MiB para um arquivo de %d MiB", delta>>20, size>>20)
	}
	assertDirEmpty(t, dir)
}

// ---- panic safety: the temp file disappears even if the download panics -----------------------

func TestDownloadMediaToUrl_TempRemovedOnPanic(t *testing.T) {
	fs := newFakeStorage(t)
	dir := setupGuard(t, fs, 0)
	boom := func(context.Context, whatsmeow.DownloadableMessage, whatsmeow.File) error { panic("boom") }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("esperava panic")
			}
		}()
		_, _ = downloadMediaToUrl(context.Background(), &DownloadMediaToUrlStruct{Message: docProto(1), UploadUrl: fs.uploadURL()}, deps(fs, boom))
	}()
	assertDirEmpty(t, dir)
}

func TestMediaToUrlError_Fields(t *testing.T) {
	e := newMediaToUrlError(413, "too_large", "declared 5 > 4", map[string]any{"declaredBytes": int64(5)})
	f := e.Fields()
	if f["code"] != "too_large" || f["error"] != "too_large: declared 5 > 4" || f["declaredBytes"] != int64(5) {
		t.Fatalf("%v", f)
	}
	if (&MediaToUrlError{Code: "busy"}).Error() != "busy" {
		t.Fatal("sem detalhe = só o código")
	}
}
