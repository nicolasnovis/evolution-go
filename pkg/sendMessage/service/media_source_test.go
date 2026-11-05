package send_service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	config "github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/mediaguard"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/util/cbcutil"
)

// ---- helpers ------------------------------------------------------------------------------------

type patternReader struct{ i byte }

func (p *patternReader) Read(b []byte) (int, error) {
	for k := range b {
		b[k] = p.i
		p.i++
	}
	return len(b), nil
}

func patternBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = (&patternReader{}).Read(b)
	return b
}

func shaOf(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// pdfBytes is a minimal (but real) PDF header so the mimetype sniffer says application/pdf.
var pdfHead = []byte("%PDF-1.4\n%âãÏÓ\n1 0 obj\n<< /Type /Catalog >>\nendobj\n")

// sourceServer serves `body` with optional Content-Length (chunked when false) and `status`.
func sourceServer(t *testing.T, body []byte, withLength bool, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if withLength {
			w.Header().Set("Content-Length", itoa(len(body)))
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func itoa(n int) string { return strconv.Itoa(n) }

// guardDir points mediaguard at a fresh dir (no hosts configured unless given) and returns it.
func guardDir(t *testing.T, hosts ...string) string {
	t.Helper()
	dir := t.TempDir()
	mediaguard.Init(0, 0)
	if err := mediaguard.InitStreaming(mediaguard.Settings{TmpDir: dir, AllowedHosts: hosts, SendLargeConcurrency: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mediaguard.Init(0, 0)
		_ = mediaguard.InitStreaming(mediaguard.Settings{TmpDir: os.TempDir()})
		mediaguard.SetDiskProbe(nil)
	})
	return dir
}

func assertDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporário não foi apagado: %v", entries)
	}
}

func asSendErr(t *testing.T, err error) *SendMediaError {
	t.Helper()
	var typed *SendMediaError
	if !errors.As(err, &typed) {
		t.Fatalf("erro sem tipo: %v", err)
	}
	return typed
}

func lim(max, above int64) sendMediaLimits {
	return sendMediaLimits{maxBytes: max, streamAbove: above, acquireWait: 50 * time.Millisecond}
}

func testService(t *testing.T) *sendService {
	t.Helper()
	cfg := &config.Config{LogDirectory: t.TempDir()}
	return &sendService{config: cfg, loggerWrapper: logger_wrapper.NewLoggerManager(cfg)}
}

// ---- fetchMediaSource: small path (today's behaviour) ------------------------------------------

func TestFetchMediaSource_SmallPathInMemory(t *testing.T) {
	guardDir(t)
	body := append(append([]byte{}, pdfHead...), patternBytes(1<<20)...)
	srv := sourceServer(t, body, true, 200)

	src, err := fetchMediaSource(context.Background(), srv.URL+"/doc.pdf", "document", false, lim(100<<20, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.streamed() || src.size != int64(len(body)) || !bytes.Equal(src.data, body) || src.mimeType != "application/pdf" {
		t.Fatalf("pequeno em memória: streamed=%v size=%d mime=%s", src.streamed(), src.size, src.mimeType)
	}
	// streaming OFF (streamAbove 0): a document above any threshold still goes in memory
	src2, err := fetchMediaSource(context.Background(), srv.URL+"/doc.pdf", "document", false, lim(100<<20, 0), nil)
	if err != nil || src2.streamed() {
		t.Fatalf("com SEND_MEDIA_STREAM_ABOVE=0 nunca faz streaming: %v %v", err, src2.streamed())
	}
	src2.Close()
}

func TestFetchMediaSource_Mp4SuffixOverride(t *testing.T) {
	guardDir(t)
	srv := sourceServer(t, patternBytes(100), true, 200)
	src, err := fetchMediaSource(context.Background(), srv.URL+"/video.MP4", "video", false, lim(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.mimeType != "video/mp4" {
		t.Fatalf("sufixo .mp4 força video/mp4: %s", src.mimeType)
	}
}

func TestFetchMediaSource_StatusNot200(t *testing.T) {
	guardDir(t)
	srv := sourceServer(t, []byte("<html>not found</html>"), true, 404)
	_, err := fetchMediaSource(context.Background(), srv.URL+"/x", "document", false, lim(0, 0), nil)
	typed := asSendErr(t, err)
	if typed.Status != 502 || typed.Code != "download_failed" || typed.Extra["status"] != 404 {
		t.Fatalf("404 na origem: %+v", typed)
	}
}

func TestFetchMediaSource_NetworkError(t *testing.T) {
	guardDir(t)
	srv := sourceServer(t, nil, true, 200)
	u := srv.URL
	srv.Close()
	_, err := fetchMediaSource(context.Background(), u+"/x", "document", false, lim(0, 0), nil)
	typed := asSendErr(t, err)
	if typed.Status != 502 || typed.Code != "download_failed" || typed.Extra["status"] != 0 {
		t.Fatalf("sem rede: %+v", typed)
	}
}

func TestFetchMediaSource_TooLarge(t *testing.T) {
	dir := guardDir(t)
	body := patternBytes(2000)

	// declared by Content-Length → refused before reading
	srv := sourceServer(t, body, true, 200)
	_, err := fetchMediaSource(context.Background(), srv.URL+"/x", "document", false, lim(1000, 0), nil)
	typed := asSendErr(t, err)
	if typed.Status != 413 || typed.Code != "too_large" || typed.Extra["declaredBytes"] != int64(2000) || typed.Extra["maxBytes"] != int64(1000) {
		t.Fatalf("Content-Length acima: %+v", typed)
	}

	// chunked (no Content-Length): counted while reading — small path
	chunked := sourceServer(t, body, false, 200)
	_, err = fetchMediaSource(context.Background(), chunked.URL+"/x", "document", false, lim(1000, 0), nil)
	if typed := asSendErr(t, err); typed.Status != 413 || typed.Code != "too_large" {
		t.Fatalf("chunked pequeno: %+v", typed)
	}
	// chunked — large path (unknown size streams when streaming is on)
	_, err = fetchMediaSource(context.Background(), chunked.URL+"/x", "document", false, lim(1000, 500), nil)
	if typed := asSendErr(t, err); typed.Status != 413 || typed.Code != "too_large" {
		t.Fatalf("chunked grande: %+v", typed)
	}
	assertDirEmpty(t, dir)

	// exactly at the cap passes
	exact := sourceServer(t, patternBytes(1000), true, 200)
	src, err := fetchMediaSource(context.Background(), exact.URL+"/x", "document", false, lim(1000, 0), nil)
	if err != nil {
		t.Fatalf("igual ao teto passa: %v", err)
	}
	src.Close()
}

// ---- fetchMediaSource: large path ----------------------------------------------------------------

func TestFetchMediaSource_LargePathToDisk(t *testing.T) {
	dir := guardDir(t)
	body := append(append([]byte{}, pdfHead...), patternBytes(3<<20)...)
	srv := sourceServer(t, body, true, 200)

	src, err := fetchMediaSource(context.Background(), srv.URL+"/big.pdf", "document", false, lim(100<<20, 1<<20), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !src.streamed() || src.data != nil || src.size != int64(len(body)) || src.mimeType != "application/pdf" {
		t.Fatalf("grande em disco: streamed=%v size=%d mime=%s", src.streamed(), src.size, src.mimeType)
	}
	if filepath.Dir(src.file.Name()) != dir || !strings.HasPrefix(filepath.Base(src.file.Name()), "tx-") {
		t.Fatalf("temporário fora do lugar: %s", src.file.Name())
	}
	got, err := io.ReadAll(src.file) // positioned at 0 after the sniff
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("conteúdo do temporário difere (err=%v len=%d)", err, len(got))
	}
	// the slot is held while the source is open
	if _, err := mediaguard.AcquireSendWithin(context.Background(), 10*time.Millisecond); !errors.Is(err, mediaguard.ErrBusy) {
		t.Fatalf("vaga devia estar ocupada: %v", err)
	}
	src.Close()
	src.Close() // idempotent
	assertDirEmpty(t, dir)
	rel, err := mediaguard.AcquireSendWithin(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatalf("vaga devia ter sido liberada: %v", err)
	}
	rel()
}

func TestFetchMediaSource_LargePathTypeRules(t *testing.T) {
	dir := guardDir(t)
	body := patternBytes(3000)
	srv := sourceServer(t, body, true, 200)
	cases := []struct {
		mediaType  string
		newsletter bool
		streamed   bool
	}{
		{"document", false, true},
		{"video", false, true},
		{"image", false, false},
		{"audio", false, false},
		{"ptv", false, false},
		{"video", true, false}, // newsletter upload needs []byte
	}
	for _, c := range cases {
		src, err := fetchMediaSource(context.Background(), srv.URL+"/x", c.mediaType, c.newsletter, lim(0, 1000), nil)
		if err != nil {
			t.Fatalf("%s: %v", c.mediaType, err)
		}
		if src.streamed() != c.streamed {
			t.Errorf("%s newsletter=%v: streamed=%v, queria %v", c.mediaType, c.newsletter, src.streamed(), c.streamed)
		}
		src.Close()
	}
	// at or below the threshold stays in memory
	src, err := fetchMediaSource(context.Background(), srv.URL+"/x", "document", false, lim(0, 3000), nil)
	if err != nil || src.streamed() {
		t.Fatalf("igual ao limiar fica em memória: %v %v", err, src.streamed())
	}
	src.Close()
	assertDirEmpty(t, dir)
}

func TestFetchMediaSource_Busy(t *testing.T) {
	dir := guardDir(t)
	rel, err := mediaguard.AcquireSendWithin(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	srv := sourceServer(t, patternBytes(3000), true, 200)
	_, err = fetchMediaSource(context.Background(), srv.URL+"/x", "document", false, lim(0, 1000), nil)
	typed := asSendErr(t, err)
	if typed.Status != 503 || typed.Code != "busy" {
		t.Fatalf("vaga ocupada: %+v", typed)
	}
	assertDirEmpty(t, dir)
	rel()
	// small path never needs the slot
	src, err := fetchMediaSource(context.Background(), srv.URL+"/x", "image", false, lim(0, 1000), nil)
	if err != nil {
		t.Fatalf("imagem não usa a vaga: %v", err)
	}
	src.Close()
}

func TestFetchMediaSource_DiskLow(t *testing.T) {
	dir := guardDir(t)
	mediaguard.SetDiskProbe(func(string) (uint64, error) { return 100, nil })
	srv := sourceServer(t, patternBytes(3000), true, 200)
	_, err := fetchMediaSource(context.Background(), srv.URL+"/x", "document", false, lim(0, 1000), nil)
	typed := asSendErr(t, err)
	if typed.Status != 503 || typed.Code != "disk_low" {
		t.Fatalf("disco baixo: %+v", typed)
	}
	assertDirEmpty(t, dir)
	// the slot must have been released on the way out
	rel, err := mediaguard.AcquireSendWithin(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatalf("vaga presa depois de disk_low: %v", err)
	}
	rel()
}

func TestFetchMediaSource_Truncated(t *testing.T) {
	dir := guardDir(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5000")
		w.WriteHeader(200)
		_, _ = w.Write(patternBytes(2500))
		// connection closes with half the promised bytes
	}))
	t.Cleanup(srv.Close)
	for _, above := range []int64{0, 1000} {
		_, err := fetchMediaSource(context.Background(), srv.URL+"/x", "document", false, lim(0, above), nil)
		if typed := asSendErr(t, err); typed.Status != 502 || typed.Code != "download_failed" {
			t.Fatalf("truncado (streamAbove=%d): %+v", above, typed)
		}
	}
	assertDirEmpty(t, dir)
}

func TestFetchMediaSource_Timeout(t *testing.T) {
	dir := guardDir(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5000")
		w.WriteHeader(200)
		_, _ = w.Write(patternBytes(100))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	for _, above := range []int64{0, 1000} {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := fetchMediaSource(ctx, srv.URL+"/x", "document", false, lim(0, above), nil)
		cancel()
		if typed := asSendErr(t, err); typed.Status != 504 || typed.Code != "timeout" {
			t.Fatalf("prazo (streamAbove=%d): %+v", above, typed)
		}
	}
	assertDirEmpty(t, dir)
}

func TestFetchMediaSource_HostRule(t *testing.T) {
	// no hosts configured: any URL, even plain http, as today
	guardDir(t)
	plain := sourceServer(t, patternBytes(10), true, 200)
	src, err := fetchMediaSource(context.Background(), plain.URL+"/x", "document", false, lim(0, 0), nil)
	if err != nil {
		t.Fatalf("sem lista aceita http: %v", err)
	}
	src.Close()

	// hosts configured: https only, listed host only
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(patternBytes(10)) }))
	t.Cleanup(tls.Close)
	u, _ := url.Parse(tls.URL)
	guardDir(t, u.Host)
	l := lim(0, 0)
	l.httpClient = tls.Client()
	if _, err := fetchMediaSource(context.Background(), plain.URL+"/x", "document", false, l, nil); asSendErr(t, err).Code != "url_not_allowed" {
		t.Fatalf("http com lista ligada: %v", err)
	}
	if _, err := fetchMediaSource(context.Background(), "https://evil.example/x", "document", false, l, nil); asSendErr(t, err).Code != "url_not_allowed" {
		t.Fatalf("host fora da lista: %v", err)
	}
	src, err = fetchMediaSource(context.Background(), tls.URL+"/x", "document", false, l, nil)
	if err != nil {
		t.Fatalf("host da lista passa: %v", err)
	}
	src.Close()
}

// ---- upload --------------------------------------------------------------------------------------

// fakeUploader reads everything it is given and hashes it, like the real client would encrypt it.
type fakeUploader struct {
	calls      int
	readerUsed bool
	sameFile   bool
	sha256     string
	length     uint64
	err        error
}

func (f *fakeUploader) Upload(_ context.Context, plaintext []byte, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	f.calls++
	f.sha256 = shaOf(plaintext)
	f.length = uint64(len(plaintext))
	return whatsmeow.UploadResponse{URL: "https://mmg/x", DirectPath: "/x", FileLength: f.length, MediaKey: []byte("k"), FileSHA256: []byte("s"), FileEncSHA256: []byte("e")}, f.err
}

func (f *fakeUploader) UploadReader(_ context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	f.calls++
	f.readerUsed = true
	f.sameFile = any(plaintext) == any(tempFile)
	h := sha256.New()
	n, err := io.Copy(h, plaintext)
	if err != nil {
		return whatsmeow.UploadResponse{}, err
	}
	f.sha256 = hex.EncodeToString(h.Sum(nil))
	f.length = uint64(n)
	return whatsmeow.UploadResponse{URL: "https://mmg/x", DirectPath: "/x", FileLength: f.length, MediaKey: []byte("k"), FileSHA256: []byte("s"), FileEncSHA256: []byte("e")}, f.err
}

func (f *fakeUploader) UploadNewsletter(_ context.Context, data []byte, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	f.calls++
	f.sha256 = shaOf(data)
	f.length = uint64(len(data))
	return whatsmeow.UploadResponse{URL: "https://mmg/n", DirectPath: "/n", FileLength: f.length, Handle: "h"}, f.err
}

func TestUploadMediaSource_LargeStreamsSameFile(t *testing.T) {
	if testing.Short() {
		t.Skip("64 MB on disk; skipped with -short")
	}
	dir := guardDir(t)
	const size = 64 << 20
	body := patternBytes(size)
	srv := sourceServer(t, body, true, 200)
	src, err := fetchMediaSource(context.Background(), srv.URL+"/big.bin", "document", false, lim(100<<20, 1<<20), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if !src.streamed() {
		t.Fatal("devia estar em disco")
	}
	up := &fakeUploader{}
	resp, err := uploadMediaSource(context.Background(), up, src, whatsmeow.MediaDocument, false)
	if err != nil {
		t.Fatal(err)
	}
	if !up.readerUsed || !up.sameFile || up.calls != 1 {
		t.Fatalf("UploadReader com o mesmo arquivo como origem e temporário: reader=%v same=%v calls=%d", up.readerUsed, up.sameFile, up.calls)
	}
	if up.sha256 != shaOf(body) || resp.FileLength != size || up.length != size {
		t.Fatalf("o upload não leu o arquivo inteiro: sha=%s len=%d", up.sha256, up.length)
	}
	src.Close()
	assertDirEmpty(t, dir)
}

func TestUploadMediaSource_SmallAndNewsletter(t *testing.T) {
	data := patternBytes(1234)
	up := &fakeUploader{}
	resp, err := uploadMediaSource(context.Background(), up, &mediaSource{data: data, size: 1234}, whatsmeow.MediaImage, false)
	if err != nil || up.readerUsed || up.sha256 != shaOf(data) || resp.FileLength != 1234 {
		t.Fatalf("pequeno usa Upload([]byte): %v %+v", err, up)
	}
	up = &fakeUploader{}
	resp, err = uploadMediaSource(context.Background(), up, &mediaSource{data: data, size: 1234}, whatsmeow.MediaVideo, true)
	if err != nil || resp.Handle != "h" || up.sha256 != shaOf(data) {
		t.Fatalf("newsletter usa UploadNewsletter: %v %+v", err, resp)
	}
	up = &fakeUploader{err: errors.New("upload failed with status code 503")}
	_, err = uploadMediaSource(context.Background(), up, &mediaSource{data: data, size: 1234}, whatsmeow.MediaImage, false)
	if typed := asSendErr(t, err); typed.Status != 502 || typed.Code != "upload_failed" {
		t.Fatalf("falha do CDN do WhatsApp: %+v", typed)
	}
	up = &fakeUploader{err: context.DeadlineExceeded}
	_, err = uploadMediaSource(context.Background(), up, &mediaSource{data: data, size: 1234}, whatsmeow.MediaImage, false)
	if typed := asSendErr(t, err); typed.Status != 504 || typed.Code != "timeout" {
		t.Fatalf("prazo no upload: %+v", typed)
	}
}

// TestEncryptStreamInPlace proves the premise the large path rests on: whatsmeow's EncryptStream can
// use the SAME file as plaintext source and ciphertext destination, and the result decrypts back.
func TestEncryptStreamInPlace(t *testing.T) {
	dir := guardDir(t)
	plain := patternBytes(1<<20 + 13) // not a multiple of the AES block
	f, err := mediaguard.CreateTemp(mediaguard.TempPrefixSend)
	if err != nil {
		t.Fatal(err)
	}
	defer mediaguard.RemoveTemp(f)
	if _, err := f.Write(plain); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	key := patternBytes(32)
	iv := patternBytes(16)
	mac := patternBytes(32)
	plainSHA, _, plainLen, cipherLen, err := cbcutil.EncryptStream(key, iv, mac, f, f)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(plainSHA) != shaOf(plain) || plainLen != uint64(len(plain)) {
		t.Fatalf("EncryptStream leu o arquivo errado: len=%d", plainLen)
	}
	info, _ := f.Stat()
	if uint64(info.Size()) != cipherLen {
		t.Fatalf("arquivo cifrado com %d bytes, esperava %d", info.Size(), cipherLen)
	}
	// strip the 10-byte MAC, decrypt in place, compare
	if err := f.Truncate(int64(cipherLen) - 10); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := cbcutil.DecryptFile(key, iv, f); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	back, err := io.ReadAll(f)
	if err != nil || !bytes.Equal(back, plain) {
		t.Fatalf("decifrado difere do original (err=%v len=%d)", err, len(back))
	}
	mediaguard.RemoveTemp(f)
	assertDirEmpty(t, dir)
}

// ---- prepare + build: the proto is the same as before ------------------------------------------

func TestPrepareAndBuild_SmallPDFSameProto(t *testing.T) {
	guardDir(t)
	s := testService(t)
	pdf := append(append([]byte{}, pdfHead...), patternBytes(1<<20)...)
	src := &mediaSource{data: pdf, size: int64(len(pdf)), mimeType: "application/pdf"}
	data := &MediaStruct{Number: "5521999999999", Type: "document", Filename: "Orçamento.pdf", Caption: "segue"}

	prep, err := s.prepareMedia(src, data, ptr(instanceFor("inst")))
	if err != nil {
		t.Fatal(err)
	}
	if prep.uploadType != whatsmeow.MediaDocument || prep.mimeType != "application/pdf" {
		t.Fatalf("prep: %+v", prep)
	}
	up := &fakeUploader{}
	uploaded, err := uploadMediaSource(context.Background(), up, src, prep.uploadType, false)
	if err != nil {
		t.Fatal(err)
	}
	msg, mediaType, err := buildMediaMessage(data, prep, uploaded, false)
	if err != nil || mediaType != "DocumentMessage" {
		t.Fatal(err, mediaType)
	}
	// caption → wrapped in DocumentWithCaptionMessage, top-level DocumentMessage nil (as before)
	if msg.DocumentMessage != nil || msg.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage() == nil {
		t.Fatalf("embrulho da legenda: %v", msg)
	}
	d := msg.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
	if d.GetURL() != "https://mmg/x" || d.GetDirectPath() != "/x" || string(d.GetMediaKey()) != "k" ||
		string(d.GetFileSHA256()) != "s" || string(d.GetFileEncSHA256()) != "e" || d.GetMimetype() != "application/pdf" ||
		d.GetFileName() != "Orçamento.pdf" || d.GetCaption() != "segue" {
		t.Fatalf("campos: %v", d)
	}
	if d.GetFileLength() != uint64(len(pdf)) || d.GetFileLength() != uploaded.FileLength {
		t.Fatalf("FileLength = len(arquivo) = uploaded.FileLength: %d %d %d", d.GetFileLength(), len(pdf), uploaded.FileLength)
	}
	if _, err := exec.LookPath("pdftoppm"); err == nil && d.GetJPEGThumbnail() == nil {
		t.Log("pdftoppm existe mas a miniatura veio vazia (PDF mínimo pode não renderizar) — não é erro do caminho")
	}

	// without caption: plain DocumentMessage
	data.Caption = ""
	msg, _, _ = buildMediaMessage(data, prep, uploaded, false)
	if msg.DocumentMessage == nil || msg.DocumentWithCaptionMessage != nil {
		t.Fatalf("sem legenda vai direto: %v", msg)
	}
}

func instanceFor(id string) instance_model.Instance { return instance_model.Instance{Id: id} }

func ptr(i instance_model.Instance) *instance_model.Instance { return &i }

func TestPrepareMedia_PDFOver20MBStreamedNoThumb(t *testing.T) {
	dir := guardDir(t)
	s := testService(t)
	f, err := mediaguard.CreateTemp(mediaguard.TempPrefixSend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(pdfHead); err != nil {
		t.Fatal(err)
	}
	_, _ = f.Seek(0, io.SeekStart)
	// size says 21 MB (the preview rule reads the recorded size, not the file)
	src := &mediaSource{file: f, size: 21 << 20, mimeType: "application/pdf"}
	prep, err := s.prepareMedia(src, &MediaStruct{Type: "document", Filename: "big.pdf"}, ptr(instanceFor("inst")))
	if err != nil {
		t.Fatal(err)
	}
	if prep.jpegThumb != nil || prep.pageCount != nil || prep.thumbW != nil {
		t.Fatalf("PDF > 20 MB em streaming vai sem prévia: %+v", prep)
	}
	// ≤ 20 MB streamed: the plaintext is read from the file (preview attempted; result depends on pdftoppm)
	src.size = int64(len(pdfHead))
	if plain := src.plaintextForThumb(); !bytes.Equal(plain, pdfHead) {
		t.Fatalf("plaintextForThumb lê o arquivo: %q", plain)
	}
	src.Close()
	assertDirEmpty(t, dir)
}

func TestPrepareMedia_TypeChecksUnchanged(t *testing.T) {
	guardDir(t)
	s := testService(t)
	inst := instanceFor("inst")
	if _, err := s.prepareMedia(&mediaSource{data: []byte("x"), size: 1, mimeType: "text/plain"}, &MediaStruct{Type: "image"}, &inst); err == nil || !strings.Contains(err.Error(), "Invalid file format: 'text/plain'") {
		t.Fatalf("imagem inválida: %v", err)
	}
	if _, err := s.prepareMedia(&mediaSource{data: []byte("x"), size: 1, mimeType: "video/quicktime"}, &MediaStruct{Type: "video"}, &inst); err == nil || !strings.Contains(err.Error(), "Only 'video/mp4'") {
		t.Fatalf("vídeo inválido: %v", err)
	}
	if _, err := s.prepareMedia(&mediaSource{data: []byte("x"), size: 1, mimeType: "text/plain"}, &MediaStruct{Type: "sticker"}, &inst); err == nil || err.Error() != "invalid media type" {
		t.Fatalf("tipo inválido: %v", err)
	}
	prep, err := s.prepareMedia(&mediaSource{data: []byte("x"), size: 1, mimeType: "image/webp"}, &MediaStruct{Type: "image"}, &inst)
	if err != nil || prep.mimeType != "image/jpeg" || prep.uploadType != whatsmeow.MediaImage {
		t.Fatalf("webp vira image/jpeg como antes: %v %+v", err, prep)
	}
}

// ---- retry: only SendMessage repeats -------------------------------------------------------------

func TestSendWithRetryOnDisconnect(t *testing.T) {
	var ensures, sends int
	var slept []time.Duration
	sleep := func(d time.Duration) { slept = append(slept, d) }
	ok := &MessageSendStruct{}

	// drops twice, then goes through: 3 sends, 2 re-ensures, waits 1s then 2s
	ensures, sends, slept = 0, 0, nil
	msg, err := sendWithRetryOnDisconnect(3,
		func() error { ensures++; return nil },
		func() (*MessageSendStruct, error) {
			sends++
			if sends < 3 {
				return nil, errors.New("client disconnected")
			}
			return ok, nil
		}, sleep, nil)
	if err != nil || msg != ok || sends != 3 || ensures != 2 || len(slept) != 2 || slept[0] != time.Second || slept[1] != 2*time.Second {
		t.Fatalf("retry: err=%v sends=%d ensures=%d slept=%v", err, sends, ensures, slept)
	}

	// a non-disconnect error returns at once
	ensures, sends, slept = 0, 0, nil
	_, err = sendWithRetryOnDisconnect(3, func() error { ensures++; return nil },
		func() (*MessageSendStruct, error) { sends++; return nil, errors.New("recipient not on whatsapp") }, sleep, nil)
	if err == nil || sends != 1 || ensures != 0 || len(slept) != 0 {
		t.Fatalf("erro definitivo: err=%v sends=%d", err, sends)
	}

	// always disconnected: last error after maxRetries sends
	ensures, sends, slept = 0, 0, nil
	_, err = sendWithRetryOnDisconnect(3, func() error { ensures++; return nil },
		func() (*MessageSendStruct, error) { sends++; return nil, errors.New("no active session found") }, sleep, nil)
	if err == nil || sends != 3 || ensures != 2 {
		t.Fatalf("sempre caído: err=%v sends=%d ensures=%d", err, sends, ensures)
	}

	// re-ensure failing: skips the send, gives up on the last attempt with that error
	ensures, sends, slept = 0, 0, nil
	_, err = sendWithRetryOnDisconnect(3, func() error { ensures++; return errors.New("client disconnected") },
		func() (*MessageSendStruct, error) { sends++; return nil, errors.New("client disconnected") }, sleep, nil)
	if err == nil || sends != 1 || ensures != 2 {
		t.Fatalf("ensure falhando: err=%v sends=%d ensures=%d", err, sends, ensures)
	}
}

func TestSendMediaError_Fields(t *testing.T) {
	e := newSendMediaError(413, "too_large", "declared 5 > 4", map[string]any{"maxBytes": int64(4)})
	f := e.Fields()
	if f["code"] != "too_large" || f["error"] != "too_large: declared 5 > 4" || f["maxBytes"] != int64(4) {
		t.Fatalf("%v", f)
	}
	if !isDisconnectError(errors.New("x: client disconnected")) || isDisconnectError(nil) || isDisconnectError(errors.New("boom")) {
		t.Fatal("isDisconnectError")
	}
}
