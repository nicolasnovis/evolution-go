package message_service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/evolution-foundation/evolution-go/pkg/mediaguard"
	"github.com/gabriel-vasile/mimetype"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
)

// POST /message/downloadmedia-to-url — streams a received media from the WhatsApp CDN to a signed
// upload URL of the Storage without ever holding the file in RAM.
//
// Why: /message/downloadmedia returns the whole file as base64 inside JSON (~4.7x the file in RAM,
// incident 2026-09-21) and every byte then crosses the app's serverless function. Here the engine
// decrypts the media INTO a temp file on disk (whatsmeow DownloadToFile: 32 KB buffers, in-place
// decrypt) and PUTs that file to the URL the app signed. RAM per operation stays constant; disk is
// 1x the file and is freed in a defer, success or failure.
//
// Errors carry an HTTP status and a stable `code` (MediaToUrlError) so the app can tell a final
// state (expired, corrupt, too_large) from a transient one (busy, disk_low, timeout).

const (
	// mediaToUrlAcquireWait bounds the wait for a download slot: past it the engine answers 503 busy
	// and the app reschedules instead of pinning its function on a queue.
	mediaToUrlAcquireWait = 20 * time.Second
	// storageBodyPreviewBytes caps how much of a Storage error body is echoed back.
	storageBodyPreviewBytes = 300
)

type DownloadMediaToUrlStruct struct {
	// Message is the media proto exactly as the webhook delivered it (wrapped forms accepted).
	Message *waE2E.Message `json:"message"`
	// UploadUrl is the signed upload URL (PUT) — https, host in MEDIA_UPLOAD_ALLOWED_HOSTS.
	UploadUrl string `json:"uploadUrl"`
	// ContentType overrides the proto mimetype for the PUT (optional).
	ContentType string `json:"contentType"`
	// MaxBytes refuses (413) media declared or measured above it. 0 = no cap from the caller.
	MaxBytes int64 `json:"maxBytes"`
	// Headers are extra request headers for the PUT (e.g. apikey). Optional.
	Headers map[string]string `json:"headers"`
}

type DownloadMediaToUrlResult struct {
	Ok         bool   `json:"ok"`
	Bytes      int64  `json:"bytes"`
	Mimetype   string `json:"mimetype"`
	FileName   string `json:"fileName"`
	SHA256     string `json:"sha256"`
	MediaType  string `json:"mediaType"`
	DownloadMs int64  `json:"downloadMs"`
	UploadMs   int64  `json:"uploadMs"`
}

// MediaToUrlError is the typed failure of the endpoint: HTTP status + machine-readable code.
type MediaToUrlError struct {
	Status int
	Code   string
	Detail string
	Extra  map[string]any
}

func (e *MediaToUrlError) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// Fields is the JSON body the handler writes: {"error","code",...extra}.
func (e *MediaToUrlError) Fields() map[string]any {
	out := map[string]any{"error": e.Error(), "code": e.Code}
	for k, v := range e.Extra {
		out[k] = v
	}
	return out
}

func newMediaToUrlError(status int, code, detail string, extra map[string]any) *MediaToUrlError {
	return &MediaToUrlError{Status: status, Code: code, Detail: detail, Extra: extra}
}

// mediaLog is the slice of the instance logger the streaming code needs (tests pass a no-op).
type mediaLog interface {
	LogInfo(format string, args ...interface{})
	LogWarn(format string, args ...interface{})
	LogError(format string, args ...interface{})
}

type noopMediaLog struct{}

func (noopMediaLog) LogInfo(string, ...interface{})  {}
func (noopMediaLog) LogWarn(string, ...interface{})  {}
func (noopMediaLog) LogError(string, ...interface{}) {}

// downloadToFileFunc is client.DownloadToFile behind an indirection so the pipeline is testable
// without a WhatsApp session.
type downloadToFileFunc func(ctx context.Context, msg whatsmeow.DownloadableMessage, f *os.File) error

type mediaToUrlDeps struct {
	download    downloadToFileFunc
	httpClient  *http.Client
	timeout     time.Duration
	acquireWait time.Duration
	log         mediaLog
}

// storageHTTPClient is the dedicated client for the PUT: no Client.Timeout (the ctx bounds the whole
// operation), but every phase that can hang has its own limit.
var storageHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second, // counted after the body is fully written
		ExpectContinueTimeout: 2 * time.Second,
		MaxIdleConns:          8,
		IdleConnTimeout:       90 * time.Second,
	},
}

// mediaNode is the downloadable node picked out of a (possibly wrapped) message.
type mediaNode struct {
	msg        whatsmeow.DownloadableMessage
	kind       string // image | video | audio | document | sticker
	mimetype   string
	fileLength int64
	fileName   string
}

// unwrapMediaMessage peels the envelopes a history-sync message may come in (documentWithCaption,
// ephemeral, viewOnce v1/v2/v2 extension). Live events already arrive unwrapped.
func unwrapMediaMessage(msg *waE2E.Message) *waE2E.Message {
	for i := 0; i < 6 && msg != nil; i++ {
		switch {
		case msg.GetDocumentWithCaptionMessage().GetMessage() != nil:
			msg = msg.GetDocumentWithCaptionMessage().GetMessage()
		case msg.GetEphemeralMessage().GetMessage() != nil:
			msg = msg.GetEphemeralMessage().GetMessage()
		case msg.GetViewOnceMessage().GetMessage() != nil:
			msg = msg.GetViewOnceMessage().GetMessage()
		case msg.GetViewOnceMessageV2().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2().GetMessage()
		case msg.GetViewOnceMessageV2Extension().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2Extension().GetMessage()
		default:
			return msg
		}
	}
	return msg
}

// pickMediaNode returns the media node of the message, or nil when there is none.
func pickMediaNode(raw *waE2E.Message) *mediaNode {
	msg := unwrapMediaMessage(raw)
	if msg == nil {
		return nil
	}
	switch {
	case msg.GetImageMessage() != nil:
		n := msg.GetImageMessage()
		return &mediaNode{msg: n, kind: "image", mimetype: n.GetMimetype(), fileLength: int64(n.GetFileLength())}
	case msg.GetVideoMessage() != nil:
		n := msg.GetVideoMessage()
		return &mediaNode{msg: n, kind: "video", mimetype: n.GetMimetype(), fileLength: int64(n.GetFileLength())}
	case msg.GetPtvMessage() != nil:
		n := msg.GetPtvMessage()
		return &mediaNode{msg: n, kind: "video", mimetype: n.GetMimetype(), fileLength: int64(n.GetFileLength())}
	case msg.GetAudioMessage() != nil:
		n := msg.GetAudioMessage()
		return &mediaNode{msg: n, kind: "audio", mimetype: n.GetMimetype(), fileLength: int64(n.GetFileLength())}
	case msg.GetDocumentMessage() != nil:
		n := msg.GetDocumentMessage()
		return &mediaNode{msg: n, kind: "document", mimetype: n.GetMimetype(), fileLength: int64(n.GetFileLength()), fileName: n.GetFileName()}
	case msg.GetStickerMessage() != nil:
		n := msg.GetStickerMessage()
		// Sticker goes out as raw webp — no webp→png decode (that is a full RGBA image in RAM).
		return &mediaNode{msg: n, kind: "sticker", mimetype: n.GetMimetype(), fileLength: int64(n.GetFileLength())}
	}
	return nil
}

// classifyDownloadError maps whatsmeow/ctx errors to the endpoint's codes.
func classifyDownloadError(err error) *MediaToUrlError {
	switch {
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404):
		return newMediaToUrlError(http.StatusGone, "expired", "media no longer available on WhatsApp (404)", map[string]any{"cdnStatus": 404})
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410):
		return newMediaToUrlError(http.StatusGone, "expired", "media no longer available on WhatsApp (410)", map[string]any{"cdnStatus": 410})
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403):
		// The CDN answers 403 for a URL past its lifetime; final, logged with the original code to measure.
		return newMediaToUrlError(http.StatusGone, "expired", "media no longer available on WhatsApp (403)", map[string]any{"cdnStatus": 403})
	case errors.Is(err, whatsmeow.ErrInvalidMediaHMAC),
		errors.Is(err, whatsmeow.ErrInvalidMediaSHA256),
		errors.Is(err, whatsmeow.ErrInvalidMediaEncSHA256),
		errors.Is(err, whatsmeow.ErrInvalidUnencryptedMediaSHA256),
		errors.Is(err, whatsmeow.ErrTooShortFile):
		return newMediaToUrlError(http.StatusUnprocessableEntity, "corrupt", err.Error(), nil)
	case errors.Is(err, whatsmeow.ErrNoURLPresent), errors.Is(err, whatsmeow.ErrUnknownMediaType):
		return newMediaToUrlError(http.StatusBadRequest, "invalid_media", err.Error(), nil)
	case errors.Is(err, context.DeadlineExceeded):
		return newMediaToUrlError(http.StatusGatewayTimeout, "timeout", "download did not finish within the deadline", nil)
	case errors.Is(err, context.Canceled):
		return newMediaToUrlError(499, "canceled", "request canceled by the caller", nil)
	}
	return newMediaToUrlError(http.StatusBadGateway, "download_failed", err.Error(), nil)
}

// downloadMediaToUrl is the whole pipeline (steps numbered as in the plan, section A.1).
func downloadMediaToUrl(parent context.Context, data *DownloadMediaToUrlStruct, deps mediaToUrlDeps) (*DownloadMediaToUrlResult, error) {
	if deps.log == nil {
		deps.log = noopMediaLog{}
	}
	if deps.httpClient == nil {
		deps.httpClient = storageHTTPClient
	}
	if deps.acquireWait <= 0 {
		deps.acquireWait = mediaToUrlAcquireWait
	}
	if data == nil || data.Message == nil {
		return nil, newMediaToUrlError(http.StatusBadRequest, "bad_request", "message is required", nil)
	}

	// 1. node
	node := pickMediaNode(data.Message)
	if node == nil {
		return nil, newMediaToUrlError(http.StatusBadRequest, "invalid_media", "message has no downloadable media", nil)
	}

	// 2. declared size — refuse before touching the network
	if data.MaxBytes > 0 && node.fileLength > data.MaxBytes {
		return nil, newMediaToUrlError(http.StatusRequestEntityTooLarge, "too_large",
			fmt.Sprintf("declared %d bytes > maxBytes %d", node.fileLength, data.MaxBytes),
			map[string]any{"declaredBytes": node.fileLength, "maxBytes": data.MaxBytes})
	}

	// 3. anti-SSRF
	if _, err := mediaguard.ValidateStorageURL(data.UploadUrl); err != nil {
		if errors.Is(err, mediaguard.ErrNotConfigured) {
			return nil, newMediaToUrlError(http.StatusServiceUnavailable, "not_configured", err.Error(), nil)
		}
		return nil, newMediaToUrlError(http.StatusBadRequest, "upload_url_not_allowed", err.Error(), nil)
	}

	// 4. deadline
	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	if deps.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, deps.timeout)
		defer cancel()
	}

	// 5. slot with a deadline
	release, err := mediaguard.AcquireWithin(ctx, deps.acquireWait)
	if err != nil {
		if errors.Is(err, mediaguard.ErrBusy) {
			return nil, newMediaToUrlError(http.StatusServiceUnavailable, "busy", "all media slots are taken, retry shortly", nil)
		}
		return nil, classifyDownloadError(err)
	}
	defer release()

	// 6. disk guard — unknown size reserves the cap (worst case)
	need := node.fileLength
	if need <= 0 {
		need = data.MaxBytes
	}
	if err := mediaguard.EnsureDiskFor(need); err != nil {
		deps.log.LogWarn("downloadmedia-to-url: %v", err)
		return nil, newMediaToUrlError(http.StatusServiceUnavailable, "disk_low", err.Error(), nil)
	}

	// 7. temp file — removed on every exit (success, error, panic unwinding)
	f, err := mediaguard.CreateTemp(mediaguard.TempPrefixReceive)
	if err != nil {
		return nil, newMediaToUrlError(http.StatusServiceUnavailable, "disk_low", "cannot create temp file: "+err.Error(), nil)
	}
	defer mediaguard.RemoveTemp(f)

	downloadStart := time.Now()
	if err := deps.download(ctx, node.msg, f); err != nil {
		mapped := classifyDownloadError(err)
		deps.log.LogWarn("downloadmedia-to-url: download failed (%s) kind=%s declared=%d: %v", mapped.Code, node.kind, node.fileLength, err)
		return nil, mapped
	}
	downloadMs := time.Since(downloadStart).Milliseconds()

	// 8. real size (fileLength in the proto is not trusted)
	info, err := f.Stat()
	if err != nil {
		return nil, newMediaToUrlError(http.StatusInternalServerError, "internal", "stat temp file: "+err.Error(), nil)
	}
	size := info.Size()
	if data.MaxBytes > 0 && size > data.MaxBytes {
		return nil, newMediaToUrlError(http.StatusRequestEntityTooLarge, "too_large",
			fmt.Sprintf("downloaded %d bytes > maxBytes %d", size, data.MaxBytes),
			map[string]any{"declaredBytes": node.fileLength, "actualBytes": size, "maxBytes": data.MaxBytes})
	}

	// 9. content type
	contentType := strings.TrimSpace(data.ContentType)
	if contentType == "" {
		contentType = strings.TrimSpace(node.mimetype)
	}
	if contentType == "" {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, newMediaToUrlError(http.StatusInternalServerError, "internal", "seek: "+err.Error(), nil)
		}
		if m, err := mimetype.DetectReader(f); err == nil && m != nil {
			contentType = m.String()
		} else {
			contentType = "application/octet-stream"
		}
	}

	// 10. PUT the file as the body
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, newMediaToUrlError(http.StatusInternalServerError, "internal", "seek: "+err.Error(), nil)
	}
	uploadStart := time.Now()
	sum, putErr := putFileToURL(ctx, deps.httpClient, f, size, data.UploadUrl, contentType, data.Headers)
	uploadMs := time.Since(uploadStart).Milliseconds()
	if putErr != nil {
		deps.log.LogWarn("downloadmedia-to-url: PUT failed (%s) bytes=%d downloadMs=%d uploadMs=%d: %v", putErr.Code, size, downloadMs, uploadMs, putErr)
		return nil, putErr
	}

	deps.log.LogInfo("downloadmedia-to-url: ok kind=%s bytes=%d downloadMs=%d uploadMs=%d type=%s", node.kind, size, downloadMs, uploadMs, contentType)

	// 11. done
	return &DownloadMediaToUrlResult{
		Ok:         true,
		Bytes:      size,
		Mimetype:   contentType,
		FileName:   node.fileName,
		SHA256:     sum,
		MediaType:  node.kind,
		DownloadMs: downloadMs,
		UploadMs:   uploadMs,
	}, nil
}

// putFileToURL streams f (size bytes, positioned at 0) to uploadUrl with a single PUT and returns the
// hex SHA-256 of what was sent. Non-2xx answers become storage_rejected (502) or storage_too_large (413).
func putFileToURL(ctx context.Context, client *http.Client, f *os.File, size int64, uploadUrl, contentType string, headers map[string]string) (string, *MediaToUrlError) {
	hasher := sha256.New()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadUrl, io.TeeReader(f, hasher))
	if err != nil {
		return "", newMediaToUrlError(http.StatusBadRequest, "upload_url_not_allowed", err.Error(), nil)
	}
	req.ContentLength = size
	// If the transport has to replay the request (stale keep-alive), rewind the file and the hash.
	req.GetBody = func() (io.ReadCloser, error) {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		hasher.Reset()
		return io.NopCloser(io.TeeReader(f, hasher)), nil
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-upsert", "true")
	req.Header.Set("Cache-Control", "max-age=3600")
	for k, v := range headers {
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "", "content-length", "host", "transfer-encoding":
			continue
		}
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return "", newMediaToUrlError(http.StatusGatewayTimeout, "timeout", "upload did not finish within the deadline", nil)
		case errors.Is(err, context.Canceled):
			return "", newMediaToUrlError(499, "canceled", "request canceled by the caller", nil)
		}
		return "", newMediaToUrlError(http.StatusBadGateway, "storage_rejected", "PUT failed: "+err.Error(), map[string]any{"status": 0})
	}
	defer resp.Body.Close()
	preview, _ := io.ReadAll(io.LimitReader(resp.Body, storageBodyPreviewBytes))
	// Drain the rest so the connection can be reused (bounded: the Storage answers tiny bodies).
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))

	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		return "", newMediaToUrlError(http.StatusRequestEntityTooLarge, "storage_too_large", "storage refused the file size",
			map[string]any{"status": resp.StatusCode, "body": string(preview)})
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", newMediaToUrlError(http.StatusBadGateway, "storage_rejected", fmt.Sprintf("storage answered %d", resp.StatusCode),
			map[string]any{"status": resp.StatusCode, "body": string(preview)})
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// DownloadMediaToUrl is the service entry point (see the file comment). Uses the REQUEST context so
// the transfer stops when the app gives up, bounded by MEDIA_TO_URL_TIMEOUT.
func (m *messageService) DownloadMediaToUrl(data *DownloadMediaToUrlStruct, instance *instance_model.Instance, request *http.Request) (*DownloadMediaToUrlResult, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, newMediaToUrlError(http.StatusServiceUnavailable, "disconnected", err.Error(), nil)
	}

	ctx := context.Background()
	if request != nil {
		ctx = request.Context()
	}
	timeout := 240 * time.Second
	if m.config != nil && m.config.MediaToUrlTimeout > 0 {
		timeout = m.config.MediaToUrlTimeout
	}
	deps := mediaToUrlDeps{
		download: func(ctx context.Context, msg whatsmeow.DownloadableMessage, f *os.File) error {
			return client.DownloadToFile(ctx, msg, f)
		},
		httpClient: storageHTTPClient,
		timeout:    timeout,
		log:        m.loggerWrapper.GetLogger(instance.Id),
	}
	return downloadMediaToUrl(ctx, data, deps)
}
