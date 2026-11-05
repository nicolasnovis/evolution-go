package send_service

import (
	"bytes"
	"context"
	"encoding/base64"
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
	"google.golang.org/protobuf/proto"
)

// /send/media by URL, split into steps so a large file never lives in RAM (plano arquivos-pesados B.2).
//
// Small path (default, and always for image/audio/ptv/newsletter): the file is read into memory and
// uploaded with client.Upload — exactly today's behaviour, now with a deadline, a status check and a
// size cap on the source GET.
//
// Large path (SEND_MEDIA_STREAM_ABOVE > 0, document/video above it): the GET is copied to a temp file
// in MEDIA_TMP_DIR (tx-*), encrypted IN PLACE and streamed to WhatsApp with client.UploadReader
// (32 KB buffers). Disk = 1x the file, RAM constant. One slot at a time (MEDIA_SEND_LARGE_CONCURRENCY)
// and a disk guard before writing. The temp file is removed in a defer on every exit.
//
// Download and upload happen ONCE; only SendMessage is retried when the session drops.

const (
	// sendAcquireWait bounds the wait for a streamed-send slot before answering 503 busy.
	sendAcquireWait = 20 * time.Second
	// pdfThumbMaxSourceBytes: above this a streamed PDF goes without the page-1 preview (the preview
	// needs the whole file in memory for pdftoppm).
	pdfThumbMaxSourceBytes = 20 << 20
)

// SendMediaError is the typed failure of /send/media by URL: HTTP status + stable code for the app.
// Anything else keeps today's `500 {"error": …}`.
type SendMediaError struct {
	Status int
	Code   string
	Detail string
	Extra  map[string]any
}

func (e *SendMediaError) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// Fields is the JSON body the handler writes: {"error","code",...extra}.
func (e *SendMediaError) Fields() map[string]any {
	out := map[string]any{"error": e.Error(), "code": e.Code}
	for k, v := range e.Extra {
		out[k] = v
	}
	return out
}

func newSendMediaError(status int, code, detail string, extra map[string]any) *SendMediaError {
	return &SendMediaError{Status: status, Code: code, Detail: detail, Extra: extra}
}

// sendLog is the slice of the instance logger these steps need (tests pass a no-op).
type sendLog interface {
	LogInfo(format string, args ...interface{})
	LogWarn(format string, args ...interface{})
	LogError(format string, args ...interface{})
}

type noopSendLog struct{}

func (noopSendLog) LogInfo(string, ...interface{})  {}
func (noopSendLog) LogWarn(string, ...interface{})  {}
func (noopSendLog) LogError(string, ...interface{}) {}

// sourceHTTPClient fetches the media URL: no Client.Timeout (the ctx bounds the whole send), but every
// phase that can hang has its own limit.
var sourceHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
		MaxIdleConns:          8,
		IdleConnTimeout:       90 * time.Second,
	},
}

// sendMediaLimits are the knobs fetchMediaSource obeys (from config.Config in production).
type sendMediaLimits struct {
	maxBytes    int64 // SEND_MEDIA_MAX_BYTES; 0 = no cap
	streamAbove int64 // SEND_MEDIA_STREAM_ABOVE; 0 = streaming off
	httpClient  *http.Client
	acquireWait time.Duration
}

func (s *sendService) sendMediaLimits() sendMediaLimits {
	lim := sendMediaLimits{httpClient: sourceHTTPClient, acquireWait: sendAcquireWait}
	if s.config != nil {
		lim.maxBytes = s.config.SendMediaMaxBytes
		lim.streamAbove = s.config.SendMediaStreamAbove
	}
	return lim
}

// mediaSource is the fetched file: in memory (small path) or on disk (large path).
type mediaSource struct {
	data     []byte
	file     *os.File
	size     int64
	mimeType string
	release  func()
	closed   bool
}

func (m *mediaSource) streamed() bool { return m != nil && m.file != nil }

// Close removes the temp file and frees the send slot. Idempotent; safe on nil.
func (m *mediaSource) Close() {
	if m == nil || m.closed {
		return
	}
	m.closed = true
	mediaguard.RemoveTemp(m.file)
	m.file = nil
	if m.release != nil {
		m.release()
	}
}

// plaintextForThumb returns the plaintext bytes for preview generation: the in-memory data on the
// small path, the file contents on the large path when it is small enough, nil otherwise. Must be
// called BEFORE the upload (UploadReader encrypts the temp file in place).
func (m *mediaSource) plaintextForThumb() []byte {
	if !m.streamed() {
		return m.data
	}
	if m.size > pdfThumbMaxSourceBytes {
		return nil
	}
	b, err := os.ReadFile(m.file.Name())
	if err != nil {
		return nil
	}
	return b
}

// streamableType says whether the media type may use the large path (image needs the bytes for its
// thumbnail, audio goes through ffmpeg in memory, ptv is tiny).
func streamableType(mediaType string) bool {
	return mediaType == "document" || mediaType == "video"
}

func ctxError(err error) *SendMediaError {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return newSendMediaError(http.StatusGatewayTimeout, "timeout", "the send did not finish within SEND_MEDIA_TIMEOUT", nil)
	case errors.Is(err, context.Canceled):
		return newSendMediaError(499, "canceled", "request canceled by the caller", nil)
	}
	return nil
}

// fetchMediaSource GETs rawURL once and decides small vs large path by Content-Length and type.
func fetchMediaSource(ctx context.Context, rawURL, mediaType string, isNewsletter bool, lim sendMediaLimits, log sendLog) (*mediaSource, error) {
	if log == nil {
		log = noopSendLog{}
	}
	if lim.httpClient == nil {
		lim.httpClient = sourceHTTPClient
	}
	if lim.acquireWait <= 0 {
		lim.acquireWait = sendAcquireWait
	}

	// Anti-SSRF only once the operator lists the Storage hosts; empty list = today's behaviour.
	if mediaguard.HostsConfigured() {
		if _, err := mediaguard.ValidateStorageURL(rawURL); err != nil {
			return nil, newSendMediaError(http.StatusBadRequest, "url_not_allowed", err.Error(), nil)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, newSendMediaError(http.StatusBadRequest, "url_not_allowed", err.Error(), nil)
	}
	resp, err := lim.httpClient.Do(req)
	if err != nil {
		if ce := ctxError(err); ce != nil {
			return nil, ce
		}
		return nil, newSendMediaError(http.StatusBadGateway, "download_failed", err.Error(), map[string]any{"status": 0})
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		return nil, newSendMediaError(http.StatusBadGateway, "download_failed",
			fmt.Sprintf("source url answered %d", resp.StatusCode), map[string]any{"status": resp.StatusCode})
	}

	declared := resp.ContentLength // -1 when unknown
	if lim.maxBytes > 0 && declared > lim.maxBytes {
		return nil, newSendMediaError(http.StatusRequestEntityTooLarge, "too_large",
			fmt.Sprintf("declared %d bytes > SEND_MEDIA_MAX_BYTES %d", declared, lim.maxBytes),
			map[string]any{"declaredBytes": declared, "maxBytes": lim.maxBytes})
	}

	useStream := lim.streamAbove > 0 && !isNewsletter && streamableType(mediaType) && (declared < 0 || declared > lim.streamAbove)
	if !useStream {
		return readSmall(resp.Body, rawURL, lim)
	}
	return readLarge(ctx, resp.Body, rawURL, declared, lim, log)
}

// readSmall is today's path: whole body in memory, capped at maxBytes.
func readSmall(body io.Reader, rawURL string, lim sendMediaLimits) (*mediaSource, error) {
	r := body
	if lim.maxBytes > 0 {
		r = io.LimitReader(body, lim.maxBytes+1)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		if ce := ctxError(err); ce != nil {
			return nil, ce
		}
		return nil, newSendMediaError(http.StatusBadGateway, "download_failed", "reading source: "+err.Error(), map[string]any{"status": 200})
	}
	if lim.maxBytes > 0 && int64(len(data)) > lim.maxBytes {
		return nil, newSendMediaError(http.StatusRequestEntityTooLarge, "too_large",
			fmt.Sprintf("source larger than SEND_MEDIA_MAX_BYTES %d", lim.maxBytes),
			map[string]any{"maxBytes": lim.maxBytes})
	}
	mime, _ := mimetype.DetectReader(bytes.NewReader(data))
	return &mediaSource{data: data, size: int64(len(data)), mimeType: sniffedMime(mime, rawURL)}, nil
}

// readLarge copies the body to a temp file on disk under the send slot and the disk guard.
func readLarge(ctx context.Context, body io.Reader, rawURL string, declared int64, lim sendMediaLimits, log sendLog) (src *mediaSource, err error) {
	release, err := mediaguard.AcquireSendWithin(ctx, lim.acquireWait)
	if err != nil {
		if errors.Is(err, mediaguard.ErrBusy) {
			return nil, newSendMediaError(http.StatusServiceUnavailable, "busy", "another large send is in progress, retry shortly", nil)
		}
		if ce := ctxError(err); ce != nil {
			return nil, ce
		}
		return nil, newSendMediaError(http.StatusServiceUnavailable, "busy", err.Error(), nil)
	}
	// `cur` (not the named result) is what the defer cleans: a `return nil, err` already zeroed `src`
	// by the time the defer runs, and the temp file + slot would leak.
	cur := &mediaSource{release: release}
	defer func() {
		if err != nil {
			cur.Close()
		}
	}()

	need := declared
	if need < 0 {
		need = lim.maxBytes
	}
	if derr := mediaguard.EnsureDiskFor(need); derr != nil {
		log.LogWarn("send/media: %v", derr)
		return nil, newSendMediaError(http.StatusServiceUnavailable, "disk_low", derr.Error(), nil)
	}
	f, cerr := mediaguard.CreateTemp(mediaguard.TempPrefixSend)
	if cerr != nil {
		return nil, newSendMediaError(http.StatusServiceUnavailable, "disk_low", "cannot create temp file: "+cerr.Error(), nil)
	}
	cur.file = f

	r := body
	if lim.maxBytes > 0 {
		r = io.LimitReader(body, lim.maxBytes+1)
	}
	n, cpErr := io.Copy(f, r)
	if cpErr != nil {
		if ce := ctxError(cpErr); ce != nil {
			return nil, ce
		}
		return nil, newSendMediaError(http.StatusBadGateway, "download_failed", "reading source: "+cpErr.Error(), map[string]any{"status": 200})
	}
	if lim.maxBytes > 0 && n > lim.maxBytes {
		return nil, newSendMediaError(http.StatusRequestEntityTooLarge, "too_large",
			fmt.Sprintf("source larger than SEND_MEDIA_MAX_BYTES %d", lim.maxBytes),
			map[string]any{"declaredBytes": declared, "maxBytes": lim.maxBytes})
	}
	if declared >= 0 && n != declared {
		return nil, newSendMediaError(http.StatusBadGateway, "download_failed",
			fmt.Sprintf("source truncated: got %d of %d bytes", n, declared), map[string]any{"status": 200})
	}
	cur.size = n

	if _, serr := f.Seek(0, io.SeekStart); serr != nil {
		return nil, newSendMediaError(http.StatusInternalServerError, "internal", "seek: "+serr.Error(), nil)
	}
	mime, _ := mimetype.DetectReader(f) // reads the first 3 KB only
	if _, serr := f.Seek(0, io.SeekStart); serr != nil {
		return nil, newSendMediaError(http.StatusInternalServerError, "internal", "seek: "+serr.Error(), nil)
	}
	cur.mimeType = sniffedMime(mime, rawURL)
	return cur, nil
}

// sniffedMime keeps today's rule: detected type, except URLs ending in .mp4 are video/mp4.
func sniffedMime(mime *mimetype.MIME, rawURL string) string {
	out := "application/octet-stream"
	if mime != nil {
		out = mime.String()
	}
	if strings.HasSuffix(strings.ToLower(rawURL), ".mp4") {
		out = "video/mp4"
	}
	return out
}

// preparedMedia is everything decided before the upload: type checks, audio conversion, previews.
type preparedMedia struct {
	uploadType whatsmeow.MediaType
	mimeType   string
	duration   int // audio seconds
	jpegThumb  []byte
	thumbW     *uint32
	thumbH     *uint32
	pageCount  *uint32
}

// prepareMedia validates the detected type against the requested one and builds previews. Runs BEFORE
// the upload because UploadReader encrypts the temp file in place.
func (s *sendService) prepareMedia(src *mediaSource, data *MediaStruct, instance *instance_model.Instance) (*preparedMedia, error) {
	log := s.loggerWrapper.GetLogger(instance.Id)
	prep := &preparedMedia{mimeType: src.mimeType}
	log.LogInfo("[%s] Tipo MIME detectado: %s", instance.Id, src.mimeType)

	switch data.Type {
	case "image":
		if prep.mimeType != "image/jpeg" && prep.mimeType != "image/png" && prep.mimeType != "image/webp" {
			errMsg := fmt.Sprintf("Invalid file format: '%s'. Only 'image/jpeg', 'image/png' and 'image/webp' are accepted", prep.mimeType)
			return nil, errors.New(errMsg)
		}
		if prep.mimeType == "image/webp" {
			prep.mimeType = "image/jpeg"
		}
		prep.uploadType = whatsmeow.MediaImage
		// Generate a JPEG preview thumbnail for better client UX (iOS in particular). On failure
		// jpegThumb is nil and the message is sent without a preview rather than failing the request.
		prep.jpegThumb = makeJPEGThumbnail(src.data, 72)

	case "video", "ptv":
		if prep.mimeType != "video/mp4" {
			errMsg := fmt.Sprintf("Invalid file format: '%s'. Only 'video/mp4' are accepted", prep.mimeType)
			return nil, errors.New(errMsg)
		}
		prep.uploadType = whatsmeow.MediaVideo

	case "audio":
		processingStart := time.Now()
		log.LogInfo("[%s] Iniciando conversão de áudio...", instance.Id)
		converterApiUrl := ""
		converterApiKey := ""
		if s.config != nil {
			converterApiUrl = s.config.ApiAudioConverter
			converterApiKey = s.config.ApiAudioConverterKey
		}
		var convertedData []byte
		var err error
		if converterApiUrl == "" {
			log.LogInfo("[%s] Usando conversão local...", instance.Id)
			convertedData, prep.duration, err = convertAudioToOpusWithDuration(src.data)
		} else {
			log.LogInfo("[%s] Usando API de conversão...", instance.Id)
			convertedData, prep.duration, err = convertAudioWithApi(converterApiUrl, converterApiKey, ConvertAudio{Base64: base64.StdEncoding.EncodeToString(src.data)})
		}
		if err != nil {
			return nil, err
		}
		src.data = convertedData
		src.size = int64(len(convertedData))
		prep.mimeType = "audio/ogg; codecs=opus"
		prep.uploadType = whatsmeow.MediaAudio
		log.LogInfo("[%s] Conversão de áudio concluída em %v", instance.Id, time.Since(processingStart))

	case "document":
		prep.uploadType = whatsmeow.MediaDocument
		// For PDF documents, rasterize page 1 into a JPEG preview thumbnail. A missing pdftoppm, a
		// failure, or a streamed file above pdfThumbMaxSourceBytes yields nil and the document is sent
		// without a preview instead of failing the request.
		if prep.mimeType == "application/pdf" {
			if plain := src.plaintextForThumb(); plain != nil {
				prep.jpegThumb, prep.thumbW, prep.thumbH = makePDFThumbnail(plain, pdfThumbWidth)
				prep.pageCount = pdfPageCount(plain)
			}
		}

	default:
		return nil, errors.New("invalid media type")
	}
	return prep, nil
}

// mediaUploader is the slice of *whatsmeow.Client the upload step uses (tests pass a fake).
type mediaUploader interface {
	Upload(ctx context.Context, plaintext []byte, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	UploadReader(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	UploadNewsletter(ctx context.Context, data []byte, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
}

// uploadMediaSource uploads once. On the large path the temp file is both source and scratch:
// it is overwritten with ciphertext and streamed (disk 1x, RAM constant).
func uploadMediaSource(ctx context.Context, up mediaUploader, src *mediaSource, uploadType whatsmeow.MediaType, isNewsletter bool) (whatsmeow.UploadResponse, error) {
	var (
		uploaded whatsmeow.UploadResponse
		err      error
	)
	switch {
	case src.streamed():
		if _, err = src.file.Seek(0, io.SeekStart); err != nil {
			return uploaded, newSendMediaError(http.StatusInternalServerError, "internal", "seek: "+err.Error(), nil)
		}
		uploaded, err = up.UploadReader(ctx, src.file, src.file, uploadType)
	case isNewsletter:
		// Newsletter: upload without encryption
		uploaded, err = up.UploadNewsletter(ctx, src.data, uploadType)
	default:
		// Regular upload with encryption
		uploaded, err = up.Upload(ctx, src.data, uploadType)
	}
	if err != nil {
		if ce := ctxError(err); ce != nil {
			return uploaded, ce
		}
		return uploaded, newSendMediaError(http.StatusBadGateway, "upload_failed", err.Error(), nil)
	}
	return uploaded, nil
}

// buildMediaMessage assembles the proto exactly as before (field for field), with FileLength taken
// from the upload response (identical to len(plaintext) on the small path).
func buildMediaMessage(data *MediaStruct, prep *preparedMedia, uploaded whatsmeow.UploadResponse, isNewsletter bool) (*waE2E.Message, string, error) {
	var media *waE2E.Message
	var mediaType string
	mimeType := prep.mimeType

	switch data.Type {
	case "image":
		if isNewsletter {
			// Newsletter: sem criptografia (sem MediaKey e FileEncSHA256)
			media = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(data.Caption),
				URL:           &uploaded.URL,
				DirectPath:    &uploaded.DirectPath,
				Mimetype:      proto.String(mimeType),
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    &uploaded.FileLength,
				JPEGThumbnail: prep.jpegThumb,
			}}
		} else {
			// Normal: com criptografia
			media = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(data.Caption),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mimeType),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
				JPEGThumbnail: prep.jpegThumb,
			}}
		}
		mediaType = "ImageMessage"
	case "video":
		if isNewsletter {
			media = &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
				Caption:    proto.String(data.Caption),
				URL:        &uploaded.URL,
				DirectPath: &uploaded.DirectPath,
				Mimetype:   proto.String(mimeType),
				FileSHA256: uploaded.FileSHA256,
				FileLength: &uploaded.FileLength,
			}}
		} else {
			media = &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
				Caption:       proto.String(data.Caption),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mimeType),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			}}
		}
		mediaType = "VideoMessage"
	case "ptv":
		if isNewsletter {
			media = &waE2E.Message{PtvMessage: &waE2E.VideoMessage{
				URL:        &uploaded.URL,
				DirectPath: &uploaded.DirectPath,
				Mimetype:   proto.String(mimeType),
				FileSHA256: uploaded.FileSHA256,
				FileLength: &uploaded.FileLength,
			}}
		} else {
			media = &waE2E.Message{PtvMessage: &waE2E.VideoMessage{
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mimeType),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			}}
		}
		mediaType = "PtvMessage"
	case "audio":
		if isNewsletter {
			media = &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				URL:              &uploaded.URL,
				PTT:              proto.Bool(true),
				DirectPath:       &uploaded.DirectPath,
				Mimetype:         proto.String(mimeType),
				FileSHA256:       uploaded.FileSHA256,
				FileLength:       &uploaded.FileLength,
				StreamingSidecar: []byte(*proto.String("QpmXDsU7YLagdg==")),
				Waveform:         []byte(*proto.String("OjAnExISDgsKCAkJBwgkHAQEBBEFAwMNAxAcKCgkFzM0QUE4Jh4eKAoKChcLCwkeFgkJCQo3JiQmIiIRPz8/Ow==")),
				Seconds:          proto.Uint32(uint32(prep.duration)),
			}}
		} else {
			media = &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				URL:              proto.String(uploaded.URL),
				PTT:              proto.Bool(true),
				DirectPath:       proto.String(uploaded.DirectPath),
				MediaKey:         uploaded.MediaKey,
				Mimetype:         proto.String(mimeType),
				FileEncSHA256:    uploaded.FileEncSHA256,
				FileSHA256:       uploaded.FileSHA256,
				FileLength:       proto.Uint64(uploaded.FileLength),
				StreamingSidecar: []byte(*proto.String("QpmXDsU7YLagdg==")),
				Waveform:         []byte(*proto.String("OjAnExISDgsKCAkJBwgkHAQEBBEFAwMNAxAcKCgkFzM0QUE4Jh4eKAoKChcLCwkeFgkJCQo3JiQmIiIRPz8/Ow==")),
				Seconds:          proto.Uint32(uint32(prep.duration)),
			}}
		}
		mediaType = "AudioMessage"
	case "document":
		if isNewsletter {
			media = &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
				URL:             &uploaded.URL,
				FileName:        &data.Filename,
				Caption:         proto.String(data.Caption),
				DirectPath:      &uploaded.DirectPath,
				Mimetype:        proto.String(mimeType),
				FileSHA256:      uploaded.FileSHA256,
				FileLength:      &uploaded.FileLength,
				JPEGThumbnail:   prep.jpegThumb,
				ThumbnailWidth:  prep.thumbW,
				ThumbnailHeight: prep.thumbH,
				PageCount:       prep.pageCount,
			}}
		} else {
			media = &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
				URL:             proto.String(uploaded.URL),
				FileName:        &data.Filename,
				Caption:         proto.String(data.Caption),
				DirectPath:      proto.String(uploaded.DirectPath),
				MediaKey:        uploaded.MediaKey,
				Mimetype:        proto.String(mimeType),
				FileEncSHA256:   uploaded.FileEncSHA256,
				FileSHA256:      uploaded.FileSHA256,
				FileLength:      proto.Uint64(uploaded.FileLength),
				JPEGThumbnail:   prep.jpegThumb,
				ThumbnailWidth:  prep.thumbW,
				ThumbnailHeight: prep.thumbH,
				PageCount:       prep.pageCount,
			}}
		}

		if media.GetDocumentMessage().GetCaption() != "" {
			media.DocumentWithCaptionMessage = &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					DocumentMessage: media.DocumentMessage,
				},
			}
			media.DocumentMessage = nil
		}

		mediaType = "DocumentMessage"
	default:
		return nil, "", errors.New("invalid media type")
	}

	// view-once: when requested, flag the media so it disappears after the recipient opens it
	// once (image, video and voice/PTV support it; documents do not). Additive — without the
	// flag the media is a normal message, exactly as before.
	if data.ViewOnce {
		if media.ImageMessage != nil {
			media.ImageMessage.ViewOnce = proto.Bool(true)
		}
		if media.VideoMessage != nil {
			media.VideoMessage.ViewOnce = proto.Bool(true)
		}
		if media.AudioMessage != nil {
			media.AudioMessage.ViewOnce = proto.Bool(true)
		}
		if media.PtvMessage != nil {
			media.PtvMessage.ViewOnce = proto.Bool(true)
		}
	}
	return media, mediaType, nil
}

// isDisconnectError mirrors the string checks the retry loops have always used.
func isDisconnectError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "client disconnected") || strings.Contains(err.Error(), "no active session"))
}

// sendWithRetryOnDisconnect runs send up to maxRetries times. Before every retry it re-ensures the
// session (ensure); a non-disconnect error returns at once. Download and upload are NOT part of this
// loop — they happened once before it.
func sendWithRetryOnDisconnect(maxRetries int, ensure func() error, send func() (*MessageSendStruct, error), sleep func(time.Duration), log sendLog) (*MessageSendStruct, error) {
	if log == nil {
		log = noopSendLog{}
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			if err := ensure(); err != nil {
				if attempt == maxRetries {
					return nil, err
				}
				continue
			}
		}
		message, err := send()
		if err == nil {
			log.LogInfo("SendMediaUrl: mensagem enviada na tentativa %d", attempt)
			return message, nil
		}
		if isDisconnectError(err) && attempt < maxRetries {
			waitTime := time.Duration(attempt) * time.Second
			log.LogWarn("SendMediaUrl: sessão caiu na tentativa %d/%d (%v), esperando %v antes de reenviar (sem baixar/subir de novo)", attempt, maxRetries, err, waitTime)
			sleep(waitTime)
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("failed to send media url after %d attempts", maxRetries)
}
