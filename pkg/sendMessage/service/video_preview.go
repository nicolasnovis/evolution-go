package send_service

import (
	"bytes"
	"context"
	"encoding/json"
	"image/jpeg"
	"image/png"
	"math"
	"os/exec"
	"strconv"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/mediaguard"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// Video preview for /send/media. The official app sends every video with an inline JPEG frame
// (JPEGThumbnail) plus its duration and size. Without them the recipient sees a black box with only
// the download size until they tap it (17/11/2026: a video sent to a group arrived exactly like that).
// ffmpeg/ffprobe already ship in the image (audio conversion), so the frame comes from there.
//
// Best effort, like the image and PDF previews: a missing binary, a timeout, low disk or an odd file
// yields an empty preview and the video is sent exactly as before.

const (
	// videoThumbWidth: the client stretches the inline thumbnail to the bubble width, so the 72 px
	// used for images would look like a smudge. 480 px stays well under videoThumbMaxBytes.
	videoThumbWidth = 480
	// videoThumbMaxBytes caps the inline JPEG: it travels inside the (encrypted) message itself.
	// Same ceiling as the PDF preview.
	videoThumbMaxBytes = 100 * 1024
	// videoPreviewTimeout bounds ffprobe + ffmpeg together (the send has its own deadline).
	videoPreviewTimeout = 20 * time.Second
	// videoFrameAt: the cover is the opening shot, like the app shows it (asked by the owner on
	// 17/11/2026: videos often open on a title card). 0.1 s instead of 0 because some encoders put a
	// blank or half-decoded picture at t=0; videos shorter than twice that use the very first frame.
	videoFrameAt = 0.1
	// videoInputFormat pins ffmpeg/ffprobe to the ISO-BMFF (mp4) demuxer instead of letting them guess
	// from the content. Only video/mp4 reaches here, but a URL ending in .mp4 is taken as video/mp4
	// whatever it holds (sniffedMime), and a guessed demuxer can open URLs by itself: a DASH manifest
	// makes ffmpeg GET any address from the VPS (checked on the image's ffmpeg 6.1.1), past
	// MEDIA_UPLOAD_ALLOWED_HOSTS. The mov demuxer reads only this file (external data references are
	// off by default); anything that is not an mp4 simply yields no preview.
	videoInputFormat = "mov"
)

// videoPreview is what goes into the VideoMessage besides the upload fields. Every field may be nil.
type videoPreview struct {
	thumb   []byte
	width   *uint32 // display size: a phone video recorded in portrait reports its rotation
	height  *uint32
	seconds *uint32
}

// applyVideoPreview fills the preview fields of a video/ptv message. Fields ffmpeg could not produce
// stay nil, which is exactly the message sent before this preview existed.
func applyVideoPreview(m *waE2E.VideoMessage, thumb []byte, p videoPreview) {
	if m == nil {
		return
	}
	m.JPEGThumbnail = thumb
	m.Width, m.Height, m.Seconds = p.width, p.height, p.seconds
}

const (
	// videoPreviewConcurrency caps how many previews decode at once. The large path already sends one
	// file at a time, but the in-memory path does not: a burst of 4K/HEVC videos would each start an
	// ffmpeg (~150 MB at 1 thread) on top of the bytes the sends already hold.
	videoPreviewConcurrency = 2
)

var (
	videoPreviewSlots = make(chan struct{}, videoPreviewConcurrency)
	// videoPreviewSlotWait: how long a send waits for a preview slot before going without a preview
	// (a var so tests can shorten it).
	videoPreviewSlotWait = 10 * time.Second
)

// acquirePreviewSlot waits up to videoPreviewSlotWait for a slot; false = go without a preview.
func acquirePreviewSlot() (release func(), ok bool) {
	timer := time.NewTimer(videoPreviewSlotWait)
	defer timer.Stop()
	select {
	case videoPreviewSlots <- struct{}{}:
		return func() { <-videoPreviewSlots }, true
	case <-timer.C:
		return func() {}, false
	}
}

// videoPreviewFromFile reads the preview of a PLAINTEXT video on disk. On the large path it must run
// before the upload: UploadReader encrypts the temp file in place.
func videoPreviewFromFile(path string) videoPreview {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return videoPreview{}
	}
	release, ok := acquirePreviewSlot()
	if !ok {
		return videoPreview{}
	}
	defer release()
	return previewOfFile(path)
}

// previewOfFile runs ffprobe + ffmpeg on a plaintext video (the caller holds a preview slot).
func previewOfFile(path string) videoPreview {
	ctx, cancel := context.WithTimeout(context.Background(), videoPreviewTimeout)
	defer cancel()

	var out videoPreview
	duration := 0.0
	if info, ok := probeVideo(ctx, path); ok {
		out.width, out.height, out.seconds = info.width, info.height, info.seconds
		duration = info.duration
	}
	out.thumb = videoFrame(ctx, path, duration)
	return out
}

// videoPreviewFromBytes is the in-memory path. ffmpeg needs a seekable input (an mp4 whose index
// sits at the end of the file cannot be read from a pipe), so the bytes go to a short-lived temp file
// first, under the same disk guard as every other temp file (tx- prefix: swept on boot if orphaned).
func videoPreviewFromBytes(data []byte) videoPreview {
	if len(data) == 0 {
		return videoPreview{}
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return videoPreview{}
	}
	// the slot first: a send waiting for one must not hold a disk reservation or a temp copy meanwhile
	releaseSlot, ok := acquirePreviewSlot()
	if !ok {
		return videoPreview{}
	}
	defer releaseSlot()
	// ReserveDisk (not EnsureDiskFor): counts what the other in-flight transfers were promised.
	// Deferred first, so it is released only after the temp file below is gone.
	releaseDisk, _, err := mediaguard.ReserveDisk(int64(len(data)))
	if err != nil {
		return videoPreview{}
	}
	defer releaseDisk()
	f, err := mediaguard.CreateTemp(mediaguard.TempPrefixSend)
	if err != nil {
		return videoPreview{}
	}
	defer mediaguard.RemoveTemp(f)
	if _, err := f.Write(data); err != nil {
		return videoPreview{}
	}
	return previewOfFile(f.Name())
}

type videoInfo struct {
	width, height, seconds *uint32
	duration               float64
}

func probeVideo(ctx context.Context, path string) (videoInfo, bool) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-f", videoInputFormat, "-select_streams", "v:0",
		"-show_entries", "stream=width,height:stream_tags=rotate:stream_side_data=rotation:format=duration",
		"-of", "json", path)
	raw, err := cmd.Output()
	if err != nil {
		return videoInfo{}, false
	}
	return parseFFprobe(raw)
}

type ffprobeOutput struct {
	Streams []struct {
		Width  int `json:"width"`
		Height int `json:"height"`
		Tags   struct {
			Rotate string `json:"rotate"`
		} `json:"tags"`
		SideData []struct {
			Rotation *float64 `json:"rotation"`
		} `json:"side_data_list"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

// parseFFprobe turns the ffprobe JSON into the message fields. A portrait phone video is stored
// landscape with a rotation (display matrix side data on current ffmpeg, a "rotate" tag on old
// files): width/height are swapped so the bubble gets the shape the viewer will see.
func parseFFprobe(raw []byte) (videoInfo, bool) {
	var p ffprobeOutput
	if err := json.Unmarshal(raw, &p); err != nil {
		return videoInfo{}, false
	}
	var info videoInfo
	if d, err := strconv.ParseFloat(p.Format.Duration, 64); err == nil && d > 0 && !math.IsInf(d, 0) {
		info.duration = d
		s := math.Round(d)
		if s < 1 {
			s = 1 // the bubble shows 0:01 rather than 0:00 for a sub-second clip
		}
		info.seconds = proto.Uint32(uint32(s))
	}
	if len(p.Streams) > 0 {
		st := p.Streams[0]
		w, h := st.Width, st.Height
		if w > 0 && h > 0 {
			rotation := 0.0
			for _, sd := range st.SideData {
				if sd.Rotation != nil {
					rotation = *sd.Rotation
					break
				}
			}
			if rotation == 0 && st.Tags.Rotate != "" {
				if r, err := strconv.ParseFloat(st.Tags.Rotate, 64); err == nil {
					rotation = r
				}
			}
			if int(math.Abs(math.Round(rotation)))%180 == 90 {
				w, h = h, w
			}
			info.width, info.height = proto.Uint32(uint32(w)), proto.Uint32(uint32(h))
		}
	}
	return info, info.seconds != nil || info.width != nil
}

// videoFrame grabs one frame as a JPEG no wider than videoThumbWidth and no heavier than
// videoThumbMaxBytes. ffmpeg applies the rotation when decoding, so the frame is already upright.
// nil on any failure.
func videoFrame(ctx context.Context, path string, duration float64) []byte {
	at := 0.0
	if duration >= 2*videoFrameAt {
		at = videoFrameAt
	}
	frame := grabFrame(ctx, path, at)
	if frame == nil && at > 0 {
		frame = grabFrame(ctx, path, 0)
	}
	if frame == nil {
		return nil
	}
	return fitJPEG(frame, videoThumbMaxBytes)
}

// grabFrame asks ffmpeg for a PNG (lossless, so the only JPEG pass is fitJPEG's and no ffmpeg
// version trips on the mjpeg encoder's colour-range rules). One decoding thread: frame threading
// keeps one frame per thread in RAM, and a 4K/HEVC phone video on several concurrent sends would
// press this small VPS (it already went OOM on media once).
func grabFrame(ctx context.Context, path string, at float64) []byte {
	args := []string{"-v", "error", "-threads", "1", "-f", videoInputFormat}
	if at > 0 {
		args = append(args, "-ss", strconv.FormatFloat(at, 'f', 3, 64))
	}
	args = append(args, "-i", path, "-frames:v", "1", "-filter_threads", "1",
		"-vf", "scale='min("+strconv.Itoa(videoThumbWidth)+",iw)':-2",
		"-f", "image2pipe", "-c:v", "png", "pipe:1")
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil || out.Len() == 0 {
		return nil
	}
	return out.Bytes()
}

// fitJPEG encodes the PNG frame as JPEG, stepping the quality down until it fits max.
func fitJPEG(pngFrame []byte, max int) []byte {
	img, err := png.Decode(bytes.NewReader(pngFrame))
	if err != nil {
		return nil
	}
	for _, q := range []int{80, 70, 60, 50, 40} {
		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: q}); err != nil {
			return nil
		}
		if buf.Len() <= max {
			return buf.Bytes()
		}
	}
	return nil
}
