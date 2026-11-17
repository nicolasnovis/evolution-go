package send_service

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/mediaguard"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestParseFFprobe_LandscapeAndDuration(t *testing.T) {
	info, ok := parseFFprobe([]byte(`{"streams":[{"width":1280,"height":720}],"format":{"duration":"12.600000"}}`))
	if !ok {
		t.Fatal("expected ok")
	}
	if info.width == nil || *info.width != 1280 || info.height == nil || *info.height != 720 {
		t.Fatalf("size = %v x %v, want 1280 x 720", info.width, info.height)
	}
	if info.seconds == nil || *info.seconds != 13 {
		t.Fatalf("seconds = %v, want 13 (rounded)", info.seconds)
	}
}

func TestParseFFprobe_DisplayMatrixRotationSwapsSize(t *testing.T) {
	// current ffmpeg: portrait phone video = landscape stream + display matrix rotation
	raw := `{"streams":[{"width":1920,"height":1080,"side_data_list":[{"rotation":-90}]}],"format":{"duration":"3.0"}}`
	info, _ := parseFFprobe([]byte(raw))
	if *info.width != 1080 || *info.height != 1920 {
		t.Fatalf("size = %d x %d, want 1080 x 1920 (rotated)", *info.width, *info.height)
	}
}

func TestParseFFprobe_RotateTagSwapsSize(t *testing.T) {
	// old files/ffmpeg: "rotate" stream tag
	raw := `{"streams":[{"width":640,"height":360,"tags":{"rotate":"270"}}],"format":{"duration":"1.0"}}`
	info, _ := parseFFprobe([]byte(raw))
	if *info.width != 360 || *info.height != 640 {
		t.Fatalf("size = %d x %d, want 360 x 640", *info.width, *info.height)
	}
}

func TestParseFFprobe_UpsideDownKeepsSize(t *testing.T) {
	raw := `{"streams":[{"width":640,"height":360,"side_data_list":[{"rotation":180}]}],"format":{}}`
	info, _ := parseFFprobe([]byte(raw))
	if *info.width != 640 || *info.height != 360 {
		t.Fatalf("size = %d x %d, want 640 x 360", *info.width, *info.height)
	}
	if info.seconds != nil {
		t.Fatalf("seconds = %d, want nil without a duration", *info.seconds)
	}
}

func TestParseFFprobe_SubSecondClipShowsOneSecond(t *testing.T) {
	info, _ := parseFFprobe([]byte(`{"streams":[],"format":{"duration":"0.4"}}`))
	if info.seconds == nil || *info.seconds != 1 {
		t.Fatalf("seconds = %v, want 1", info.seconds)
	}
}

func TestParseFFprobe_GarbageIsNotOK(t *testing.T) {
	for _, raw := range []string{``, `not json`, `{}`, `{"streams":[{"width":0,"height":0}],"format":{"duration":"N/A"}}`} {
		if _, ok := parseFFprobe([]byte(raw)); ok {
			t.Fatalf("parseFFprobe(%q) ok = true, want false", raw)
		}
	}
}

func TestApplyVideoPreview_EmptyPreviewLeavesMessageAsBefore(t *testing.T) {
	m := &waE2E.VideoMessage{Caption: proto.String("x")}
	applyVideoPreview(m, nil, videoPreview{})
	if m.JPEGThumbnail != nil || m.Width != nil || m.Height != nil || m.Seconds != nil {
		t.Fatalf("empty preview changed the message: %+v", m)
	}
	applyVideoPreview(nil, []byte{1}, videoPreview{}) // must not panic
}

func TestApplyVideoPreview_FillsFields(t *testing.T) {
	m := &waE2E.VideoMessage{}
	applyVideoPreview(m, []byte{1, 2}, videoPreview{width: proto.Uint32(360), height: proto.Uint32(640), seconds: proto.Uint32(5)})
	if len(m.JPEGThumbnail) != 2 || m.GetWidth() != 360 || m.GetHeight() != 640 || m.GetSeconds() != 5 {
		t.Fatalf("fields not filled: %+v", m)
	}
}

func TestFitJPEG_NeverAboveTheCap(t *testing.T) {
	// noise is the worst case for JPEG: the result is either under the cap or nil, never above it
	img := image.NewRGBA(image.Rect(0, 0, 480, 854))
	r := rand.New(rand.NewSource(1))
	for i := range img.Pix {
		img.Pix[i] = byte(r.Intn(256))
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	if out := fitJPEG(buf.Bytes(), videoThumbMaxBytes); out != nil && len(out) > videoThumbMaxBytes {
		t.Fatalf("thumbnail has %d bytes, cap is %d", len(out), videoThumbMaxBytes)
	}
	if fitJPEG([]byte("not a png"), videoThumbMaxBytes) != nil {
		t.Fatal("expected nil for a frame that does not decode")
	}
}

func TestFitJPEG_FlatFrameFits(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 480, 270))
	for y := 0; y < 270; y++ {
		for x := 0; x < 480; x++ {
			img.Set(x, y, color.RGBA{R: 200, G: 40, B: 40, A: 255})
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	out := fitJPEG(buf.Bytes(), videoThumbMaxBytes)
	if out == nil {
		t.Fatal("expected a thumbnail")
	}
	if _, err := jpeg.Decode(bytes.NewReader(out)); err != nil {
		t.Fatalf("not a JPEG: %v", err)
	}
}

// ─── real videos (skipped where ffmpeg/ffprobe are not installed) ────────────────────────────────

func needFFmpeg(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not installed", bin)
		}
	}
}

// makeTestVideo renders an mp4 with ffmpeg's test pattern. The default mp4 muxer writes the index
// (moov) at the END, the case a pipe cannot read.
func makeTestVideo(t *testing.T, size string, seconds string, extra ...string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "v.mp4")
	args := []string{"-v", "error", "-f", "lavfi", "-i", "testsrc=duration=" + seconds + ":size=" + size + ":rate=25",
		"-pix_fmt", "yuv420p", "-c:v", "libx264"}
	args = append(append(args, extra...), out)
	if b, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot render the test video here: %v %s", err, b)
	}
	return out
}

func thumbSize(t *testing.T, thumb []byte) (int, int) {
	t.Helper()
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(thumb))
	if err != nil {
		t.Fatalf("thumbnail is not a JPEG: %v", err)
	}
	return cfg.Width, cfg.Height
}

func TestVideoPreviewFromFile_RealVideo(t *testing.T) {
	needFFmpeg(t)
	p := videoPreviewFromFile(makeTestVideo(t, "640x360", "3"))
	if p.thumb == nil {
		t.Fatal("expected a thumbnail")
	}
	if len(p.thumb) > videoThumbMaxBytes {
		t.Fatalf("thumbnail has %d bytes", len(p.thumb))
	}
	if w, h := thumbSize(t, p.thumb); w != videoThumbWidth || h != 270 {
		t.Fatalf("thumbnail %dx%d, want %dx270", w, h, videoThumbWidth)
	}
	if p.width == nil || *p.width != 640 || *p.height != 360 {
		t.Fatalf("size = %v x %v, want 640 x 360", p.width, p.height)
	}
	if p.seconds == nil || *p.seconds != 3 {
		t.Fatalf("seconds = %v, want 3", p.seconds)
	}
}

func TestVideoPreviewFromBytes_IndexAtTheEnd(t *testing.T) {
	needFFmpeg(t)
	data, err := os.ReadFile(makeTestVideo(t, "320x240", "2"))
	if err != nil {
		t.Fatal(err)
	}
	p := videoPreviewFromBytes(data)
	if p.thumb == nil || p.seconds == nil || *p.seconds != 2 {
		t.Fatalf("preview from memory failed: thumb=%d bytes, seconds=%v", len(p.thumb), p.seconds)
	}
	// small source: never upscaled
	if w, _ := thumbSize(t, p.thumb); w != 320 {
		t.Fatalf("thumbnail width %d, want 320 (no upscale)", w)
	}
}

func TestVideoPreview_ShortClipUsesFirstFrame(t *testing.T) {
	needFFmpeg(t)
	p := videoPreviewFromFile(makeTestVideo(t, "320x240", "0.5"))
	if p.thumb == nil {
		t.Fatal("expected a thumbnail for a half-second clip")
	}
	if p.seconds == nil || *p.seconds != 1 {
		t.Fatalf("seconds = %v, want 1", p.seconds)
	}
}

func TestVideoPreview_PortraitPhoneVideo(t *testing.T) {
	needFFmpeg(t)
	src := makeTestVideo(t, "640x360", "2")
	rotated := filepath.Join(t.TempDir(), "r.mp4")
	if b, err := exec.Command("ffmpeg", "-v", "error", "-display_rotation", "90", "-i", src, "-c", "copy", rotated).CombinedOutput(); err != nil {
		t.Skipf("this ffmpeg cannot write a display rotation: %v %s", err, b)
	}
	p := videoPreviewFromFile(rotated)
	if p.width == nil || *p.width != 360 || *p.height != 640 {
		t.Fatalf("size = %v x %v, want 360 x 640 (portrait)", p.width, p.height)
	}
	if p.thumb == nil {
		t.Fatal("expected a thumbnail")
	}
	if w, h := thumbSize(t, p.thumb); w >= h {
		t.Fatalf("thumbnail %dx%d should be portrait (ffmpeg applies the rotation)", w, h)
	}
}

func TestVideoPreview_NotAVideoGivesNothing(t *testing.T) {
	needFFmpeg(t)
	p := videoPreviewFromBytes([]byte("definitely not an mp4"))
	if p.thumb != nil || p.width != nil || p.height != nil || p.seconds != nil {
		t.Fatalf("expected an empty preview, got %+v", p)
	}
	if p := videoPreviewFromBytes(nil); p.thumb != nil {
		t.Fatal("expected an empty preview for no bytes")
	}
}

func TestVideoPreview_OnlyTheMP4DemuxerIsUsed(t *testing.T) {
	// a URL ending in .mp4 passes as video/mp4 whatever it holds: a real video in another container
	// must not be opened by a guessed demuxer (the same guess that opens URLs, see below)
	needFFmpeg(t)
	mkv := filepath.Join(t.TempDir(), "v.mkv")
	args := []string{"-v", "error", "-f", "lavfi", "-i", "testsrc=duration=2:size=320x240:rate=25",
		"-pix_fmt", "yuv420p", "-c:v", "libx264", mkv}
	if b, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot render the test video here: %v %s", err, b)
	}
	if p := videoPreviewFromFile(mkv); p.thumb != nil || p.width != nil || p.seconds != nil {
		t.Fatalf("a Matroska file got a preview (thumb %d bytes), so the input format is being guessed", len(p.thumb))
	}
}

func TestVideoPreview_ManifestCannotMakeFFmpegFetchURLs(t *testing.T) {
	// a DASH manifest saved as "x.mp4": with a guessed demuxer ffmpeg GETs the address inside it
	// (SSRF from the VPS). Skipped where ffmpeg has no DASH demuxer (it needs libxml2; the image has it).
	needFFmpeg(t)
	if out, err := exec.Command("ffmpeg", "-hide_banner", "-demuxers").Output(); err != nil || !strings.Contains(string(out), " dash ") {
		t.Skip("this ffmpeg has no DASH demuxer")
	}
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.NotFound(w, r)
	}))
	defer srv.Close()
	mpd := `<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" mediaPresentationDuration="PT3S" minBufferTime="PT1S" profiles="urn:mpeg:dash:profile:isoff-on-demand:2011">
<BaseURL>` + srv.URL + `/</BaseURL>
<Period><AdaptationSet mimeType="video/mp4"><Representation id="1" bandwidth="1000" width="320" height="240"><BaseURL>seg.mp4</BaseURL></Representation></AdaptationSet></Period></MPD>
`
	p := videoPreviewFromBytes([]byte(mpd))
	if n := hits.Load(); n != 0 {
		t.Fatalf("ffmpeg fetched %d URL(s) named inside the file", n)
	}
	if p.thumb != nil || p.width != nil || p.seconds != nil {
		t.Fatalf("expected an empty preview, got thumb %d bytes, width %v, seconds %v", len(p.thumb), p.width, p.seconds)
	}
}

// prepare → upload → build, on both paths: the preview reaches the VideoMessage/PtvMessage, the
// uploaded bytes are the untouched video, and no temp file is left behind.
func TestPrepareAndBuild_VideoCarriesPreview(t *testing.T) {
	needFFmpeg(t)
	video, err := os.ReadFile(makeTestVideo(t, "640x360", "3"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		typ      string
		streamed bool
	}{
		{"video in memory", "video", false},
		{"video streamed", "video", true},
		{"ptv in memory", "ptv", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := guardDir(t)
			s := testService(t)
			src := &mediaSource{data: video, size: int64(len(video)), mimeType: "video/mp4"}
			if tc.streamed {
				f, err := mediaguard.CreateTemp(mediaguard.TempPrefixSend)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write(video); err != nil {
					t.Fatal(err)
				}
				src = &mediaSource{file: f, size: int64(len(video)), mimeType: "video/mp4"}
			}
			data := &MediaStruct{Number: "5500000000000", Type: tc.typ}
			prep, err := s.prepareMedia(src, data, ptr(instanceFor("inst")))
			if err != nil {
				t.Fatal(err)
			}
			up := &fakeUploader{}
			uploaded, err := uploadMediaSource(context.Background(), up, src, prep.uploadType, false)
			if err != nil {
				t.Fatal(err)
			}
			if up.sha256 != shaOf(video) {
				t.Fatal("the upload did not get the original video bytes")
			}
			msg, _, err := buildMediaMessage(data, prep, uploaded, false)
			if err != nil {
				t.Fatal(err)
			}
			v := msg.GetVideoMessage()
			if tc.typ == "ptv" {
				v = msg.GetPtvMessage()
			}
			if v == nil || len(v.GetJPEGThumbnail()) == 0 || v.GetWidth() != 640 || v.GetHeight() != 360 || v.GetSeconds() != 3 {
				t.Fatalf("preview missing: thumb %d bytes, %dx%d, %ds", len(v.GetJPEGThumbnail()), v.GetWidth(), v.GetHeight(), v.GetSeconds())
			}
			src.Close()
			assertDirEmpty(t, dir)
		})
	}
}

func TestVideoPreview_BusySlotsGoWithoutPreview(t *testing.T) {
	needFFmpeg(t)
	path := makeTestVideo(t, "320x240", "2")
	old := videoPreviewSlotWait
	videoPreviewSlotWait = 50 * time.Millisecond
	t.Cleanup(func() { videoPreviewSlotWait = old })
	// take every slot, as concurrent sends would
	for i := 0; i < videoPreviewConcurrency; i++ {
		videoPreviewSlots <- struct{}{}
	}
	if p := videoPreviewFromFile(path); p.thumb != nil || p.seconds != nil {
		t.Fatal("with every slot busy the video must go without a preview")
	}
	for i := 0; i < videoPreviewConcurrency; i++ {
		<-videoPreviewSlots
	}
	if p := videoPreviewFromFile(path); p.thumb == nil {
		t.Fatal("with a free slot the preview must come back")
	}
	if len(videoPreviewSlots) != 0 {
		t.Fatalf("%d slot(s) leaked", len(videoPreviewSlots))
	}
}

func TestVideoPreviewFromBytes_ReleasesDiskReservation(t *testing.T) {
	needFFmpeg(t)
	data, err := os.ReadFile(makeTestVideo(t, "320x240", "2"))
	if err != nil {
		t.Fatal(err)
	}
	before := mediaguard.DiskReserved()
	if p := videoPreviewFromBytes(data); p.thumb == nil {
		t.Fatal("expected a thumbnail")
	}
	if after := mediaguard.DiskReserved(); after != before {
		t.Fatalf("disk reservation leaked: %d before, %d after", before, after)
	}
	if len(videoPreviewSlots) != 0 {
		t.Fatalf("%d slot(s) leaked", len(videoPreviewSlots))
	}
}

func TestVideoPreview_CoverIsTheOpeningShot(t *testing.T) {
	needFFmpeg(t)
	// 1 s red, then 2 s blue: the thumbnail must be the opening (red) shot, as the owner asked
	out := filepath.Join(t.TempDir(), "abertura.mp4")
	cmd := exec.Command("ffmpeg", "-v", "error",
		"-f", "lavfi", "-i", "color=c=red:s=320x240:d=1:r=25",
		"-f", "lavfi", "-i", "color=c=blue:s=320x240:d=2:r=25",
		"-filter_complex", "[0:v][1:v]concat=n=2:v=1[v]", "-map", "[v]", "-pix_fmt", "yuv420p", out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot render the test video here: %v %s", err, b)
	}
	p := videoPreviewFromFile(out)
	if p.thumb == nil {
		t.Fatal("expected a thumbnail")
	}
	img, err := jpeg.Decode(bytes.NewReader(p.thumb))
	if err != nil {
		t.Fatal(err)
	}
	b := img.Bounds()
	r, _, bl, _ := img.At(b.Dx()/2, b.Dy()/2).RGBA()
	if r <= bl {
		t.Fatalf("thumbnail is not the opening shot: centre pixel r=%d b=%d (want red)", r>>8, bl>>8)
	}
}
