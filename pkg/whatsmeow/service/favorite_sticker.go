package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// Favorite stickers (N1 PATCH). WhatsApp syncs the user's starred stickers to every linked device
// through app state (the favoriteSticker collection). The CRM mirrors them in its sticker library:
// each favorite is downloaded here and posted as base64 on a "FavoriteSticker" webhook; an unfavorite
// or removal goes out without media. A full sync of the collection replays every current favorite
// (see whatsmeow-lib events.FavoriteSticker), which is how POST /chat/sync-stickers pulls the list.

// stickerDownloads caps concurrent downloads per process: a full sync can replay dozens at once.
var stickerDownloads = make(chan struct{}, 4)

const stickerMaxBytes = 2 << 20 // WhatsApp stickers are ≤ ~1 MB; anything bigger is not a sticker

func (mycli *MyClient) sendFavoriteSticker(evt *events.FavoriteSticker) {
	log := mycli.loggerWrapper.GetLogger(mycli.userID)
	act := evt.Action
	favorite := !evt.Removed && act != nil && act.GetIsFavorite()
	data := map[string]interface{}{
		"hash":         evt.Hash,
		"favorite":     favorite,
		"fromFullSync": evt.FromFullSync,
		"timestamp":    evt.Timestamp.Unix(),
	}

	if favorite {
		data["mimetype"] = act.GetMimetype()
		data["width"] = act.GetWidth()
		data["height"] = act.GetHeight()
		data["isAvatar"] = act.GetIsAvatarSticker()
		switch {
		case act.GetIsLottie():
			// Lottie (animated vector) stickers are a WhatsApp-only format the CRM can't render or resend.
			data["skipped"] = "lottie"
		case !strings.HasPrefix(act.GetDirectPath(), "/"):
			data["skipped"] = "no-direct-path"
		default:
			if b, err := mycli.downloadFavoriteSticker(act.GetDirectPath(), act.GetFileEncSHA256(), act.GetMediaKey(), act.GetImageHash()); err != nil {
				log.LogWarn("[%s] favorite sticker %s: download falhou: %v", mycli.userID, evt.Hash, err)
				data["skipped"] = "download-failed"
			} else {
				data["base64"] = base64.StdEncoding.EncodeToString(b)
			}
		}
	}

	postMap := map[string]interface{}{
		"event":         "FavoriteSticker",
		"data":          data,
		"instanceToken": mycli.token,
		"instanceId":    mycli.userID,
		"instanceName":  mycli.Instance.Name,
	}
	values, err := json.Marshal(postMap)
	if err != nil {
		log.LogError("[%s] favorite sticker %s: marshal falhou: %v", mycli.userID, evt.Hash, err)
		return
	}
	queueName := strings.ToLower(fmt.Sprintf("%s.favoritesticker", mycli.userID))
	log.LogInfo("[%s] FavoriteSticker %s favorite=%v fullSync=%v (%d bytes)", mycli.userID, evt.Hash, favorite, evt.FromFullSync, len(values))
	mycli.service.CallWebhook(mycli.Instance, queueName, values)
}

func (mycli *MyClient) downloadFavoriteSticker(directPath string, encHash, mediaKey []byte, imageHash string) ([]byte, error) {
	stickerDownloads <- struct{}{}
	defer func() { <-stickerDownloads }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// imageHash is the plaintext sha256 when present; without it the MAC check alone authenticates.
	var fileHash []byte
	if h, err := base64.StdEncoding.DecodeString(imageHash); err == nil && len(h) == 32 {
		fileHash = h
	}
	b, err := mycli.WAClient.DownloadMediaWithPath(ctx, directPath, encHash, fileHash, mediaKey, whatsmeow.MediaImage, "", true)
	if err != nil && fileHash != nil && errors.Is(err, whatsmeow.ErrInvalidMediaSHA256) {
		b, err = mycli.WAClient.DownloadMediaWithPath(ctx, directPath, encHash, nil, mediaKey, whatsmeow.MediaImage, "", true)
	}
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || len(b) > stickerMaxBytes {
		return nil, fmt.Errorf("tamanho inesperado: %d bytes", len(b))
	}
	return b, nil
}
