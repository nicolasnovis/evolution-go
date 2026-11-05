package message_handler

import (
	"errors"
	"net/http"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	message_service "github.com/evolution-foundation/evolution-go/pkg/message/service"
	"github.com/gin-gonic/gin"
)

type MessageHandler interface {
	React(ctx *gin.Context)
	ChatPresence(ctx *gin.Context)
	SubscribePresence(ctx *gin.Context)
	MarkRead(ctx *gin.Context)
	MarkPlayed(ctx *gin.Context)
	DownloadMedia(ctx *gin.Context)
	DownloadMediaToUrl(ctx *gin.Context)
	GetMessageStatus(ctx *gin.Context)
	DeleteMessageEveryone(ctx *gin.Context)
	EditMessage(ctx *gin.Context)
	PinMessage(ctx *gin.Context)
	VotePoll(ctx *gin.Context)
}

type messageHandler struct {
	messageService message_service.MessageService
}

// React a message
// @Summary React a message
// @Description React to a message with support for fromMe field and participant field for group messages
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.ReactStruct true "React to a message with fromMe and participant fields"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/react [post]
func (m *messageHandler) React(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.ReactStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Number == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "phone number is required"})
		return
	}

	if data.Reaction == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "message reaction is required"})
		return
	}

	message, err := m.messageService.React(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": message})
}

// ChatPresence set chat presence
// @Summary Set chat presence
// @Description Set chat presence
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.ChatPresenceStruct true "Set chat presence"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/presence [post]
func (m *messageHandler) ChatPresence(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.ChatPresenceStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Number == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "phone number is required"})
		return
	}

	if data.State == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "state is required"})
		return
	}

	ts, err := m.messageService.ChatPresence(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// SubscribePresence subscribe to a contact's presence (online / last-seen)
// @Summary Subscribe to a contact's presence
// @Description Subscribe to a contact's presence so the instance starts receiving Presence (online/offline/last-seen) webhook events for that number
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.SubscribePresenceStruct true "Number to subscribe presence for"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/subscribe [post]
func (m *messageHandler) SubscribePresence(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.SubscribePresenceStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Number == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "phone number is required"})
		return
	}

	if err := m.messageService.SubscribePresence(data, instance); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// MarkRead mark a message as read
// @Summary Mark a message as read
// @Description Mark a message as read
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.MarkReadStruct true "Mark a message as read"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/markread [post]
func (m *messageHandler) MarkRead(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.MarkReadStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Number == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "phone number is required"})
		return
	}

	if len(data.Id) < 1 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}

	ts, err := m.messageService.MarkRead(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// MarkPlayed mark an audio message as played (blue mic icon)
// @Summary Mark an audio message as played
// @Description Mark an audio message as played
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.MarkPlayedStruct true "Mark an audio message as played"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/markplayed [post]
func (m *messageHandler) MarkPlayed(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.MarkPlayedStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Number == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "phone number is required"})
		return
	}

	if len(data.Id) < 1 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}

	ts, err := m.messageService.MarkPlayed(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// DownloadMedia download a media message (image, video, audio, document)
// @Summary Download media
// @Description Download the media content of a message (image, video, audio or document)
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.DownloadMediaStruct true "Download media"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/downloadmedia [post]
func (m *messageHandler) DownloadMedia(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.DownloadMediaStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	dataUrl, ts, err := m.messageService.DownloadMedia(data, instance, ctx.Request)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"base64":    dataUrl.String(),
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// DownloadMediaToUrl streams a media message to a signed upload URL (no base64, no RAM copy)
// @Summary Download media straight to a Storage upload URL
// @Description Decrypts the media to a temp file on disk and PUTs it to uploadUrl (https, host in MEDIA_UPLOAD_ALLOWED_HOSTS). Errors carry a stable "code": too_large (413), expired (410), corrupt (422), busy/disk_low/not_configured/disconnected (503), timeout (504), storage_rejected/download_failed (502), upload_url_not_allowed/invalid_media (400).
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.DownloadMediaToUrlStruct true "Media proto + signed upload URL"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 410 {object} gin.H "Media expired on WhatsApp"
// @Failure 413 {object} gin.H "Media above maxBytes"
// @Failure 422 {object} gin.H "Media corrupt"
// @Failure 503 {object} gin.H "Busy / disk low / not configured"
// @Router /message/downloadmedia-to-url [post]
func (m *messageHandler) DownloadMediaToUrl(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found", "code": "internal"})
		return
	}

	var data *message_service.DownloadMediaToUrlStruct
	if err := ctx.ShouldBindBodyWithJSON(&data); err != nil || data == nil {
		detail := "invalid body"
		if err != nil {
			detail = err.Error()
		}
		ctx.JSON(http.StatusBadRequest, gin.H{"error": detail, "code": "bad_request"})
		return
	}
	if data.Message == nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "message is required", "code": "bad_request"})
		return
	}
	if data.UploadUrl == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "uploadUrl is required", "code": "bad_request"})
		return
	}

	result, err := m.messageService.DownloadMediaToUrl(data, instance, ctx.Request)
	if err != nil {
		var typed *message_service.MediaToUrlError
		if errors.As(err, &typed) {
			ctx.JSON(typed.Status, typed.Fields())
			return
		}
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "code": "internal"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": result})
}

// GetMessageStatus get message status
// @Summary Get message status
// @Description Get message status
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.MessageStatusStruct true "Get message status"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/status [post]
func (m *messageHandler) GetMessageStatus(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.MessageStatusStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Id == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}

	message, ts, err := m.messageService.GetMessageStatus(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"result":    message,
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// DeleteMessageEveryone delete a message for everyone
// @Summary Delete a message for everyone
// @Description Delete a message for everyone
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.MessageStruct true "Delete a message for everyone"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/delete [post]
func (m *messageHandler) DeleteMessageEveryone(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.MessageStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Chat == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "chat is required"})
		return
	}

	if data.MessageID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "messageId is required"})
		return
	}

	msgId, ts, err := m.messageService.DeleteMessageEveryone(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"messageId": msgId,
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// EditMessage edit a message
// @Summary Edit a message
// @Description Edit a message
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.EditMessageStruct true "Edit a message"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/edit [post]
func (m *messageHandler) EditMessage(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.EditMessageStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Chat == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "chat is required"})
		return
	}

	if data.Message == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "message is required"})
		return
	}

	if data.MessageID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "messageId is required"})
		return
	}

	msgId, ts, err := m.messageService.EditMessage(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responseData := gin.H{
		"messageId": msgId,
		"timestamp": ts,
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": responseData})
}

// PinMessage pin (or unpin) a message in the chat for everyone
// @Summary Pin a message
// @Description Pin/unpin a message in the conversation (shows for both sides)
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.PinMessageStruct true "Pin a message"
// @Success 200 {object} gin.H "success"
// @Failure 400 {object} gin.H "Error on validation"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /message/pin [post]
func (m *messageHandler) PinMessage(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *message_service.PinMessageStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Chat == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "chat is required"})
		return
	}

	if data.MessageID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "messageId is required"})
		return
	}

	msgId, ts, err := m.messageService.PinMessage(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": gin.H{"messageId": msgId, "timestamp": ts}})
}

func NewMessageHandler(
	messageService message_service.MessageService,
) MessageHandler {
	return &messageHandler{
		messageService: messageService,
	}
}

// VotePoll votes on an existing poll (or removes the vote with an empty options list)
// @Summary Vote on a poll
// @Tags Message
// @Accept json
// @Produce json
// @Param message body message_service.VotePollStruct true "Poll vote"
// @Success 200 {object} gin.H "success"
// @Router /message/pollvote [post]
func (m *messageHandler) VotePoll(ctx *gin.Context) {
	instance, ok := ctx.MustGet("instance").(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}
	var data *message_service.VotePollStruct
	if err := ctx.ShouldBindBodyWithJSON(&data); err != nil || data == nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if data.Number == "" || data.PollId == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "number and pollId are required"})
		return
	}
	id, err := m.messageService.VotePoll(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "success", "data": gin.H{"id": id}})
}
