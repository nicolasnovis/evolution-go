package chat_service

import (
	"context"
	"errors"
	"strings"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type ChatService interface {
	ChatPin(data *BodyStruct, instance *instance_model.Instance) (string, error)
	ChatUnpin(data *BodyStruct, instance *instance_model.Instance) (string, error)
	ChatArchive(data *BodyStruct, instance *instance_model.Instance) (string, error)
	ChatUnarchive(data *BodyStruct, instance *instance_model.Instance) (string, error)
	ChatMute(data *BodyStruct, instance *instance_model.Instance) (string, error)
	ChatUnmute(data *BodyStruct, instance *instance_model.Instance) (string, error)
	HistorySyncRequest(data *HistorySyncRequestStruct, instance *instance_model.Instance) (*whatsmeow.SendResponse, error)
}

type chatService struct {
	clientPointer    map[string]*whatsmeow.Client
	whatsmeowService whatsmeow_service.WhatsmeowService
	loggerWrapper    *logger_wrapper.LoggerManager
}

type BodyStruct struct {
	Chat string `json:"chat"`
	// Archive precisa da ÂNCORA da última mensagem do chat (timestamp + MessageKey): sem ela o
	// WhatsApp IGNORA a mutação de arquivar (era o bug do "not working"). O Novi já conhece a última
	// msg no banco e manda esses campos. Vazio → cai no comportamento antigo (sem âncora). Pin/Mute
	// não usam isso. LastMessageID é o ID do WhatsApp da msg (stanza id), não o id interno do CRM.
	LastMessageID        string `json:"lastMessageId,omitempty"`
	LastMessageFromMe    bool   `json:"lastMessageFromMe,omitempty"`
	LastMessageTimestamp int64  `json:"lastMessageTimestamp,omitempty"` // unix seconds
}

// sendAppStateResilient manda a mutação de app-state (pin/arquivar) e, se o WhatsApp reclamar de
// DESSINCRONIA (LTHash / patch mismatch no regular_low — a coleção de pin/arquivar/mute), força um
// FULL resync dessa coleção e tenta UMA vez de novo. É o caminho de recuperação do whatsmeow pra
// quando o estado local diverge do servidor (comum em instância re-pareada). Sem isso, pin/arquivar
// fica preso pra sempre em HTTP 500 nessa instância.
func (c *chatService) sendAppStateResilient(client *whatsmeow.Client, patch appstate.PatchInfo, instanceId string) error {
	err := client.SendAppState(context.Background(), patch)
	if err == nil {
		return nil
	}
	msg := err.Error()
	if !strings.Contains(msg, "LTHash") && !strings.Contains(msg, "app state") && !strings.Contains(msg, "app-state") {
		return err
	}
	c.loggerWrapper.GetLogger(instanceId).LogWarn("[%s] app-state dessincronizado (%s) — full resync do regular_low + retry", instanceId, msg)
	if ferr := client.FetchAppState(context.Background(), appstate.WAPatchRegularLow, true, false); ferr != nil {
		c.loggerWrapper.GetLogger(instanceId).LogError("[%s] full resync do app-state falhou: %v", instanceId, ferr)
		return err // devolve o erro ORIGINAL do send
	}
	return client.SendAppState(context.Background(), patch)
}

// archiveAnchor monta o timestamp + MessageKey da última msg do chat pra BuildArchive. Sem
// LastMessageID devolve o par vazio (time.Time{}, nil) — o mesmo que o código fazia antes.
func archiveAnchor(data *BodyStruct, chat types.JID) (time.Time, *waCommon.MessageKey) {
	if data.LastMessageID == "" {
		return time.Time{}, nil
	}
	key := &waCommon.MessageKey{
		RemoteJID: proto.String(chat.String()),
		FromMe:    proto.Bool(data.LastMessageFromMe),
		ID:        proto.String(data.LastMessageID),
	}
	var ts time.Time
	if data.LastMessageTimestamp > 0 {
		ts = time.Unix(data.LastMessageTimestamp, 0)
	}
	return ts, key
}

type HistorySyncRequestStruct struct {
	MessageInfo *types.MessageInfo `json:"messageInfo"`
	Count       int                `json:"count"`
}

func (c *chatService) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	client := c.clientPointer[instanceId]
	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking client connection status - Client exists: %v", instanceId, client != nil)

	if client == nil {
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] No client found, attempting to start new instance", instanceId)
		err := c.whatsmeowService.StartInstance(instanceId)
		if err != nil {
			c.loggerWrapper.GetLogger(instanceId).LogError("[%s] Failed to start instance: %v", instanceId, err)
			return nil, errors.New("no active session found")
		}

		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Instance started, waiting 2 seconds...", instanceId)
		time.Sleep(2 * time.Second)

		client = c.clientPointer[instanceId]
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking new client - Exists: %v, Connected: %v",
			instanceId,
			client != nil,
			client != nil && client.IsConnected())

		if client == nil || !client.IsConnected() {
			c.loggerWrapper.GetLogger(instanceId).LogError("[%s] New client validation failed - Exists: %v, Connected: %v",
				instanceId,
				client != nil,
				client != nil && client.IsConnected())
			return nil, errors.New("no active session found")
		}
	} else if !client.IsConnected() {
		c.loggerWrapper.GetLogger(instanceId).LogError("[%s] Existing client is disconnected - Connected status: %v",
			instanceId,
			client.IsConnected())
		return nil, errors.New("client disconnected")
	}

	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Client successfully validated - Connected: %v", instanceId, client.IsConnected())
	return client, nil
}

func (c *chatService) ChatPin(data *BodyStruct, instance *instance_model.Instance) (string, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	err = c.sendAppStateResilient(client, appstate.BuildPin(recipient, true), instance.Id)
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error pin chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

func (c *chatService) ChatUnpin(data *BodyStruct, instance *instance_model.Instance) (string, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	err = c.sendAppStateResilient(client, appstate.BuildPin(recipient, false), instance.Id)
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error unpin chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

func (c *chatService) ChatArchive(data *BodyStruct, instance *instance_model.Instance) (string, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	lastTs, lastKey := archiveAnchor(data, recipient)
	err = c.sendAppStateResilient(client, appstate.BuildArchive(recipient, true, lastTs, lastKey), instance.Id)
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error archive chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

func (c *chatService) ChatUnarchive(data *BodyStruct, instance *instance_model.Instance) (string, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	lastTs, lastKey := archiveAnchor(data, recipient)
	err = c.sendAppStateResilient(client, appstate.BuildArchive(recipient, false, lastTs, lastKey), instance.Id)
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error unarchive chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

func (c *chatService) ChatMute(data *BodyStruct, instance *instance_model.Instance) (string, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	err = client.SendAppState(context.Background(), appstate.BuildMute(recipient, true, 1*time.Hour))
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error mute chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

func (c *chatService) ChatUnmute(data *BodyStruct, instance *instance_model.Instance) (string, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	err = client.SendAppState(context.Background(), appstate.BuildMute(recipient, false, 0*time.Hour))
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error unmute chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

func (c *chatService) HistorySyncRequest(data *HistorySyncRequestStruct, instance *instance_model.Instance) (*whatsmeow.SendResponse, error) {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, err
	}

	messageInfo := types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     data.MessageInfo.Chat,
			IsFromMe: data.MessageInfo.IsFromMe,
			IsGroup:  data.MessageInfo.IsGroup,
		},
		ID:        data.MessageInfo.ID,
		Timestamp: data.MessageInfo.Timestamp,
	}

	histRequest := client.BuildHistorySyncRequest(&messageInfo, data.Count)

	// On-demand history-sync is a PEER request: it must be sent to our OWN device (self), never to
	// the chat/contact. Sending it to messageInfo.Chat only "worked" when a live signal session with
	// that contact happened to exist (recent chats), returned no history, and hard-failed on dormant
	// chats with "no signal session established" — which is exactly where we need the backfill. The
	// contact never fulfils our history request; only our primary device does. Route to self.
	if client.Store == nil || client.Store.ID == nil {
		return nil, errors.New("instance not logged in (no self JID for peer history-sync)")
	}
	self := client.Store.ID.ToNonAD()

	res, err := client.SendMessage(context.Background(), self, histRequest, whatsmeow.SendRequestExtra{Peer: true})
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error history sync request: %v", instance.Id, err)
		return nil, err
	}

	return &res, nil
}

func NewChatService(
	clientPointer map[string]*whatsmeow.Client,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	loggerWrapper *logger_wrapper.LoggerManager,
) ChatService {
	return &chatService{
		clientPointer:    clientPointer,
		whatsmeowService: whatsmeowService,
		loggerWrapper:    loggerWrapper,
	}
}
