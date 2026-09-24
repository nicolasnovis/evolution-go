package chat_service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/appstatehealth"
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
	DeleteMessageForMe(data *DeleteForMeStruct, instance *instance_model.Instance) error
	RecoverAppState(instance *instance_model.Instance) error
	ResetAppState(instance *instance_model.Instance) error
	HistorySyncRequest(data *HistorySyncRequestStruct, instance *instance_model.Instance) (*whatsmeow.SendResponse, error)
}

type chatService struct {
	clientPointer    map[string]*whatsmeow.Client
	whatsmeowService whatsmeow_service.WhatsmeowService
	loggerWrapper    *logger_wrapper.LoggerManager
	// serializa mutações de app-state POR instância. O SendAppState do whatsmeow não tem trava de
	// topo: dois envios sobrepostos na mesma coleção (ex.: fixar + arquivar quase juntos) leem a mesma
	// versão, ambos assinam versão+1 e o 2º toma 409 conflict — e UMA sobreposição já envenena o
	// regular_low (trava LTHash pra sempre). Um mutex por instância mata essa corrida.
	appStateMu sync.Map // instanceId(string) → *sync.Mutex
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

// DeleteForMeStruct: "apagar para mim" vindo do CRM. A mensagem some deste número em TODOS os aparelhos
// (celular incluso), sem apagar pra outra pessoa. MessageID é o ID do WhatsApp (stanza id); Sender só
// importa em grupo, em mensagem de outra pessoa (o participante que mandou); MessageTimestamp é o horário
// ORIGINAL da mensagem em unix seconds.
type DeleteForMeStruct struct {
	Chat             string `json:"chat"`
	MessageID        string `json:"messageId"`
	FromMe           bool   `json:"fromMe"`
	Sender           string `json:"sender,omitempty"`
	MessageTimestamp int64  `json:"messageTimestamp"`
	DeleteMedia      bool   `json:"deleteMedia,omitempty"`
}

// sendAppStateResilient serializa a mutação de app-state (pin/arquivar) POR instância e deixa o
// SendAppState do whatsmeow trabalhar sozinho. O SendAppState já: encoda com a versão do store, no
// 409 conflict aplica os patches devolvidos e re-tenta 1x, e faz um resync incremental síncrono que
// PERSISTE a versão nova. NÃO pré-full-syncamos mais (era redundante — o resync interno já faz — e no
// estado dessincronizado só queimava round-trip). O que faltava era a TRAVA: sem ela, dois sends
// sobrepostos assinavam a mesma versão e o 2º dava 409, envenenando a coleção. Se a coleção JÁ está
// travada (LTHash), o erro sobe pro chamador; recuperar é via RecoverAppState (recovery request).
func (c *chatService) sendAppStateResilient(client *whatsmeow.Client, patch appstate.PatchInfo, instanceId string) error {
	coll := string(patch.Type)
	// Guard 1: never publish onto a collection flagged poisoned (its server-side snapshot can't be
	// verified). Another patch would only pile more bad data onto it — the toggle stays local in the
	// CRM until a reset-appstate + re-scan. The auto-heal sets/clears this flag (appstatehealth).
	if appstatehealth.IsPoisoned(instanceId, coll) {
		return fmt.Errorf("app-state %s da instância %s está envenenado (aguardando reset+QR) — envio pulado", coll, instanceId)
	}
	muAny, _ := c.appStateMu.LoadOrStore(instanceId, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	ctx := context.Background()
	// Guard 2 (belt-and-suspenders): catch up to the server's latest version before publishing, so we
	// never build a patch on a stale base. Best-effort — on error we still try the send, whose own
	// 409-conflict path (now atomic + locked) re-applies server patches and retries safely.
	if err := client.FetchAppState(ctx, patch.Type, false, false); err != nil {
		c.loggerWrapper.GetLogger(instanceId).LogWarn("[%s] sync-before-send de %s falhou (seguindo mesmo assim): %v", instanceId, coll, err)
	}
	return client.SendAppState(ctx, patch)
}

// RecoverAppState pede ao aparelho PRIMÁRIO uma cópia NÃO-CRIPTOGRAFADA da coleção regular_low
// (pin/arquivar/mute). Isso contorna a verificação de MAC — é a recuperação recomendada pelo whatsmeow
// (issue #858) pra quando a chave de app-state dessincroniza e a coleção trava com "mismatching
// LTHash" (nem o full-sync resolve, pois o próprio snapshot não verifica). A resposta chega como
// PeerDataOperationResponse e o whatsmeow a processa sozinho (handleAppStateRecovery → reconstrói a
// coleção → emite events.AppStateSyncComplete{Recovery:true}). Se o celular não responder, re-parear
// é o único caminho. Serializa junto com os sends (mesma trava por instância).
func (c *chatService) RecoverAppState(instance *instance_model.Instance) error {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}
	muAny, _ := c.appStateMu.LoadOrStore(instance.Id, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	msg := whatsmeow.BuildAppStateRecoveryRequest(appstate.WAPatchRegularLow)
	if _, err := client.SendPeerMessage(context.Background(), msg); err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] app-state recovery request FALHOU: %v", instance.Id, err)
		return err
	}
	c.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] app-state recovery request enviado (regular_low) — aguardando o primário responder", instance.Id)
	return nil
}

// ResetAppState pede ao aparelho PRIMÁRIO pra RESETAR a coleção regular_low (apaga o patch envenenado
// no SERVIDOR — o que nem re-parear nem o recovery request limpam). ⚠️ DERRUBA todos os dispositivos
// linkados DESTE número (os companheiros: Novi/evogo + WhatsApp Web/Desktop) → o número precisa
// re-escanear o QR depois. O primário (celular) NÃO cai. É o ÚLTIMO recurso quando a coleção trava.
// whatsmeow.BuildFatalAppStateExceptionNotification via SendPeerMessage. Serializa junto (mutex).
func (c *chatService) ResetAppState(instance *instance_model.Instance) error {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}
	muAny, _ := c.appStateMu.LoadOrStore(instance.Id, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	msg := whatsmeow.BuildFatalAppStateExceptionNotification(appstate.WAPatchRegularLow)
	if _, err := client.SendPeerMessage(context.Background(), msg); err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] fatal app-state exception FALHOU: %v", instance.Id, err)
		return err
	}
	c.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] fatal app-state exception enviado (RESET regular_low) — dispositivos linkados vão deslogar; re-escanear o QR depois", instance.Id)
	return nil
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

// resolveAppStateTarget devolve o JID que o app-state de CHAT (regular_low: pin/arquivar/mute) do
// aparelho PRIMÁRIO usa como índice. O WhatsApp migrou o endereçamento de chat pra LID: o celular
// indexa pin/arquivar pelo <lid>@lid do contato, NÃO pelo <número>@s.whatsapp.net. Escrever a mutação
// pelo número cai num índice que o telefone não consulta — o efeito observado é o pin/arquivo "grudar":
// o ADD (pinned:true) até aparece porque o servidor mapeia número→contato, mas o REMOVE (pinned:false)
// nunca limpa o índice LID que o telefone de fato exibe, então continua fixado. PROVADO no número spike
// comparando o index_mac: nosso pin por número = 27971a60…, o do telefone e o nosso por LID = ebc21ea1…
// (idênticos). Resolver pro LID faz o index_mac bater com o do telefone e a mutação reflete nos 2 sentidos.
// Contatos sem LID conhecido, ou JIDs que não são de usuário (grupo/broadcast/já-@lid), caem no original.
func (c *chatService) resolveAppStateTarget(client *whatsmeow.Client, recipient types.JID, instanceId string) types.JID {
	if recipient.Server != types.DefaultUserServer { // só @s.whatsapp.net tem par LID; @lid/@g.us/@broadcast passam direto
		return recipient
	}
	if client.Store == nil || client.Store.LIDs == nil {
		return recipient
	}
	lid, err := client.Store.LIDs.GetLIDForPN(context.Background(), recipient)
	if err != nil || lid.IsEmpty() {
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] app-state target: sem LID pra %s, usando o número", instanceId, recipient.String())
		return recipient
	}
	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] app-state target: %s → %s (LID)", instanceId, recipient.String(), lid.String())
	return lid
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

	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	err = c.sendAppStateResilient(client, appstate.BuildPin(target, true), instance.Id)
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

	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	err = c.sendAppStateResilient(client, appstate.BuildPin(target, false), instance.Id)
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

	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	lastTs, lastKey := archiveAnchor(data, target)
	err = c.sendAppStateResilient(client, appstate.BuildArchive(target, true, lastTs, lastKey), instance.Id)
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

	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	lastTs, lastKey := archiveAnchor(data, target)
	err = c.sendAppStateResilient(client, appstate.BuildArchive(target, false, lastTs, lastKey), instance.Id)
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error unarchive chat: %v", instance.Id, err)
		return "", err
	}

	return ts.String(), nil
}

// DeleteMessageForMe: CRM→WA "apagar para mim". Mutation deleteMessageForMe no regular_high (mesma
// coleção do star). O chat passa pelo resolveAppStateTarget pelo mesmo motivo do pin: o celular indexa
// conversa 1:1 pelo LID, e a mutação por número não bate com a mensagem que ele exibe.
func (c *chatService) DeleteMessageForMe(data *DeleteForMeStruct, instance *instance_model.Instance) error {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}
	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		return errors.New("invalid chat")
	}
	var sender types.JID
	if data.Sender != "" {
		if sj, ok := utils.ParseJID(data.Sender); ok {
			sender = sj
		}
	}
	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	if sender.IsEmpty() && !data.FromMe {
		sender = target // 1:1 recebida: o remetente é o próprio chat → vira "0" no índice
	}
	ts := time.Unix(data.MessageTimestamp, 0)
	err = c.sendAppStateResilient(client, appstate.BuildDeleteForMe(target, sender, data.MessageID, data.FromMe, data.DeleteMedia, ts), instance.Id)
	if err != nil {
		c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error delete for me: %v", instance.Id, err)
		return err
	}
	return nil
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

	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	err = client.SendAppState(context.Background(), appstate.BuildMute(target, true, 1*time.Hour))
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

	target := c.resolveAppStateTarget(client, recipient, instance.Id)
	err = client.SendAppState(context.Background(), appstate.BuildMute(target, false, 0*time.Hour))
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
