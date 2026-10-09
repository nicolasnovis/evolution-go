package message_service

import (
	"context"
	"errors"
	"fmt"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// VotePollStruct votes on an existing poll. Options are the option NAMES (WhatsApp hashes them);
// an empty list removes the vote. Sender is the poll's author (required in groups; for a poll we
// sent ourselves, FromMe=true is enough).
type VotePollStruct struct {
	Number  string   `json:"number"`
	PollId  string   `json:"pollId"`
	Sender  string   `json:"sender,omitempty"`
	FromMe  bool     `json:"fromMe"`
	Options []string `json:"options"`
}

// candidatosDeJID returns the JID plus its LID<->PN counterpart: whatsmeow keys a poll's secret by
// the chat and author exactly as they were addressed when it arrived, which may be either form.
func candidatosDeJID(ctx context.Context, client *whatsmeow.Client, jid types.JID) []types.JID {
	out := []types.JID{jid}
	switch jid.Server {
	case types.HiddenUserServer:
		if pn, err := client.Store.LIDs.GetPNForLID(ctx, jid); err == nil && !pn.IsEmpty() {
			out = append(out, pn)
		}
	case types.DefaultUserServer:
		if lid, err := client.Store.LIDs.GetLIDForPN(ctx, jid); err == nil && !lid.IsEmpty() {
			out = append(out, lid)
		}
	}
	return out
}

func (m *messageService) VotePoll(data *VotePollStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}
	ctx := context.Background()

	chat, ok := utils.ParseJID(data.Number)
	if !ok || data.PollId == "" {
		return "", errors.New("number and pollId are required")
	}
	chat = utils.CanonicalJID(chat).ToNonAD()

	var autores []types.JID
	if data.FromMe {
		if client.Store.ID != nil {
			autores = append(autores, client.Store.ID.ToNonAD())
		}
		if lid := client.Store.GetLID(); !lid.IsEmpty() {
			autores = append(autores, lid.ToNonAD())
		}
	} else if sender, ok := utils.ParseJID(data.Sender); ok {
		autores = candidatosDeJID(ctx, client, utils.CanonicalJID(sender).ToNonAD())
	} else if chat.Server != types.GroupServer {
		autores = candidatosDeJID(ctx, client, chat) // 1:1: the author is the contact
	}
	if len(autores) == 0 {
		return "", errors.New("poll sender is required")
	}

	// find the (chat, author) pair under which the poll's secret was stored
	var pollInfo *types.MessageInfo
	for _, c := range candidatosDeJID(ctx, client, chat) {
		for _, a := range autores {
			secret, realSender, gErr := client.Store.MsgSecrets.GetMessageSecret(ctx, c, a, types.MessageID(data.PollId))
			if gErr != nil || secret == nil {
				continue
			}
			if realSender.IsEmpty() {
				realSender = a
			}
			pollInfo = &types.MessageInfo{
				MessageSource: types.MessageSource{Chat: c, Sender: realSender, IsFromMe: data.FromMe, IsGroup: c.Server == types.GroupServer},
				ID:            types.MessageID(data.PollId),
			}
			break
		}
		if pollInfo != nil {
			break
		}
	}
	if pollInfo == nil {
		return "", whatsmeow.ErrOriginalMessageSecretNotFound
	}

	msg, err := client.BuildPollVote(ctx, pollInfo, data.Options)
	if err != nil {
		return "", fmt.Errorf("build poll vote: %w", err)
	}
	resp, err := client.SendMessage(ctx, pollInfo.Chat, msg)
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] VotePoll send failed: %v", instance.Id, err)
		return "", err
	}
	m.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Poll vote sent on %s (%d option(s))", instance.Id, data.PollId, len(data.Options))
	return string(resp.ID), nil
}
