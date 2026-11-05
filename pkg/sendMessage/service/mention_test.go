package send_service

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// docComLegenda builds the message exactly as SendMediaFile / sendMediaUrlWithRetry do for a
// document with caption: wrapped in DocumentWithCaptionMessage, top-level DocumentMessage nil.
func docComLegenda(ci *waE2E.ContextInfo) *waE2E.Message {
	return &waE2E.Message{
		DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				DocumentMessage: &waE2E.DocumentMessage{
					FileName:    proto.String("Orçamento (1).pdf"),
					Caption:     proto.String("segue o orçamento"),
					ContextInfo: ci,
				},
			},
		},
	}
}

func TestAplicarMencoes_DocComLegendaEmGrupoSemMencao_NaoPanicaNemMexe(t *testing.T) {
	original := &waE2E.ContextInfo{}
	msg := docComLegenda(original)

	// Regression: this used to panic (nil DocumentMessage) and fail the whole send with 500.
	aplicarMencoes(msg, "DocumentMessage", &SendDataStruct{Number: "123@g.us"})

	inner := msg.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
	if inner == nil {
		t.Fatal("inner DocumentMessage must be preserved")
	}
	if inner.ContextInfo != original {
		t.Fatal("ContextInfo must stay the very same object when no mention was requested")
	}
	if inner.ContextInfo.NonJIDMentions != nil || len(inner.ContextInfo.MentionedJID) != 0 {
		t.Fatalf("no mention must be written, got %v", inner.ContextInfo)
	}
	if msg.DocumentMessage != nil {
		t.Fatal("top-level DocumentMessage must stay nil (wrapper is the one sent)")
	}
}

func TestAplicarMencoes_SemMencao_NaoCriaContextInfo(t *testing.T) {
	msg := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("oi")}}

	aplicarMencoes(msg, "ExtendedTextMessage", &SendDataStruct{Number: "123@g.us"})

	if msg.ExtendedTextMessage.ContextInfo != nil {
		t.Fatal("without a mention the ContextInfo must not be created")
	}
}

func TestAplicarMencoes_DocComLegendaMaisMentionAll_MarcaNonJIDMentionsNoDocumentoInterno(t *testing.T) {
	msg := docComLegenda(nil)

	aplicarMencoes(msg, "DocumentMessage", &SendDataStruct{Number: "123@g.us", MentionAll: true})

	ci := msg.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage().GetContextInfo()
	if ci == nil {
		t.Fatal("ContextInfo must be created on the inner DocumentMessage")
	}
	if ci.GetNonJIDMentions() != 1 {
		t.Fatalf("want NonJIDMentions=1, got %v", ci.NonJIDMentions)
	}
	if len(ci.MentionedJID) != 0 {
		t.Fatalf("@all must not carry a participant list, got %v", ci.MentionedJID)
	}
}

func TestAplicarMencoes_DocSemLegendaMaisMencaoNomeada(t *testing.T) {
	msg := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("a.pdf")}}
	jids := []string{"5521999999999@s.whatsapp.net"}

	aplicarMencoes(msg, "DocumentMessage", &SendDataStruct{Number: "123@g.us", MentionedJID: jids})

	ci := msg.GetDocumentMessage().GetContextInfo()
	if ci == nil {
		t.Fatal("ContextInfo must be created on the DocumentMessage")
	}
	if len(ci.MentionedJID) != 1 || ci.MentionedJID[0] != jids[0] {
		t.Fatalf("want MentionedJID=%v, got %v", jids, ci.MentionedJID)
	}
	if ci.NonJIDMentions != nil {
		t.Fatalf("named mention alone must not set NonJIDMentions, got %v", ci.NonJIDMentions)
	}
}

func TestAplicarMencoes_MentionAllMaisNomeada_Somam(t *testing.T) {
	msg := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("@all @fulano")}}
	jids := []string{"5521999999999@s.whatsapp.net"}

	aplicarMencoes(msg, "ImageMessage", &SendDataStruct{Number: "123@g.us", MentionAll: true, MentionedJID: jids})

	ci := msg.GetImageMessage().GetContextInfo()
	if ci.GetNonJIDMentions() != 1 || len(ci.MentionedJID) != 1 {
		t.Fatalf("@all and named mention must both be kept, got %v", ci)
	}
}

func TestContextInfoParaMencao_PonteiroNilRetornaNil(t *testing.T) {
	casos := []struct {
		nome string
		msg  *waE2E.Message
		tipo string
	}{
		{"mensagem nil", nil, "ExtendedTextMessage"},
		{"ExtendedText nil", &waE2E.Message{}, "ExtendedTextMessage"},
		{"Image nil", &waE2E.Message{}, "ImageMessage"},
		{"Video nil", &waE2E.Message{}, "VideoMessage"},
		{"Ptv nil", &waE2E.Message{}, "PtvMessage"},
		{"Audio nil", &waE2E.Message{}, "AudioMessage"},
		{"Document e wrapper nil", &waE2E.Message{}, "DocumentMessage"},
		{"wrapper sem Message", &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{}}, "DocumentMessage"},
		{"wrapper sem DocumentMessage interno", &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{}}}, "DocumentMessage"},
		{"Poll nil", &waE2E.Message{}, "PollCreationMessage"},
		{"Sticker nil", &waE2E.Message{}, "StickerMessage"},
		{"Location nil", &waE2E.Message{}, "LocationMessage"},
		{"Contact nil", &waE2E.Message{}, "ContactMessage"},
		{"tipo sem menção", &waE2E.Message{ListMessage: &waE2E.ListMessage{}}, "ListMessage"},
		{"tipo desconhecido", &waE2E.Message{}, "Whatever"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			if got := contextInfoParaMencao(c.msg, c.tipo); got != nil {
				t.Fatalf("want nil, got %v", got)
			}
		})
	}
}

func TestContextInfoParaMencao_CriaOuReaproveitaContextInfo(t *testing.T) {
	// creates when missing
	msg := &waE2E.Message{StickerMessage: &waE2E.StickerMessage{}}
	ci := contextInfoParaMencao(msg, "StickerMessage")
	if ci == nil || msg.StickerMessage.ContextInfo != ci {
		t.Fatal("must create and attach a ContextInfo to the StickerMessage")
	}

	// reuses the existing one (quoted reply already set it)
	existente := &waE2E.ContextInfo{StanzaID: proto.String("ABC")}
	msg = &waE2E.Message{VideoMessage: &waE2E.VideoMessage{ContextInfo: existente}}
	if got := contextInfoParaMencao(msg, "VideoMessage"); got != existente {
		t.Fatal("must return the existing ContextInfo, not replace it")
	}
}

func TestTemMencao(t *testing.T) {
	if temMencao(nil) || temMencao(&SendDataStruct{}) {
		t.Fatal("no mention expected")
	}
	if !temMencao(&SendDataStruct{MentionAll: true}) || !temMencao(&SendDataStruct{MentionedJID: []string{"x"}}) {
		t.Fatal("mention expected")
	}
}

func TestDefinirContextInfoDoDocumento(t *testing.T) {
	ci := &waE2E.ContextInfo{StanzaID: proto.String("Q1")}

	// document without caption: top-level DocumentMessage
	semLegenda := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("a.pdf")}}
	definirContextInfoDoDocumento(semLegenda, ci)
	if semLegenda.DocumentMessage.ContextInfo != ci {
		t.Fatal("must set the ContextInfo on the top-level DocumentMessage")
	}

	// document with caption: inner DocumentMessage of the wrapper
	comLegenda := docComLegenda(nil)
	definirContextInfoDoDocumento(comLegenda, ci)
	if comLegenda.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage().ContextInfo != ci {
		t.Fatal("must set the ContextInfo on the inner DocumentMessage")
	}
	if comLegenda.DocumentMessage != nil {
		t.Fatal("top-level DocumentMessage must stay nil")
	}

	// malformed / empty: must not panic
	for nome, m := range map[string]*waE2E.Message{
		"mensagem nil":                        nil,
		"sem documento":                       {},
		"wrapper sem Message":                 {DocumentWithCaptionMessage: &waE2E.FutureProofMessage{}},
		"wrapper sem DocumentMessage interno": {DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{}}},
	} {
		t.Run(nome, func(t *testing.T) {
			definirContextInfoDoDocumento(m, ci)
		})
	}
}

// mensagemPorTipo builds each message type the way the send builders do (document with caption
// wrapped, view-once as a flag on the media itself).
func mensagemPorTipo(tipo string) (*waE2E.Message, string) {
	switch tipo {
	case "texto":
		return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("@all oi")}}, "ExtendedTextMessage"
	case "imagem":
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("x")}}, "ImageMessage"
	case "imagem view-once":
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{ViewOnce: proto.Bool(true)}}, "ImageMessage"
	case "video":
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{}}, "VideoMessage"
	case "video view-once":
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{ViewOnce: proto.Bool(true)}}, "VideoMessage"
	case "ptv":
		return &waE2E.Message{PtvMessage: &waE2E.VideoMessage{}}, "PtvMessage"
	case "audio":
		return &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true)}}, "AudioMessage"
	case "doc sem legenda":
		return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("a.pdf")}}, "DocumentMessage"
	case "doc com legenda":
		return docComLegenda(nil), "DocumentMessage"
	case "enquete":
		return &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{Name: proto.String("?")}}, "PollCreationMessage"
	case "figurinha":
		return &waE2E.Message{StickerMessage: &waE2E.StickerMessage{}}, "StickerMessage"
	case "localizacao":
		return &waE2E.Message{LocationMessage: &waE2E.LocationMessage{}}, "LocationMessage"
	case "contato":
		return &waE2E.Message{ContactMessage: &waE2E.ContactMessage{}}, "ContactMessage"
	}
	panic("tipo desconhecido: " + tipo)
}

// contextInfoDoEnvio mirrors the quoted / plain branch of SendMessage (every mention-capable type
// gets a ContextInfo before the mention step), plus the forwarding step.
func contextInfoDoEnvio(msg *waE2E.Message, messageType string, citada, encaminhada bool) {
	novo := func() *waE2E.ContextInfo {
		if citada {
			return &waE2E.ContextInfo{StanzaID: proto.String("Q1"), Participant: proto.String("p@s.whatsapp.net")}
		}
		return &waE2E.ContextInfo{}
	}
	if messageType == "DocumentMessage" {
		definirContextInfoDoDocumento(msg, novo())
	} else if ci := contextInfoParaMencao(msg, messageType); ci != nil {
		*ci = *novo()
	}
	if encaminhada {
		if ci := contextInfoParaMencao(msg, messageType); ci != nil {
			ci.ForwardingScore = proto.Uint32(1)
			ci.IsForwarded = proto.Bool(true)
		}
	}
}

func TestAplicarMencoes_MatrizCompleta_NuncaPanicaEMantemComportamento(t *testing.T) {
	tipos := []string{"texto", "imagem", "imagem view-once", "video", "video view-once", "ptv", "audio",
		"doc sem legenda", "doc com legenda", "enquete", "figurinha", "localizacao", "contato"}
	jids := []string{"5521999999999@s.whatsapp.net"}
	mencoes := map[string]*SendDataStruct{
		"sem menção":     {},
		"@todos":         {MentionAll: true},
		"nomeada":        {MentionedJID: jids},
		"@todos+nomeada": {MentionAll: true, MentionedJID: jids},
	}
	for _, tipo := range tipos {
		for nomeMencao, base := range mencoes {
			for _, citada := range []bool{false, true} {
				for _, encaminhada := range []bool{false, true} {
					for _, destino := range []string{"120363000000000000@g.us", "5521988887777"} {
						nome := tipo + "/" + nomeMencao + "/" + destino
						if citada {
							nome += "/citada"
						}
						if encaminhada {
							nome += "/encaminhada"
						}
						t.Run(nome, func(t *testing.T) {
							msg, messageType := mensagemPorTipo(tipo)
							data := *base
							data.Number = destino
							contextInfoDoEnvio(msg, messageType, citada, encaminhada)
							antes := proto.Clone(msg)

							// same gate as SendMessage
							if destino == "120363000000000000@g.us" {
								aplicarMencoes(msg, messageType, &data)
							}

							ci := contextInfoParaMencao(msg, messageType)
							if ci == nil {
								t.Fatal("every mention-capable type must carry a ContextInfo here")
							}
							ehGrupo := destino == "120363000000000000@g.us"
							if !ehGrupo || !temMencao(&data) {
								if !proto.Equal(antes, msg) {
									t.Fatalf("without a group mention the message must not change:\nbefore %v\nafter  %v", antes, msg)
								}
								return
							}
							// with a mention: identical to 473d747
							if data.MentionAll != (ci.GetNonJIDMentions() == 1) {
								t.Fatalf("NonJIDMentions mismatch: %v", ci.NonJIDMentions)
							}
							if len(data.MentionedJID) > 0 {
								if len(ci.MentionedJID) != 1 || ci.MentionedJID[0] != jids[0] {
									t.Fatalf("MentionedJID mismatch: %v", ci.MentionedJID)
								}
							} else if len(ci.MentionedJID) != 0 {
								t.Fatalf("@all must not carry a list: %v", ci.MentionedJID)
							}
							if citada && ci.GetStanzaID() != "Q1" {
								t.Fatal("quoted reply must be preserved alongside the mention")
							}
							if encaminhada && !ci.GetIsForwarded() {
								t.Fatal("forwarded flag must be preserved alongside the mention")
							}
						})
					}
				}
			}
		}
	}
}
