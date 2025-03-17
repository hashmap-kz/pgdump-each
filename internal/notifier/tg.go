package notifier

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"gopgdump/config"
)

type tgNotifier struct{}

var _ Notifier = &tgNotifier{}

func NewTgNotifier() Notifier {
	return &tgNotifier{}
}

func (n *tgNotifier) SendMessage(r *AlertRequest) {
	cfg := config.Cfg()

	if !cfg.Notify.Enable {
		return
	}

	telegramConfig := cfg.Notify.Telegram
	if !telegramConfig.Enable {
		return
	}

	if r.Message == "" {
		return
	}

	tgBotToken := telegramConfig.Token
	tgBotChatID := telegramConfig.ChatID

	renderedMessage := getTemplate(r)

	endPoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", tgBotToken)
	formData := url.Values{
		"chat_id":    {tgBotChatID},
		"parse_mode": {"html"},
		"text":       {renderedMessage},
	}
	//nolint:gosec
	resp, err := http.PostForm(endPoint, formData)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func getTemplate(r *AlertRequest) string {
	t := time.Now()
	ts := t.Format("2006-01-02 15:04:05")

	switch r.Status {
	case NotifyStatusInfo:
		return fmt.Sprintf(InfoTemplate, r.Message, ts)
	case NotifyStatusWarn:
		return fmt.Sprintf(WarnTemplate, r.Message, ts)
	case NotifyStatusError:
		return fmt.Sprintf(ErrorTemplate, r.Message, ts)
	}

	return fmt.Sprintf(DefaultTemplate, r.Message, ts)
}

var (
	DefaultTemplate = `
<b>⚪ NOTE ⚪</b>

%s

Date: %s
`

	InfoTemplate = `
<b>🟢 INFO 🟢</b>

%s

Date: %s
`

	WarnTemplate = `
<b>🟡 WARNING 🟡</b>

%s

Date: %s
`

	ErrorTemplate = `
<b>🔴 ERROR 🔴</b>

%s

Date: %s
`
)
