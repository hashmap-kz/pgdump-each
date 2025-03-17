package notifier

type NotifyStatus string

var (
	NotifyStatusInfo  NotifyStatus = "info"
	NotifyStatusWarn  NotifyStatus = "warn"
	NotifyStatusError NotifyStatus = "error"
)

type AlertRequest struct {
	// info, warn, error
	Status  NotifyStatus `json:"status"`
	Message string       `json:"message"`
}

type Notifier interface {
	SendMessage(r *AlertRequest)
}
