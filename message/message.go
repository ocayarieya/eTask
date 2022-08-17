package message

import (
	"time"

	"github.com/KKKKjl/eTask/utils"
)

type IMessage interface {
	GetID() string
	GetData() interface{}
	GetStatus() string
	GetError() error
	GetTTl() int64
}

type Message struct {
	ID            string        `json:"id"`
	NameSpace     string        `json:"namespace"`
	Args          []interface{} `json:"args"`
	CreatedAt     time.Time     `json:"created_at"`
	TTl           int64         `json:"ttl"`
	Out           []interface{} `json:"result"`
	Callback      interface{}   `json:"callback"`
	Retry         int           `json:"retry"`
	ExecutionTime int64         `json:"execution_time"`
	Status        int           `json:"status"`
	Stackback     string        `json:"stackback"`
	NextJobId     string        `json:"next_job_id"`
}

func NewMessage(nameSpace string, args []interface{}, opts ...Option) *Message {
	msg := &Message{
		ID:        utils.GetUUID(),
		Args:      args,
		CreatedAt: time.Now(),
		NameSpace: nameSpace,
	}

	for _, opt := range opts {
		opt(msg)
	}

	return msg
}

func (m *Message) GetID() string {
	return m.ID
}

func (m *Message) GetStatus() string {
	return ""
}

func (m *Message) GetError() error {
	return nil
}

func (m *Message) GetTTl() int64 {
	return m.TTl
}
