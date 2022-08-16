package pool

import (
	"encoding/json"

	"github.com/KKKKjl/eTask/message"
	"github.com/go-redis/redis"
)

// Pool stores all task meta data
type Pool struct {
	client *redis.Client
	name   string
}

func NewPool(client *redis.Client, name string) *Pool {
	return &Pool{
		client: client,
		name:   name,
	}
}

func (p *Pool) Add(msg message.Message) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return p.client.HSet(p.name, msg.ID, buf).Err()
}

func (p *Pool) Get(id string) ([]byte, error) {
	buf, err := p.client.HGet(p.name, id).Bytes()
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (p *Pool) Delete(id string) error {
	return p.client.HDel(p.name, id).Err()
}
