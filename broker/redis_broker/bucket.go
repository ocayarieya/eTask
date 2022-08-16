package redis_broker

import (
	"errors"
	"time"

	"github.com/KKKKjl/eTask/timer"
	"github.com/go-redis/redis"
)

// DelayBucket store all delay task id
type DelayBucket struct {
	client *redis.Client
	timer  *timer.Timer
	name   string
}

func NewDelayBucket(client *redis.Client, name string) *DelayBucket {
	return &DelayBucket{
		client: client,
		name:   name,
		timer:  timer.NewTimer(client, time.Second*10, []string{name, "ready_queue"}),
	}
}

func (b *DelayBucket) Add(id string, ttl int64) error {
	if ttl <= 0 {
		return errors.New("ttl must be greater than 0")
	}

	executeTime := time.Now().Unix() + ttl

	return b.client.ZAdd(b.name, redis.Z{
		Score:  float64(executeTime),
		Member: id,
	}).Err()
}

func (b *DelayBucket) Close() {
	b.timer.Stop()
}
