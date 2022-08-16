package redis_broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KKKKjl/eTask/message"
	"github.com/KKKKjl/eTask/pool"
	"github.com/go-redis/redis"
)

const (
	DEFAULT_BUCKET_COUNT = 10
)

var (
	ErrNoMessage = errors.New("no message")
)

type RedisBroker struct {
	client  *redis.Client
	buckets []*DelayBucket
	pool    *pool.Pool
	name    string
}

func NewRedisBroker(client *redis.Client) *RedisBroker {
	buckets := make([]*DelayBucket, 0, DEFAULT_BUCKET_COUNT)
	for i := 0; i < DEFAULT_BUCKET_COUNT; i++ {
		bucket := NewDelayBucket(client, fmt.Sprintf("%s_%d", "delay_bucket", i))
		buckets = append(buckets, bucket)
	}

	return &RedisBroker{
		client:  client,
		buckets: buckets,
		pool:    pool.NewPool(client, "task_pool"),
		name:    "ready_queue",
	}
}

func (r *RedisBroker) Enqueue(msg message.Message) error {
	if err := r.pool.Add(msg); err != nil {
		return err
	}

	if msg.TTl > 0 {
		return r.addDelayTask(msg)
	} else {
		return r.client.LPush(r.name, msg.ID).Err()
	}
}

func (r *RedisBroker) Dequeue() (*message.Message, error) {
	res, err := r.client.BLPop(time.Second*2, r.name).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNoMessage
		}
		return nil, err
	}

	buf, err := r.pool.Get(res[1])
	if err != nil {
		return nil, err
	}

	msg := &message.Message{}
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (r *RedisBroker) addDelayTask(msg message.Message) error {
	bucket := r.getBucket(msg.ID)

	if err := bucket.Add(msg.ID, msg.TTl); err != nil {
		return err
	}

	return nil
}

func (r *RedisBroker) getBucket(key string) *DelayBucket {
	return r.buckets[fnv32(key)%uint32(len(r.buckets))]
}

func (*RedisBroker) Scheme() string {
	return "redis"
}

func (r *RedisBroker) Name() string {
	return r.name
}

func (r *RedisBroker) Close() error {
	for _, bucket := range r.buckets {
		bucket.Close()
	}
	return r.client.Close()
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
