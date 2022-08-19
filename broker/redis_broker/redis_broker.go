package redis_broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/internal/pool"
	"github.com/KKKKjl/eTask/internal/task"
	"github.com/go-redis/redis"
	"golang.org/x/sync/errgroup"
)

const (
	DEFAULT_BUCKET_COUNT = 10
)

var (
	ErrNoMessage         = errors.New("no message")
	ErrGroupNotCompleted = errors.New("group task not completed")
)

type RedisBroker struct {
	client  *redis.Client
	buckets []*DelayBucket
	pool    *pool.Pool
	name    string
}

type GroupMetaData struct {
	UUID     string   `json:"uuid"`
	JobUUIDS []string `json:"job_uuids"`
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

func (r *RedisBroker) Enqueue(msg task.Message) error {
	if err := r.HSet(msg); err != nil {
		return err
	}

	if msg.TTl > 0 {
		return r.addDelayTask(msg)
	} else {
		return r.client.LPush(r.name, msg.ID).Err()
	}
}

func (r *RedisBroker) HSet(msg task.Message) error {
	return r.pool.Add(msg.ID, msg)
}

func (r *RedisBroker) HGet(key string) (*task.Message, error) {
	buf, err := r.pool.Get(key)
	if err != nil {
		return nil, err
	}

	msg := &task.Message{}
	if err := json.Unmarshal(buf, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (r *RedisBroker) Dequeue() (*task.Message, error) {
	res, err := r.client.BRPop(time.Second*2, r.name).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNoMessage
		}
		return nil, err
	}

	return r.HGet(res[1])
}

func (r *RedisBroker) CreateGroup(groupUUID string, uuids []string) error {
	group := &GroupMetaData{
		UUID:     groupUUID,
		JobUUIDS: uuids,
	}

	return r.pool.Add(groupUUID, group)
}

func (r *RedisBroker) GetGroupInfo(groupUUID string) (*GroupMetaData, error) {
	buf, err := r.pool.Get(groupUUID)
	if err != nil {
		return nil, err
	}

	group := &GroupMetaData{}
	if err := json.Unmarshal(buf, group); err != nil {
		return nil, err
	}

	return group, nil
}

func (r *RedisBroker) IsGroupTaskCompleted(groupUUID string) (bool, error) {
	group, err := r.GetGroupInfo(groupUUID)
	if err != nil {
		return false, err
	}

	var g errgroup.Group

	for _, uuid := range group.JobUUIDS {
		uuid := uuid

		g.Go(func() error {
			buf, err := r.pool.Get(uuid)
			if err != nil {
				return err
			}

			msg := &task.Message{}
			if err := json.Unmarshal(buf, msg); err != nil {
				return err
			}

			if msg.Status != int(backend.TaskStatusDone) {
				return ErrGroupNotCompleted
			}

			return nil
		})
	}

	return g.Wait() == nil, nil
}

func (r *RedisBroker) DeleteGroup(groupUUID string) error {
	return r.pool.Delete(groupUUID)
}

func (r *RedisBroker) addDelayTask(msg task.Message) error {
	if err := r.getBucket(msg.ID).Add(msg.ID, msg.TTl); err != nil {
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
