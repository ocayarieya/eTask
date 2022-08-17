package redis_backend

import (
	"context"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/message"
	"github.com/KKKKjl/eTask/pool"
	"github.com/go-redis/redis"
)

const (
	DEFAULT_KEY_EXPIRATION = 60 * 60 * 24 // 1 day
)

type RedisBackend struct {
	client *redis.Client
	pool   *pool.Pool
}

func NewRedisBackend(client *redis.Client) *RedisBackend {
	return &RedisBackend{
		client: client,
		pool:   pool.NewPool(client, "task_pool"),
	}
}

// UpdateTask updates a task result in the backend.
func (r *RedisBackend) UpdateTask(msg *message.Message, status backend.TaskStatus) error {
	msg.Status = int(status)
	return r.pool.Add(*msg)
}

// GetTask returns a task result from the backend asynchronously.
func (r *RedisBackend) GetResult(ctx context.Context, key string) ([]byte, error) {
	<-ctx.Done()
	return r.pool.Get(key)
}
