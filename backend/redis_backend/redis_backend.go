package redis_backend

import (
	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/internal/pool"
	"github.com/KKKKjl/eTask/internal/task"
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
func (r *RedisBackend) UpdateTask(msg *task.Message, status backend.TaskStatus) error {
	msg.Status = int(status)
	return r.pool.Add(msg.ID, msg)
}

// GetTask returns a task result from the backend asynchronously.
func (r *RedisBackend) GetResult(key string) ([]byte, error) {
	return r.pool.Get(key)
}
