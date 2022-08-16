package backend

import (
	"context"

	"github.com/KKKKjl/eTask/message"
)

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusRunning
	TaskStatusDone
	TaskStatusError
)

const (
	// BackendTypeRedis is the name of the redis backend
	BackendTypeRedis = "redis"
	// BackendTypeMemory is the name of the memory backend
	BackendTypeMemory = "memory"
)

type Backend interface {
	// UpdateTask updates the status of a task.
	UpdateTask(msg *message.Message, status TaskStatus) error

	// GetResult returns the result of a task.
	GetResult(ctx context.Context, key string) ([]byte, error)
}
