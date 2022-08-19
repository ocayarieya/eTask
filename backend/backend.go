package backend

import (
	"github.com/KKKKjl/eTask/internal/task"
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
	UpdateTask(msg *task.Message, status TaskStatus) error

	// GetResult returns the result of a task.
	GetResult(key string) ([]byte, error)
}
