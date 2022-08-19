package broker

import "github.com/KKKKjl/eTask/internal/task"

type Broker interface {
	// Enqueue adds a message to the queue.
	Enqueue(msg task.Message) error

	// Dequeue removes and returns a message from the queue.
	Dequeue() (msg *task.Message, err error)

	// Hset adds a message to the message pool.
	HSet(msg task.Message) error

	// Hget get a message from the message pool.
	HGet(key string) (*task.Message, error)

	// CreateGroup creates a group of messages.
	CreateGroup(groupUUID string, uuids []string) error

	// IsGroupTaskCompleted returns true if all tasks in the group are completed.
	IsGroupTaskCompleted(groupUUID string) (bool, error)

	// DeleteGroup deletes a group of messages.
	DeleteGroup(groupUUID string) error

	// Scheme returns the scheme of the broker.
	Scheme() string

	// Close closes the broker.
	Close() error
}
