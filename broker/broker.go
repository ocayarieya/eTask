package broker

import "github.com/KKKKjl/eTask/message"

type Broker interface {
	// Enqueue adds a message to the queue.
	Enqueue(msg message.Message) error

	// Dequeue removes and returns a message from the queue.
	Dequeue() (msg *message.Message, err error)

	// Scheme returns the scheme of the broker.
	Scheme() string

	// Close closes the broker.
	Close() error
}
