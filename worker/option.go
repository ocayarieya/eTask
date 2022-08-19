package worker

import (
	"time"

	"github.com/KKKKjl/eTask/internal/task"
)

type Option func(*Worker)

func WithErrorHandler(fn func(msg *task.Message, err error)) Option {
	return func(w *Worker) {
		w.errHandler = fn
	}
}

func WithLimitInterval(d time.Duration) Option {
	return func(w *Worker) {
		w.limitInterval = d
	}
}
