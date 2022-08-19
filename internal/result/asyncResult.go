package result

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/internal/task"
)

type AsyncResult struct {
	id      string
	backend backend.Backend
}

func NewAsyncResult(id string, backend backend.Backend) *AsyncResult {
	return &AsyncResult{
		id:      id,
		backend: backend,
	}
}

func (a *AsyncResult) ID() string {
	return a.id
}

func (a *AsyncResult) Wait(ctx context.Context) ([]interface{}, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			buf, err := a.backend.GetResult(a.id)
			if err != nil {
				return nil, err
			}

			if buf == nil {
				time.Sleep(time.Second)
				continue
			}

			var result task.Message
			if err := json.Unmarshal(buf, &result); err != nil {
				return nil, err
			}

			if result.Status != int(backend.TaskStatusDone) {
				time.Sleep(time.Second)
			} else {
				return result.Out, nil
			}
		}
	}
}

func (a *AsyncResult) Done() bool {
	buf, err := a.backend.GetResult(a.id)
	if err != nil || buf == nil {
		return false
	}

	var result task.Message
	if err := json.Unmarshal(buf, &result); err != nil {
		return false
	}

	return result.Status == int(backend.TaskStatusDone)
}
