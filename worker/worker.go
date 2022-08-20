package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/broker"
	"github.com/KKKKjl/eTask/internal/logger"
	"github.com/KKKKjl/eTask/internal/task"
	"github.com/KKKKjl/eTask/internal/utils"
	"github.com/sirupsen/logrus"
)

const (
	DEFAULT_MAX_WORKERS = 1000
)

var (
	ErrMaxTasksReached = errors.New("max tasks reached")
	ErrTaskExpired     = errors.New("task expired")
	ErrNotFunction     = errors.New("not a function")
)

type FuncWorker struct {
	fn interface{} // task func
	cb interface{} // callback func
}

type Worker struct {
	id            string
	broker        broker.Broker
	backend       backend.Backend
	tasks         map[string]FuncWorker
	lock          sync.RWMutex
	limitInterval time.Duration
	done          chan struct{}
	once          sync.Once
	logger        *logrus.Logger
	errHandler    func(msg *task.Message, err error)
}

func New(b broker.Broker, backend backend.Backend, opts ...Option) *Worker {
	w := &Worker{
		id:            utils.GetUUID(),
		broker:        b,
		backend:       backend,
		tasks:         make(map[string]FuncWorker),
		done:          make(chan struct{}),
		limitInterval: 5 * time.Second,
		logger:        logger.GetLogger(),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Add adds a task to the worker.
func (w *Worker) Add(name string, fn interface{}, cb interface{}) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	utils.Assert(func() bool {
		return reflect.TypeOf(fn).Kind() == reflect.Func
	}, "task must be a func")

	if cb != nil {
		utils.Assert(func() bool {
			return reflect.TypeOf(cb).Kind() == reflect.Func
		}, "callback must be a func")
	}

	if _, ok := w.tasks[name]; !ok {
		w.tasks[name] = FuncWorker{
			fn: fn,
			cb: cb,
		}
		return nil
	}

	return fmt.Errorf("task %s already exists", name)
}

// GetTaskByName returns the task by name.
func (w *Worker) GetTaskByName(name string) FuncWorker {
	w.lock.RLock()
	defer w.lock.RUnlock()

	task, ok := w.tasks[name]
	if !ok {
		return FuncWorker{}
	}

	return task
}

func (w *Worker) run(ctx context.Context) {
	ticker := time.NewTicker(w.limitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// dequeue a task
			msg, err := w.broker.Dequeue()
			if err != nil || msg == nil {
				w.logger.Errorf("fetch task from %s err: %v", w.broker.Scheme(), err)
				continue
			}

			w.logger.Debugf("got message: id(%s), namespace(%s), group(%s), nextJob(%s)", msg.ID, msg.NameSpace, msg.GroupId, msg.NextJobId)

			out, err := w.consume(msg.NameSpace, msg.NextJobId, msg.Args...)
			if err != nil {
				if msg.Retry > 0 && !errors.Is(err, ErrTaskExpired) {
					w.createDelayTask(msg)
				}

				if w.errHandler != nil {
					w.errHandler(msg, err)
				}

				w.logger.Errorf("consume task %s err: %v", msg.ID, err)

				w.markAsFailed(msg)
				continue
			}

			w.markAsSuccess(msg, out)
		}
	}
}

// consum a message and envoke a task
func (w *Worker) consume(namespace string, nextJobId string, kwargs ...interface{}) ([]interface{}, error) {
	defer func() {
		if err := recover(); err != nil {
			w.logger.Errorf("captured panic: %v", err)
		}
	}()

	funcWorker := w.GetTaskByName(namespace)
	if funcWorker.fn == nil {
		return nil, fmt.Errorf("task %s not found", namespace)
	}

	t := reflect.ValueOf(funcWorker.fn).Type()

	args := make([]reflect.Value, len(kwargs))
	for i, v := range kwargs {
		requiredType := t.In(i).Kind()

		// NOTE: JSON unmarshal will convert all numeric types to float64.
		if requiredType == reflect.Int && reflect.TypeOf(v).Kind() == reflect.Float64 {
			v = int(v.(float64))
		}

		if requiredType == reflect.Float32 && reflect.TypeOf(v).Kind() == reflect.Float64 {
			v = float32(v.(float64))
		}

		realType := reflect.TypeOf(v).Kind()
		if realType != requiredType {
			return nil, fmt.Errorf("argument type mismatch, required %v got %v", t.In(i), realType)
		}

		args[i] = reflect.ValueOf(v)
	}

	res, err := w.invoke(funcWorker.fn, funcWorker.cb, args)
	if err != nil {
		return nil, err
	}

	// if nextJob is not nil, it means the task is a chain task.
	if nextJobId != "" {
		nextJob, err := w.broker.HGet(nextJobId)
		if err != nil {
			return nil, err
		}

		input := w.parseResult(res)

		nextJobArgs := make([]interface{}, 0, len(nextJob.Args)+len(input))
		nextJobArgs = append(nextJobArgs, nextJob.Args...)
		nextJobArgs = append(nextJobArgs, input...)

		return w.consume(nextJob.NameSpace, nextJob.NextJobId, nextJobArgs...)
	}

	return w.parseResult(res), nil
}

// envoke a task
func (w *Worker) invoke(fn interface{}, cb interface{}, args []reflect.Value) ([]reflect.Value, error) {
	fnValue := reflect.ValueOf(fn)
	if fnValue.Kind() != reflect.Func {
		return nil, ErrNotFunction
	}

	t := fnValue.Type()
	if len(args) != t.NumIn() {
		return nil, fmt.Errorf("argument count mismatch, required %d got %d", t.NumIn(), len(args))
	}

	res := fnValue.Call(args)

	// call callback function
	if cb != nil {
		if reflect.TypeOf(cb).NumIn() > 0 {
			return w.invoke(cb, nil, res)
		}

		return w.invoke(cb, nil, []reflect.Value{})
	}

	return res, nil
}

func (w *Worker) parseResult(outs []reflect.Value) []interface{} {
	result := make([]interface{}, 0, len(outs))
	for _, v := range outs {
		result = append(result, reflect.Indirect(v).Interface())
	}
	return result
}

func (w *Worker) createDelayTask(retryTask *task.Message) {
	if retryTask.Retry <= 0 {
		return
	}

	// exponential extension ttl
	rand.Seed(time.Now().UnixNano())
	ttl := math.Pow(float64(retryTask.Retry), 4) + 15 + rand.Float64()*float64(retryTask.Retry)*30

	// decrease retry count
	retryTask.Retry--

	// reset task ttl
	retryTask.TTl = int64(ttl)

	// submit delay task
	w.broker.Enqueue(*retryTask)

	w.logger.Debugf("create delaytask for task(%s)", retryTask.ID)
}

// Run starts the worker.(non-blocking)
func (w *Worker) StartWorker(ctx context.Context, workerNum int) {
	if workerNum <= 1 {
		panic("worker num must be greater than 1")
	}

	// limit the number of goroutines to prevent memory leak
	// TODO auto adjust goroutine number
	var capacity int = workerNum
	if workerNum > DEFAULT_MAX_WORKERS {
		capacity = DEFAULT_MAX_WORKERS
	}

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		<-w.done
		cancel()
	}()

	ch := make(chan struct{}, capacity)
	for i := 0; i < workerNum; i++ {
		ch <- struct{}{}

		utils.Async(func() {
			defer func() {
				<-ch
			}()
			w.run(ctx)
		})
	}
}

// Stop stops the worker gracefully.
func (w *Worker) Stop() {
	// ensure that the worker is stopped only once
	w.once.Do(func() {
		// if err := w.broker.Close(); err != nil {
		// 	log.Errorf("close broker(%s) %s connection err: %v", w.id, w.broker.Scheme(), err)
		// }
		close(w.done)
	})
}

// SetErrorHandler sets the error handler for the worker.
func (w *Worker) SetErrorHandler(fn func(msg *task.Message, err error)) {
	w.errHandler = fn
}

func (w *Worker) markAsSuccess(msg *task.Message, out []interface{}) error {
	w.logger.Debugf("mark task %s as success", msg.ID)

	msg.Out = out

	err := w.backend.UpdateTask(msg, backend.TaskStatusDone)
	if err != nil {
		return err
	}

	if msg.GroupId != "" {
		// check if all tasks in the group are finished
		completed, err := w.broker.IsGroupTaskCompleted(msg.GroupId)
		if err != nil {
			return err
		}

		if !completed {
			return nil
		}

		w.logger.Debugf("Group task %s all completed.", msg.GroupId)

		// group task all completed, delete group
		if err := w.broker.DeleteGroup(msg.GroupId); err != nil {
			return err
		}

		w.logger.Debugf("Group task %s deleted.", msg.GroupId)
	}

	return nil
}

func (w *Worker) markAsFailed(msg *task.Message) error {
	w.logger.Debugf("mark task %s as failed.", msg.ID)
	return w.backend.UpdateTask(msg, backend.TaskStatusError)
}
