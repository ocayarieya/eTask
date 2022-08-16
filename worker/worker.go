package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/broker"
	"github.com/KKKKjl/eTask/logger"
	"github.com/KKKKjl/eTask/message"
	"github.com/KKKKjl/eTask/utils"
)

const (
	DEFAULT_MAX_WORKERS = 1000
)

var (
	ErrMaxTasksReached = errors.New("max tasks reached")
	ErrTaskExpired     = errors.New("task expired")
)

var log = logger.GetLogger()

type Worker struct {
	id            string
	broker        broker.Broker
	backend       backend.Backend
	tasks         map[string]interface{}
	lock          sync.RWMutex
	limitInterval time.Duration
	done          chan struct{}
	once          sync.Once
	errHandler    func(msg *message.Message, err error)
}

func New(b broker.Broker, backend backend.Backend, opts ...Option) *Worker {
	w := &Worker{
		id:            utils.GetUUID(),
		broker:        b,
		backend:       backend,
		tasks:         make(map[string]interface{}),
		done:          make(chan struct{}),
		limitInterval: 5 * time.Second,
		errHandler: func(msg *message.Message, err error) {
			log.Errorf("envoke task(%s) error: %v", msg.ID, err)
		},
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Add adds a task to the worker.
func (w *Worker) Add(name string, fn interface{}) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if _, ok := w.tasks[name]; !ok {
		w.tasks[name] = fn
		return nil
	}

	return fmt.Errorf("task %s already exists", name)
}

// GetTaskByName returns the task by name.
func (w *Worker) GetTaskByName(name string) interface{} {
	w.lock.RLock()
	defer w.lock.RUnlock()

	task, ok := w.tasks[name]
	if !ok {
		return nil
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
			var (
				err error
				msg *message.Message
			)

			// dequeue a task
			msg, err = w.broker.Dequeue()
			if err != nil || msg == nil {
				log.Errorf("fetch task from %s err: %v", w.broker.Scheme(), err)
				continue
			}

			err = w.envoke(msg)
			if err != nil {
				if msg.Retry > 0 && !errors.Is(err, ErrTaskExpired) {
					w.createDelayTask(msg)
				}

				w.errHandler(msg, err)
				w.backend.UpdateTask(msg, backend.TaskStatusError)
				continue
			}

			log.Infof("Envoke task(%s) success.", msg.ID)
			w.backend.UpdateTask(msg, backend.TaskStatusDone)
		}
	}
}

// envoke a task
func (w *Worker) envoke(msg *message.Message) (err error) {
	defer func() {
		if err := recover(); err != nil {
			msg.Stackback = string(debug.Stack())
			log.Errorf("envoke task(%s) error: %v", msg.ID, err)
		}
	}()

	// check expired task
	// if msg.TTl > 0 && int64(time.Since(msg.CreatedAt).Seconds()) > msg.TTl {
	// 	log.Infof("task(%s) expired", msg.ID)
	// 	return ErrTaskExpired
	// }

	fn := w.GetTaskByName(msg.NameSpace)
	if fn == nil {
		return fmt.Errorf("task %s not found", msg.NameSpace)
	}

	fnValue := reflect.ValueOf(fn)
	if fnValue.Kind() != reflect.Func {
		return fmt.Errorf("task(%s) is not a function", msg.ID)
	}

	t := fnValue.Type()
	if len(msg.Args) != t.NumIn() {
		return fmt.Errorf("task(%s) argument count mismatch, required %d got %d", msg.ID, t.NumIn(), len(msg.Args))
	}

	args := make([]reflect.Value, len(msg.Args))
	for i, v := range msg.Args {
		inValue := reflect.ValueOf(v)
		requiredType := t.In(i).Kind()

		// NOTE: JSON unmarshal will convert all numeric types to float64.
		if requiredType == reflect.Int {
			inValue = reflect.ValueOf(int(v.(float64)))
		}

		if inValue.Type().Kind() != requiredType {
			return fmt.Errorf("Task(%s) argument type mismatch, required %v got %v", msg.ID, t.In(i), inValue.Type())
		}

		args[i] = inValue
	}

	res := fnValue.Call(args)
	if len(res) > 0 {
		msg.Out = w.parseResult(res)
	}

	return nil
}

func (w *Worker) parseResult(outs []reflect.Value) []interface{} {
	result := make([]interface{}, 0, len(outs))
	for _, v := range outs {
		result = append(result, w.getRealValue(v))
	}
	return result
}

func (w *Worker) getRealValue(t reflect.Value) interface{} {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return t.Int()
	case reflect.String:
		return t.String()
	case reflect.Bool:
		return t.Bool()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return t.Uint()
	case reflect.Float32, reflect.Float64:
		return t.Float()
	case reflect.Slice, reflect.Map:
		return t.Interface()
	default:
		return t.Interface()
	}
}

func (w *Worker) createDelayTask(retryTask *message.Message) {
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

	log.Infof("create delaytask for task(%s)", retryTask.ID)
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
func (w *Worker) SetErrorHandler(fn func(msg *message.Message, err error)) {
	w.errHandler = fn
}
