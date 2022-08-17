package client

import (
	"context"
	"encoding/json"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/broker"
	"github.com/KKKKjl/eTask/message"
	"github.com/KKKKjl/eTask/worker"
	"github.com/KKKKjl/eTask/workflow"
)

type EtaskClient struct {
	broker  broker.Broker
	backend backend.Backend
	worker  *worker.Worker
	done    chan struct{}
}

func New(b broker.Broker, backend backend.Backend) *EtaskClient {
	return &EtaskClient{
		broker:  b,
		backend: backend,
		worker:  worker.New(b, backend),
		done:    make(chan struct{}),
	}
}

func (e *EtaskClient) EnsureAsync(ctx context.Context, name string, args []interface{}, opts ...message.Option) *ResultCmd {
	msg := message.NewMessage(name, args, opts...)

	cmd := NewResultCmd("result", ctx, msg)

	e.process(cmd)
	return cmd
}

func (e *EtaskClient) PipelineAsync(ctx context.Context, pipline workflow.Pipeliner) *ResultCmd {
	cmd := NewResultCmd("result", ctx, pipline.Jobs()[0])

	for _, job := range pipline.Jobs()[1:] {
		if err := e.broker.HSet(*job); err != nil {
			cmd.err = err
			return cmd
		}
	}
	e.process(cmd)
	return cmd
}

func (e *EtaskClient) process(cmd *ResultCmd) {
	if cmd.err != nil {
		return
	}

	if err := e.broker.Enqueue(*cmd._args[1].(*message.Message)); err != nil {
		cmd.err = err
		return
	}

	buf, err := e.backend.GetResult(cmd._args[0].(context.Context), cmd._args[1].(*message.Message).ID)
	if err != nil {
		cmd.err = err
		return
	}

	cmd.val = string(buf)
}

// register a task to the worker
func (e *EtaskClient) Add(name string, fn interface{}, callback interface{}) error {
	return e.worker.Add(name, fn, callback)
}

// start worker
func (e *EtaskClient) Run(workerNum int) {
	e.worker.StartWorker(context.Background(), workerNum)
}

// stop worker and wait for all tasks to be done
func (e *EtaskClient) Shutdown() {
	e.worker.Stop()
}

type baseCmd struct {
	_args []interface{}
	err   error
}

type ResultCmd struct {
	baseCmd
	val string
}

func NewResultCmd(name string, args ...interface{}) *ResultCmd {
	return &ResultCmd{
		baseCmd: baseCmd{
			_args: args,
		},
	}
}

func (r *ResultCmd) Bytes() []byte {
	return []byte(r.val)
}

func (r *ResultCmd) String() string {
	return r.val
}

func (r *ResultCmd) Result(val interface{}) error {
	if r.err != nil {
		return r.err
	}

	return json.Unmarshal([]byte(r.val), val)
}
