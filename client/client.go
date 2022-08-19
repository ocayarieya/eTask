package client

import (
	"context"

	"github.com/KKKKjl/eTask/backend"
	"github.com/KKKKjl/eTask/broker"
	"github.com/KKKKjl/eTask/internal/result"
	"github.com/KKKKjl/eTask/internal/task"
	"github.com/KKKKjl/eTask/internal/workflow"
	"github.com/KKKKjl/eTask/worker"

	"golang.org/x/sync/errgroup"
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

func (e *EtaskClient) EnsureAsync(name string, args []interface{}, opts ...task.Option) (*result.AsyncResult, error) {
	msg := task.NewMessage(name, args, opts...)

	if err := e.broker.Enqueue(*msg); err != nil {
		return nil, err
	}

	return result.NewAsyncResult(msg.ID, e.backend), nil
}

func (e *EtaskClient) PipelineAsync(pipline workflow.Pipeliner) (*result.AsyncResult, error) {
	var g errgroup.Group

	if err := e.broker.Enqueue(*pipline.Jobs()[0]); err != nil {
		return nil, err
	}

	for _, job := range pipline.Jobs()[1:] {

		job := job
		g.Go(func() error {
			return e.broker.HSet(*job)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return result.NewAsyncResult(pipline.Jobs()[pipline.Length()-1].ID, e.backend), nil
}

func (e *EtaskClient) GroupAsync(group *workflow.Group) ([]*result.AsyncResult, error) {
	groupResult := make([]*result.AsyncResult, len(group.Jobs()))

	// create a group
	if err := e.broker.CreateGroup(group.GroupUUID(), group.JobUUIDS()); err != nil {
		return nil, err
	}

	var g errgroup.Group
	for index, job := range group.Jobs() {
		index := index
		job := job

		g.Go(func() error {
			err := e.broker.Enqueue(*job)
			if err != nil {
				return err
			}

			groupResult[index] = result.NewAsyncResult(job.ID, e.backend)
			return nil
		})

	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return groupResult, nil
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
