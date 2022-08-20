package worker

import (
	"testing"

	"github.com/KKKKjl/eTask/internal/task"
	"github.com/stretchr/testify/assert"
)

func sum(a, b int) int {
	return a + b
}

func multiply(a, b int) int {
	return a * b
}

type fakeBroker struct {
}

func (*fakeBroker) Enqueue(msg task.Message) error {
	return nil
}

func (*fakeBroker) Dequeue() (msg *task.Message, err error) {
	return nil, nil
}

func (*fakeBroker) HSet(msg task.Message) error {
	return nil
}

func (*fakeBroker) HGet(key string) (*task.Message, error) {
	return &task.Message{
		NameSpace: "multiply",
		Args:      []interface{}{4},
		NextJobId: "",
	}, nil
}

func (*fakeBroker) CreateGroup(groupUUID string, uuids []string) error {
	return nil
}

func (*fakeBroker) IsGroupTaskCompleted(groupUUID string) (bool, error) {
	return false, nil
}

func (*fakeBroker) DeleteGroup(groupUUID string) error {
	return nil
}

func (*fakeBroker) Scheme() string {
	return ""
}

func (*fakeBroker) Close() error {
	return nil
}

func TestConsume(t *testing.T) {
	assert := assert.New(t)

	worker := New(nil, nil)
	worker.Add("sum", sum, nil)

	job := &task.Message{
		NameSpace: "sum",
		Args:      []interface{}{1, 2},
	}

	out, err := worker.consume(job.NameSpace, job.NextJobId, job.Args...)
	assert.Nil(err)
	assert.Equal(int(3), out[0])
}

func TestCallback(t *testing.T) {
	assert := assert.New(t)

	fn := func(a int) int {
		return a + 1
	}

	worker := New(nil, nil)
	worker.Add("sum", sum, fn)
	job := &task.Message{
		NameSpace: "sum",
		Args:      []interface{}{1, 2},
	}

	out, err := worker.consume(job.NameSpace, job.NextJobId, job.Args...)
	assert.Nil(err)
	assert.Equal(int(4), out[0])
}

func TestPipline(t *testing.T) {
	assert := assert.New(t)

	worker := New(&fakeBroker{}, nil)
	worker.Add("sum", sum, nil)
	worker.Add("multiply", multiply, nil)

	job := &task.Message{
		NameSpace: "sum",
		Args:      []interface{}{1, 2},
		NextJobId: "2c0f98da-28d4-4a71-831a-c56bcdebe3af",
	}

	out, err := worker.consume(job.NameSpace, job.NextJobId, job.Args...)
	assert.Nil(err)
	assert.Equal(int(12), out[0])
}
