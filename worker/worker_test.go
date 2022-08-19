package worker

import (
	"encoding/json"
	"testing"

	"github.com/KKKKjl/eTask/internal/task"
	"github.com/stretchr/testify/assert"
)

func sum(a, b int) int {
	return a + b
}

func TestConsume(t *testing.T) {
	assert := assert.New(t)

	worker := New(nil, nil)
	worker.Add("sum", sum, nil)

	var msg task.Message
	assert.Nil(json.Unmarshal([]byte("{\"id\":\"2c0f98da-28d4-4a71-831a-c56bcdebe3af\",\"namespace\":\"sum\",\"args\":[1,2],\"created_at\":\"2022-08-16T12:34:11.6716804+08:00\",\"ttl\":0,\"result\":null,\"callback\":null,\"retry\":0,\"execution_time\":0,\"status\":0,\"stackback\":\"\"}"), &msg))

	out, err := worker.consume(msg.NameSpace, msg.NextJobId, msg.Args...)
	assert.Nil(err)
	assert.Equal(int64(3), out[0])
}

func TestCallback(t *testing.T) {
	assert := assert.New(t)

	fn := func(a int) int {
		return a + 1
	}

	worker := New(nil, nil)
	worker.Add("sum", sum, fn)

	var msg task.Message
	assert.Nil(json.Unmarshal([]byte("{\"id\":\"2c0f98da-28d4-4a71-831a-c56bcdebe3af\",\"namespace\":\"sum\",\"args\":[1,2],\"created_at\":\"2022-08-16T12:34:11.6716804+08:00\",\"ttl\":0,\"result\":null,\"callback\":null,\"retry\":0,\"execution_time\":0,\"status\":0,\"stackback\":\"\"}"), &msg))

	out, err := worker.consume(msg.NameSpace, msg.NextJobId, msg.Args...)
	assert.Nil(err)
	assert.Equal(int64(4), out[0])
}
