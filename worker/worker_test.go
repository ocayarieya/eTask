package worker

import (
	"encoding/json"
	"testing"

	"github.com/KKKKjl/eTask/message"
	"github.com/stretchr/testify/assert"
)

func sum(a, b int) int {
	return a + b
}

func division(a, b int) int {
	return a / b
}

func TestEnvoke(t *testing.T) {
	assert := assert.New(t)

	worker := New(nil, nil)
	worker.Add("sum", sum)

	var msg message.Message
	json.Unmarshal([]byte("{\"id\":\"2c0f98da-28d4-4a71-831a-c56bcdebe3af\",\"namespace\":\"sum\",\"args\":[1,2],\"created_at\":\"2022-08-16T12:34:11.6716804+08:00\",\"ttl\":0,\"result\":null,\"callback\":null,\"retry\":0,\"execution_time\":0,\"status\":0,\"stackback\":\"\"}"), &msg)

	err := worker.envoke(&msg)
	assert.Nil(err)
	assert.Equal(int64(3), msg.Out[0])
}

func TestEnvokeError(t *testing.T) {
	assert := assert.New(t)

	msg := message.NewMessage("division", []interface{}{1, 0})

	worker := New(nil, nil)
	worker.Add("division", division)

	err := worker.envoke(msg)
	assert.NotNil(err)
}
