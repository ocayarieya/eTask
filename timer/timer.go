package timer

import (
	"strconv"
	"sync"
	"time"

	"github.com/KKKKjl/eTask/logger"
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

const (
	LUA_SCRIPT = `
	local src = KEYS[1]
	local dst = KEYS[2]
	local currTime = KEYS[3]

	local members = redis.call('ZRANGEBYSCORE', src, 0, currTime)
	if #members > 0 then
		for _, member in ipairs(members) do
			redis.call('ZREM', src, member)
			redis.call('LPUSH', dst, member)
		end
		return 1
	end
	return 0
	`
)

// Timer fecthes task from delay bucket and put into 'ready' queue
type Timer struct {
	client   *redis.Client
	interval time.Duration
	luaHash  string
	done     chan struct{}
	log      *logrus.Logger
	once     sync.Once
	args     []string
}

func NewTimer(client *redis.Client, interval time.Duration, args []string) *Timer {
	timer := &Timer{
		client:   client,
		interval: interval,
		done:     make(chan struct{}),
		log:      logger.GetLogger(),
		args:     args,
	}

	// cache lua script
	hash, err := client.ScriptLoad(LUA_SCRIPT).Result()
	if err != nil {
		panic(err)
	}

	timer.luaHash = hash

	go timer.Ticker()

	return timer
}

func (t *Timer) Ticker() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-t.done:
			if t.log != nil {
				t.log.Info("Timer stopped")
			}
			return
		case <-ticker.C:
			nowStr := strconv.FormatInt(time.Now().Unix(), 10)

			args := make([]string, 0, len(t.args)+1)
			args = append(args, t.args...)
			args = append(args, nowStr)

			err := t.client.EvalSha(t.luaHash, args).Err()
			if err != nil {
				if t.log != nil {
					t.log.Errorf("execute lua script err: %v", err)
				}

				continue
			}
		}
	}
}

func (t *Timer) Stop() {
	t.once.Do(func() {
		close(t.done)
	})
}
