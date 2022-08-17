package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/KKKKjl/eTask/backend/redis_backend"
	"github.com/KKKKjl/eTask/broker/redis_broker"
	"github.com/KKKKjl/eTask/client"
	"github.com/go-redis/redis"
)

func sum(a, b int) int {
	return a + b
}

func main() {
	// create a redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:49153",
		Password: "redispw", // no password set
		DB:       0,         // use default DB
	})

	// check connection
	if _, err := rdb.Ping().Result(); err != nil {
		panic(err)
	}

	client := client.New(
		redis_broker.NewRedisBroker(rdb),
		redis_backend.NewRedisBackend(rdb),
	)

	// register a task
	client.Add("sum", sum, nil)

	// run worker and set max
	client.Run(10)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit
	// shutdown worker gracefully
	client.Shutdown()
}
