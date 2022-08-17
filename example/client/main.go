package main

import (
	"context"
	"log"

	"github.com/KKKKjl/eTask/backend/redis_backend"
	"github.com/KKKKjl/eTask/broker/redis_broker"
	"github.com/KKKKjl/eTask/client"
	"github.com/KKKKjl/eTask/message"
	"github.com/KKKKjl/eTask/workflow"
	"github.com/go-redis/redis"
)

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

	// create a pipeline
	pipeline := workflow.NewPipeliner(
		message.NewMessage("sum", []interface{}{1, 2}),
		message.NewMessage("sum", []interface{}{}),
	)

	var result message.Message
	if err := client.PipelineAsync(context.Background(), pipeline).Result(&result); err != nil {
		panic(err)
	}

	// invoke and wait for response
	if err := client.EnsureAsync(context.Background(), "sum", []interface{}{1, 2}, message.WithTTl(20)).Result(&result); err != nil {
		panic(err)
	}

	log.Printf("result: %v", result.Out)
}
