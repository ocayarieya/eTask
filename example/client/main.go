package main

import (
	"context"
	"log"

	"github.com/KKKKjl/eTask/backend/redis_backend"
	"github.com/KKKKjl/eTask/broker/redis_broker"
	"github.com/KKKKjl/eTask/client"
	"github.com/KKKKjl/eTask/message"
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

	// invoke and wait for response
	var result message.Message
	if err := client.EnsureAsync(context.Background(), "sum", []interface{}{1, 2}, message.WithTTl(20)).Result(&result); err != nil {
		panic(err)
	}

	log.Printf("result: %v", result.Out)
}
