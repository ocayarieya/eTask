package main

import (
	"context"
	"log"
	"sync"

	"github.com/KKKKjl/eTask/backend/redis_backend"
	"github.com/KKKKjl/eTask/broker/redis_broker"
	"github.com/KKKKjl/eTask/client"
	"github.com/KKKKjl/eTask/internal/task"
	"github.com/KKKKjl/eTask/internal/workflow"
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

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		// create a pipeline
		pipeline := workflow.NewPipeliner(
			task.NewMessage("sum", []interface{}{5, 5}),
			task.NewMessage("sum", []interface{}{10}),
			task.NewMessage("multiply", []interface{}{5}),
		)

		// execute the pipeline
		result, err := client.PipelineAsync(pipeline)
		if err != nil {
			log.Printf("pipline error: %v", err)
			return
		}

		// wait for the result
		out, err := result.Wait(context.TODO())
		if err != nil {
			log.Printf("pipline error: %v", err)
			return
		}

		log.Printf("pipline result: %v", out)
	}()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()

	// 	// create a group
	// 	group := workflow.NewGroup(
	// 		task.NewMessage("sum", []interface{}{1, 2}),
	// 		task.NewMessage("sum", []interface{}{3, 4}),
	// 	)

	// 	// execute the group
	// 	groupResult, err := client.GroupAsync(group)
	// 	if err != nil {
	// 		log.Printf("group error: %v", err)
	// 		return
	// 	}

	// 	// wait for the result
	// 	for _, result := range groupResult {
	// 		out, err := result.Wait(context.TODO())
	// 		if err != nil {
	// 			log.Printf("group error: %v", err)
	// 			return
	// 		}

	// 		log.Printf("group %s result: %v", result.ID(), out)
	// 	}
	// }()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	result, err := client.EnsureAsync("sum", []interface{}{1, 2}, task.WithTTl(5))
	// 	if err != nil {
	// 		log.Printf("error: %v", err)
	// 		return
	// 	}

	// 	out, err := result.Wait(context.TODO())
	// 	if err != nil {
	// 		log.Printf("error: %v", err)
	// 		return
	// 	}

	// 	log.Printf("result: %v", out[0])
	// }()

	wg.Wait()
}
