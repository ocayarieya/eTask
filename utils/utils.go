package utils

import (
	"log"

	"github.com/google/uuid"
)

func GetUUID() string {
	return uuid.New().String()
}

func Async(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovered err:%v", err)
		}
	}()

	go f()
}
