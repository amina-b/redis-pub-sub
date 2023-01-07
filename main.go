package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	redis "github.com/go-redis/redis/v9"
)

func main() {

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDRESS"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})
	channel := os.Getenv("REDIS_CHANNEL")
	errChan1 := make(chan error)
	errChan2 := make(chan error)

	// SUBSCRIBERS
	var receivedMsg1, receivedMsg2 string
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		m1, err := subscriber1(rdb, channel)
		if err != nil {
			errChan1 <- err
			return
		}
		receivedMsg1 = m1
	}()

	go func() {

		defer wg.Done()
		m2, err := subscriber2(rdb, channel)
		if err != nil {
			errChan2 <- err
			return
		}
		receivedMsg2 = m2
	}()

	// PUBLISHER
	msg, err := getMessage()
	if err != nil {
		return
	}

	resp := rdb.Publish(context.Background(), channel, msg)

	if resp.Err() != nil {
		log.Fatalf("failed to publish message", err.Error())
	}

	err = <-errChan1
	if err != nil {
		log.Println(err)
	}

	err = <-errChan2
	if err != nil {
		log.Println(err)
	}

	wg.Wait()
	close(errChan1)
	close(errChan2)

	log.Println("res 1: ", receivedMsg1)
	log.Println("res 2: ", receivedMsg2)

}

// example of the worker
func subscriber1(rdb *redis.Client, channel string) (string, error) {
	sub := rdb.Subscribe(context.Background(), channel)
	m, err := sub.ReceiveMessage(context.Background())
	err = errors.New("test err from S1")
	if err != nil {
		log.Println("Error: ", err)
		return "", err
	}

	return fmt.Sprintf("this is message from the FIRST subscriber: %s", m.Payload), nil
}

// example of the workder
func subscriber2(rdb *redis.Client, channel string) (string, error) {
	sub := rdb.Subscribe(context.Background(), channel)
	m, err := sub.ReceiveMessage(context.Background())
	err = errors.New("test err from S2")
	if err != nil {
		log.Println("Error: ", err)
		return "", err
	}

	return fmt.Sprintf("this is message from the SECOND subscriber: %s", m.Payload), nil
}

// get input from console
func getMessage() (string, error) {

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter text: ")

	msg, err := reader.ReadString('\n')

	if err != nil {
		log.Printf("failed to read input. Error: %v", err)
		return "", err
	}

	return msg, nil

}
