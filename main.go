package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

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

	go func() {
		m1, err := subscriber1(rdb, channel)
		if err != nil {
			errChan1 <- err
		}
		close(errChan1)
		receivedMsg1 = m1
	}()

	go func() {
		m2, err := subscriber2(rdb, channel)
		if err != nil {
			errChan2 <- err

		}
		close(errChan2)
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

	select {
	case err1 := <-errChan1:
		if err1 != nil {
			log.Println(err1)
		}
	case err2 := <-errChan2:
		if err2 != nil {
			log.Println(err2)
		}

	}

	log.Println("res 1: ", receivedMsg1)
	log.Println("res 2: ", receivedMsg2)

}

// example of the worker
func subscriber1(rdb *redis.Client, channel string) (string, error) {
	sub := rdb.Subscribe(context.Background(), channel)
	m, err := sub.ReceiveMessage(context.Background())

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
