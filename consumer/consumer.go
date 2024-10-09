package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	conn, err := connectConsumer([]string{"localhost:29092"})

	if err != nil {
		panic(err)
	}

	consumer, err := conn.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer Started")

	signChannel := make(chan os.Signal, 1)
	signal.Notify(signChannel, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneChannel := make(chan struct{})

	go func() {
		for {
			select {

			case err := <-consumer.Errors():
				fmt.Println(err)

			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Recieved Message Count: %d | Topic: %s | Message: %s", msgCount, string(msg.Topic), string(msg.Value))

			case <-signChannel:
				fmt.Println("Interruption Detected")
				doneChannel <- struct{}{}
			}
		}
	}()

	<-doneChannel
	fmt.Println("Processed", msgCount, "messages")
	if err := conn.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)

	if err != nil {
		return nil, err
	}

	return conn, nil
}
