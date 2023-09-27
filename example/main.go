package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"piper"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	connect, err := piper.NewConnection(
		"amqp://root:pass@localhost:5672",
		[]time.Duration{
			time.Second * 10,
			time.Second * 20,
			time.Second * 30,
			time.Second * 60,
		},
	)
	if err != nil {
		panic(err)
	}
	err = connect.Connect(ctx)
	if err != nil {
		panic(err)
	}
	publish, err := piper.NewPublisher(piper.PublisherConfig{
		Exchange:     "test.exchange",
		ExchangeKind: "topic",
		RoutingKey:   "test",
	}, connect)
	err = publish.Connect()
	if err != nil {
		panic(err)
	}
	go publish.Run(ctx)

	reportPublisher, err := piper.NewPublisher(piper.PublisherConfig{
		Exchange:     "test.report",
		ExchangeKind: "topic",
		RoutingKey:   "test.report",
	}, connect)
	err = reportPublisher.Connect()
	if err != nil {
		panic(err)
	}
	go reportPublisher.Run(ctx)

	consumer, err := piper.NewConsumer(piper.ConsumerConfig{
		Exchange:     "test.exchange",
		ExchangeKind: "topic",
		RoutingKey:   "test",
		Queue:        "test",
		Routines:     40,
	}, connect)
	go consumer.Run(ctx)

	go func() {
		for i := 0; i <= 20; i++ {
			publish.Publish() <- piper.Message{
				Payload: i,
				UID:     "uid",
			}
		}
	}()
	go func() {
		<-sigs
		cancel()
	}()
	for message := range consumer.Read() {
		fmt.Println(message.Payload.(float64))
		if reportPublisher.IsClose() {
			continue
		}
		reportPublisher.Publish() <- piper.Message{
			Payload: &piper.DoneReport{
				Status: 1,
			},
			UID: message.UID,
		}
	}
	fmt.Println("Complete Program")
}
