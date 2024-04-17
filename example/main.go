package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"piper"
	"sync"
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

	publisher, err := piper.NewPublisher(piper.PublisherConfig{
		Exchange:     "test.exchange",
		ExchangeKind: "topic",
		RoutingKey:   "test",
	}, connect)

	err = publisher.Connect()
	if err != nil {
		panic(err)
	}
	go publisher.Run(ctx)

	consumer, err := piper.NewConsumer(piper.ConsumerConfig{
		Exchange:     "test.exchange",
		ExchangeKind: "topic",
		RoutingKey:   "test",
		Queue:        "test",
		Routines:     40,
	}, connect)

	var wg sync.WaitGroup

	go func(ctx context.Context) {
		wg.Add(1)
		defer wg.Done()

		go consumer.Start(ctx, processing())
		<-consumer.WaitClose()

		log.Println("Consumer closed")
	}(ctx)

	for i := 0; i <= 20; i++ {
		publisher.Publish(ctx) <- piper.Message{
			Payload: i,
			UID:     "uid",
		}
	}

	<-sigs

	log.Println(ctx, "[os.SIGNAL] start shutdown")
	cancel()
	wg.Wait()

	log.Println("[os.SIGNAL] success shutdown")
}

func processing() func(delivery amqp.Delivery, index int) error {
	return func(delivery amqp.Delivery, index int) error {
		log.Println(delivery, index)
		return nil
	}
}
