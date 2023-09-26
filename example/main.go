package main

import (
	"piper"
	"time"

	"github.com/streadway/amqp"
)

func main() {
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
	err = connect.Connect()
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
	go publish.Run()

	reportPublisher, err := piper.NewPublisher(piper.PublisherConfig{
		Exchange:     "test.report",
		ExchangeKind: "topic",
		RoutingKey:   "test.report",
	}, connect)
	err = reportPublisher.Connect()
	if err != nil {
		panic(err)
	}
	go reportPublisher.Run()

	consumer, err := piper.NewConsumer(piper.ConsumerConfig{
		Exchange:     "test.exchange",
		ExchangeKind: "topic",
		RoutingKey:   "test",
		Queue:        "test",
		Routines:     40,
	}, connect)
	go consumer.Run()

	go func() {
		for i := 0; i <= 20; i++ {
			publish.Publish() <- piper.Message{
				Payload: i,
				UID:     "uid",
			}
		}
	}()
	for message := range consumer.Read() {
		reportPublisher.Publish() <- piper.Message{
			Payload: &piper.DoneReport{
				Status: 1,
			},
			UID: message.UID,
		}

	}
}

func main2() {
	connect, err := amqp.Dial("amqp://root:pass@localhost:5672")
	if err != nil {
		panic(err)
	}
	c, err := piper.NewReadQueue(connect, "test.exchange", "test", 40, "test")
	if err != nil {
		panic(err)
	}

	wq, err := piper.NewWriteQueue(connect, "test.exchange", "test")
	if err != nil {
		panic(err)
	}
	go wq.Run()

	go func() {
		for i := 0; i <= 20; i++ {
			wq.Write() <- piper.Message{
				Payload: i,
				UID:     "uid",
			}
		}
	}()

	rq, err := c.WithReport("test.report", "test.report")
	if err != nil {
		panic(err)
	}
	go func() {
		err := rq.Run()
		if err != nil {
			panic(err)
		}
	}()
	for message := range rq.Read() {
		rq.Report() <- piper.Report{
			Done: &piper.DoneReport{
				Status: 1,
			},
			UID: message.UID,
		}
	}
}
