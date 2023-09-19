package main

import (
	"fmt"
	"piper/app/rabbitmq"

	"github.com/streadway/amqp"
)

func main() {
	connect, err := amqp.Dial("amqp://user:pass@localhost:5672")
	if err != nil {
		panic(err)
	}
	c, err := rabbitmq.NewReadQueue(connect, "amq.topic", "kek", 2, "kek")
	if err != nil {
		panic(err)
	}

	c.WithReport("kek.report", "kek.report")
	go c.Run()
	if err != nil {
		panic(err)
	}
	for kek := range c.MessagesChannel {
		fmt.Println(kek)
		c.Report.Report <- rabbitmq.Report{
			Done: &rabbitmq.DoneReport{
				Status: 1,
			},
			UID: kek.UID,
		}
	}
}
