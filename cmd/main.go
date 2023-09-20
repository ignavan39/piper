package main

import (
	"fmt"
	"piper"

	"github.com/streadway/amqp"
)

func main() {
	connect, err := amqp.Dial("amqp://user:pass@localhost:5672")
	if err != nil {
		panic(err)
	}
	c, err := piper.NewReadQueue(connect, "amq.topic", "test", 2, "test")
	if err != nil {
		panic(err)
	}

	c.WithReport("test.report", "test.report")
	go c.Run()
	if err != nil {
		panic(err)
	}
	for message := range c.MessagesChannel {
		fmt.Println(message)
		c.Report.Report <- piper.Report{
			Done: &piper.DoneReport{
				Status: 1,
			},
			UID: message.UID,
		}
	}
}
