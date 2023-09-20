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
	c, err := piper.NewReadQueue(connect, "test.exchange", "test", 2, "test")
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

	c.WithReport("test.report", "test.report")
	go c.Run()
	if err != nil {
		panic(err)
	}
	for message := range c.Read() {
		fmt.Println(message)
		c.Report() <- piper.Report{
			Done: &piper.DoneReport{
				Status: 1,
			},
			UID: message.UID,
		}
	}
}
