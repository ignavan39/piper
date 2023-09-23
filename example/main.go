package main

import (
	"piper"
	"time"
)

func main() {
	conn, err := piper.NewConnection(piper.ConnectionOptions{
		Host:     "localhost",
		Password: "pass",
		UserName: "root",
		Port:     5672,
	}, time.Second*5, time.Second*5)

	if err != nil {
		panic(err)
	}
	c, err := piper.NewReadQueue(conn, "test.exchange", "test", 40, "test")
	if err != nil {
		panic(err)
	}

	wq, err := piper.NewWriteQueue(conn, "test.exchange", "test")
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
