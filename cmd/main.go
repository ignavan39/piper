package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

type Message struct {
	UID     string `json:"uid"`
	Payload string `json:"payload"`
}

type DoneReport struct {
	Status int `json:"status"`
}

type RejectReport struct {
	Status int    `json:"status"`
	Reason string `json:"reason"`
}

type FailReport struct {
	Status int    `json:"status"`
	Reason string `json:"reason"`
}

type Report struct {
	UID    string
	Done   *DoneReport   `json:"done,omitempty"`
	Reject *RejectReport `json:"reject,omitempty"`
	Fail   *FailReport   `json:"fail,omitempty"`
}

type ReadQueueReport struct {
	ReportChannel      *amqp.Channel
	Report             chan Report
	ReportExchangeName string
	ReportRoutingKey   string
}

type ReadQueue struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	queue           string
	done            chan error
	routines        int
	messagesChannel chan Message
	report          *ReadQueueReport
}

func NewReadQueue(
	conn *amqp.Connection,
	exchange string,
	routingKey string,
	routines int,
	queue string,
) (*ReadQueue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("[Rq][createChannel][%s]: %s", queue, err)
	}

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("[Rq][createChannel][%s]: %s", queue, err)
	}

	if err := channel.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("[Rq][queueBind][%s]: %s", queue, err)
	}

	return &ReadQueue{
		channel:         channel,
		conn:            conn,
		queue:           queue,
		done:            make(chan error),
		routines:        routines,
		messagesChannel: make(chan Message),
	}, nil
}

func (rq *ReadQueue) WithReport(reportExchangeName string, reportRoutingKey string) (*ReadQueue, error) {
	reportChannel, err := rq.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("[Rq][%s][createReportChannel]: %s", rq.queue, err)
	}

	report := make(chan Report)
	rq.report = &ReadQueueReport{
		ReportExchangeName: reportExchangeName,
		ReportChannel:      reportChannel,
		Report:             report,
		ReportRoutingKey:   reportRoutingKey,
	}
	return rq, nil
}

func (rq *ReadQueue) Run() error {
	defer close(rq.messagesChannel)

	deliveries, err := rq.channel.Consume(rq.queue, "", false, false, false, false, nil)

	if err != nil {
		return fmt.Errorf("[Rq][%s][Run]: %s", rq.queue, err)
	}

	go func() {
		for {
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					fmt.Printf("[Rq][%s][Run][channel closed]\n", rq.queue)
					return
				}
				var message *Message
				if err := json.NewDecoder(bytes.NewReader(delivery.Body)).Decode(&message); err != nil {
					fmt.Printf("[Rq][%s][Run][failed decode]: %s\n", rq.queue, err)
				}
				if err := delivery.Ack(false); err != nil {
					fmt.Printf("[Rq][%s][Run][failed ack]: %s", rq.queue, err)
					continue
				}
				fmt.Println(message)
				rq.messagesChannel <- *message
			}
		}
	}()

	if rq.report != nil {
		go func() {
			for report := range rq.report.Report {
				rq.report.ReportChannel.Publish(rq.report.ReportExchangeName,rq.report.ReportRoutingKey,false,false)
			}
		}()
	}
	return nil
}

func main() {
	connect, err := amqp.Dial("amqp://user:pass@localhost:5672")
	if err != nil {
		panic(err)
	}
	c, err := NewReadQueue(connect, "amq.topic", "kek", 2, "kek")
	if err != nil {
		panic(err)
	}
	// var wg sync.WaitGroup
	// wg.Add(1)
	// go func() {
	go c.Run()
	if err != nil {
		panic(err)
	}

	for kek := range c.messagesChannel {
		fmt.Println(kek)
	}
	// defer wg.Done()
	// }()
	//
	// wg.Wait()
}
