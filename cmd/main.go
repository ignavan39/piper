package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

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
	channel      *amqp.Channel
	Report       chan Report
	exchangeName string
	routingKey   string
}

type ReadQueue struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	queue           string
	done            chan error
	routines        int
	messagesChannel chan Message
	Report          *ReadQueueReport
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

	err = reportChannel.ExchangeDeclare(reportExchangeName, "topic", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("[Rq][%s][createReportChannel]: %s", rq.queue, err)
	}
	report := make(chan Report)
	rq.Report = &ReadQueueReport{
		exchangeName: reportExchangeName,
		channel:      reportChannel,
		Report:       report,
		routingKey:   reportRoutingKey,
	}
	return rq, nil
}

func (rq *ReadQueue) Run() error {
	defer close(rq.messagesChannel)
	var wg sync.WaitGroup
	deliveries, err := rq.channel.Consume(rq.queue, "", false, false, false, false, nil)

	if err != nil {
		return fmt.Errorf("[Rq][%s][Run]: %s", rq.queue, err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
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
				rq.messagesChannel <- *message
			}
		}
	}()

	if rq.Report != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for report := range rq.Report.Report {
				buffer, err := json.Marshal(report)
				if err != nil {
					fmt.Printf("[Rq][%s][Run][report] - failed marshal: %s", rq.queue, err)
					continue
				}

				rq.Report.channel.Publish(rq.Report.exchangeName, rq.Report.routingKey, false, false, amqp.Publishing{
					ContentType: "application/json",
					Body:        buffer,
				})
			}
		}()
	}
	wg.Wait()
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

	c.WithReport("kek.report", "kek.report")
	go c.Run()
	if err != nil {
		panic(err)
	}
	for kek := range c.messagesChannel {
		fmt.Println(kek)
		c.Report.Report <- Report{
			Done: &DoneReport{
				Status: 1,
			},
			UID: kek.UID,
		}
	}
	// defer wg.Done()
	// }()
	//
	// wg.Wait()
}
