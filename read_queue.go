package piper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

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
	MessagesChannel chan Message
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

	if _, err := channel.QueueDeclare(
		queue,
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("[Rq][createChannel][%s]: %s", queue, err)
	}

	if err := channel.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("[Rq][queueBind][%s]: %s", queue, err)
	}

	channel.Qos(routines, 0, false)

	return &ReadQueue{
		channel:         channel,
		conn:            conn,
		queue:           queue,
		done:            make(chan error),
		routines:        routines,
		MessagesChannel: make(chan Message),
	}, nil
}

func (rq *ReadQueue) WithReport(reportExchangeName string, reportRoutingKey string) (*ReadQueue, error) {
	reportChannel, err := rq.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("[Rq][%s][createReportChannel]: %s", rq.queue, err)
	}

	err = reportChannel.ExchangeDeclare(
		reportExchangeName,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
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
	defer close(rq.MessagesChannel)
	var wg sync.WaitGroup
	deliveries, err := rq.channel.Consume(
		rq.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("[Rq][%s][Run]: %s", rq.queue, err)
	}

	wg.Add(rq.routines)
	go func() {
		for i := 0; i < rq.routines; i++ {
			go func(idx int) {
				defer wg.Done()
				for {
					select {
					case delivery, ok := <-deliveries:
						if !ok {
							fmt.Printf("[Rq][%s][%d][Run][channel closed]\n", rq.queue, idx)
							return
						}
						var message *Message
						if err := json.NewDecoder(bytes.NewReader(delivery.Body)).Decode(&message); err != nil {
							fmt.Printf("[Rq][%s][%d][Run][failed decode]: %s\n", rq.queue, idx, err)
							if err := delivery.Ack(false); err != nil {
								fmt.Printf("[Rq][%s][%d][Run][failed ack]: %s", rq.queue, idx, err)
								continue
							}
							continue
						}
						if err := delivery.Ack(false); err != nil {
							fmt.Printf("[Rq][%s][%d][Run][failed ack]: %s", rq.queue, idx, err)
							continue
						}
						fmt.Println(idx)
						rq.MessagesChannel <- *message
					}
				}
			}(i)
		}
	}()

	if rq.Report != nil {
		wg.Add(1)
		go func() {
			defer close(rq.Report.Report)
			defer wg.Done()
			for report := range rq.Report.Report {
				buffer, err := json.Marshal(report)
				if err != nil {
					fmt.Printf("[Rq][%s][Run][report] - failed marshal: %s", rq.queue, err)
					continue
				}

				rq.Report.channel.Publish(
					rq.Report.exchangeName,
					rq.Report.routingKey,
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body:        buffer,
					})
			}
		}()
	}
	wg.Wait()
	return nil
}
