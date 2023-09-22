package piper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
)

type ReadQueueReport struct {
	channel      *amqp.Channel
	report       chan Report
	exchangeName string
	routingKey   string
}

type ReadQueue struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    string
	done     chan error
	routines int
	read     chan Message
	report   *ReadQueueReport
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

	if err := channel.ExchangeDeclare(
		exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("[Rq][declareExchange][%s]: %s", queue, err)
	}

	if err := channel.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("[Rq][queueBind][%s]: %s", queue, err)
	}

	channel.Qos(routines, 0, false)

	return &ReadQueue{
		channel:  channel,
		conn:     conn,
		queue:    queue,
		done:     make(chan error),
		routines: routines,
		read:     make(chan Message),
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
	rq.report = &ReadQueueReport{
		exchangeName: reportExchangeName,
		channel:      reportChannel,
		report:       report,
		routingKey:   reportRoutingKey,
	}
	return rq, nil
}

func (rq *ReadQueue) Report() chan Report {
	return rq.report.report
}

func (rq *ReadQueue) Read() <-chan Message {
	return rq.read
}

func (rq *ReadQueue) Run() error {
	defer close(rq.read)
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
	wg.Add(1)
	pool := NewWorkerPool(rq.routines, rq.queue, deliveries)
	go pool.RunWorkerPool()

	go func() {
		defer wg.Done()
		for {
			select {
			case result, ok := <-pool.Results():
				if !ok {
					fmt.Printf("[Rq][%s][Run][queue result channel closed]\n", rq.queue)
					return
				}
				var message *Message
				if err := json.NewDecoder(bytes.NewReader(result.Delivery.Body)).Decode(&message); err != nil {
					fmt.Printf("[Rq][%s][%d][Run][failed decode]: %s\n", rq.queue, result.WorkerId, err)
					if err := result.Delivery.Ack(false); err != nil {
						fmt.Printf("[Rq][%s][%d][Run][failed ack]: %s", rq.queue, result.WorkerId, err)
						continue
					}
					continue
				}
				if err := result.Delivery.Ack(false); err != nil {
					fmt.Printf("[Rq][%s][%d][Run][failed ack]: %s", rq.queue, result.WorkerId, err)
					continue
				}
				fmt.Println(*message)
				rq.read <- *message
			}
		}
	}()

	if rq.report != nil {
		wg.Add(1)
		go func() {
			defer close(rq.report.report)
			defer wg.Done()
			for report := range rq.report.report {
				buffer, err := json.Marshal(report)
				if err != nil {
					fmt.Printf("[Rq][%s][Run][report] - failed marshal: %s", rq.queue, err)
					continue
				}

				rq.report.channel.Publish(
					rq.report.exchangeName,
					rq.report.routingKey,
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
