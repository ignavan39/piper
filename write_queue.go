package piper

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

type WriteQueue struct {
	WriteChannel chan Message
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchange     string
	routingKey   string
	done         chan error
}

func NewWriteQueue(
	conn *amqp.Connection,
	exchange string,
	routingKey string,
) (*WriteQueue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("[Wq][createChannel][%s]: %s", exchange, err)
	}

	err = channel.ExchangeDeclare(
		exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("[Rq][%s][createReportChannel]: %s", exchange, err)
	}

	wq := &WriteQueue{
		WriteChannel: make(chan Message),
		conn:         conn,
		channel:      channel,
		exchange:     exchange,
		routingKey:   routingKey,
	}
	return wq, nil
}

func (wq *WriteQueue) Run() {
	defer close(wq.WriteChannel)
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case payload, ok := <-wq.WriteChannel:
				fmt.Println(payload)
				if !ok {
					fmt.Printf("[Wq][%s-%s][Run][channel closed]\n", wq.exchange, wq.routingKey)
					return
				}
				buffer, err := json.Marshal(payload)
				if err != nil {
					fmt.Printf("[Wq][%s-%s][Run][report] - failed marshal: %s", wq.exchange, wq.routingKey, err)
					continue
				}
				wq.channel.Publish(wq.exchange, wq.routingKey, false, false, amqp.Publishing{
					Body:        buffer,
					ContentType: "application/json",
				})
			}
		}
	}()

	wg.Wait()
}