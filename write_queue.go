package piper

import (
	"fmt"

	"github.com/streadway/amqp"
)

type WriteQueue struct {
	WriteChannel chan Message
	conn         *amqp.Connection
	channel      *amqp.Channel
	routines     int
	exchange     string
	routingKey   string
	done         chan error
}

func NewWriteQueue(
	conn *amqp.Connection,
	exchange string,
	routingKey string,
	routines int,
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
		routines:     routines,
		exchange:     exchange,
		routingKey:   routingKey,
	}
	return wq, nil
}
