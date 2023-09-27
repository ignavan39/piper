package piper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type Consumer struct {
	conn   *Connection
	config ConsumerConfig
	name   string
	read   chan Message
}

func NewConsumer(config ConsumerConfig, ch *Connection) (*Consumer, error) {
	if ch == nil {
		return nil, errors.New("connection is not defined")
	}
	c := &Consumer{
		config: config,
		read:   make(chan Message),
		name:   config.Exchange + "_" + config.RoutingKey + "_" + config.Queue,
		conn:   ch,
	}
	return c, nil
}

func (c *Consumer) Run() {
	go func() {
		err := c.consume()
		if err != nil {
			fmt.Println(fmt.Errorf("[Rq][%s][Consume]: %s", c.config.Queue, err))
		}
	}()
}

func (c *Consumer) Read() <-chan Message {
	return c.read
}

func (c *Consumer) Connect() (<-chan amqp.Delivery, error) {
	if err := c.conn.ExchangeDeclare(
		c.config.Exchange,
		c.config.ExchangeKind,
		false,
		false,
		false,
		false,
		nil); err != nil {
		return nil, fmt.Errorf("[Rq][declareExchange][%s-%s]: %s", c.config.Exchange, c.config.ExchangeKind, err)
	}

	if _, err := c.conn.QueueDeclare(
		c.config.Queue,
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("[Rq][createChannel][%s]: %s", c.config.Queue, err)
	}

	if err := c.conn.QueueBind(
		c.config.Queue,
		c.config.RoutingKey,
		c.config.Exchange,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("[Rq][queueBind][%s]: %s", c.config.Queue, err)
	}
	if err := c.conn.Qos(c.config.Routines, 0, false); err != nil {
		return nil, fmt.Errorf("[Rq][qos][%s]: %s", c.config.Queue, err)
	}
	deliveries, err := c.conn.Consume(
		c.config.Queue,
		c.name,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("[Rq][%s][Run]: %s", c.config.Queue, err)
	}

	return deliveries, nil
}
func (c *Consumer) consume() error {
	var deliveries <-chan amqp.Delivery
	var wg sync.WaitGroup
	var err error
	for {
		if deliveries, err = c.Connect(); err != nil {
			fmt.Printf("[Rq][%s][Error connect consumer]: %s", c.config.Queue, err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	fmt.Printf("[Rq][%s][consumer connected]\n", c.config.Queue)

	wg.Add(1)
	pool := NewWorkerPool(c.config.Routines, c.config.Queue, deliveries)
	go pool.RunWorkerPool()
	go func() {
		defer wg.Done()
		for {
			select {
			case result, ok := <-pool.Results():
				if !ok {
					if c.conn.IsClosed() {
						return
					}
					fmt.Printf("[Rq][%s][Run][try to reconnect consumer]\n", c.config.Queue)
					go c.consume()
					return
				}
				var message *Message
				if err := json.NewDecoder(bytes.NewReader(result.Delivery.Body)).Decode(&message); err != nil {
					fmt.Printf("[Rq][%s][%d][Run][failed decode]: %s\n", c.config.Queue, result.WorkerId, err)
					if err := result.Delivery.Ack(false); err != nil {
						fmt.Printf("[Rq][%s][%d][Run][failed ack]: %s", c.config.Queue, result.WorkerId, err)
						continue
					}
					continue
				}
				if err := result.Delivery.Ack(false); err != nil {
					fmt.Printf("[Rq][%s][%d][Run][failed ack]: %s", c.config.Queue, result.WorkerId, err)
					continue
				}
				fmt.Println(*message)
				c.read <- *message
			}
		}
	}()
	wg.Wait()
	return err
}
