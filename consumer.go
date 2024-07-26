package piper

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"time"
)

type Consumer struct {
	conn      *Connection
	config    ConsumerConfig
	name      string
	read      chan *amqp.Delivery
	done      chan bool
	waitClose chan bool
}

func NewConsumer(config ConsumerConfig, ch *Connection) (*Consumer, error) {
	if ch == nil {
		return nil, errors.New("connection is not defined")
	}
	c := &Consumer{
		config:    config,
		read:      make(chan *amqp.Delivery),
		name:      config.Exchange + "_" + config.RoutingKey + "_" + config.Queue,
		conn:      ch,
		done:      make(chan bool),
		waitClose: make(chan bool),
	}
	return c, nil
}
func (c *Consumer) Done() chan bool {
	return c.done
}

func (c *Consumer) WaitClose() chan bool {
	return c.waitClose
}

func (c *Consumer) run(ctx context.Context) {
	err := c.consume(ctx)
	if err != nil {
		log.Printf("[ERROR] [AMQP CONSUME][Consumer][%s] error run: %s", c.config.Queue, err)
	}
}

func (c *Consumer) Start(ctx context.Context, callback func(delivery *amqp.Delivery, index int) error) {
	go c.run(ctx)
	var wg sync.WaitGroup
	wg.Add(c.config.Routines)
	for i := 0; i < c.config.Routines; i++ {
		go func(index int, q *Consumer) {
			defer wg.Done()
			for {
				select {
				case delivery, ok := <-q.Read():
					if !ok {
						return
					}
					err := callback(delivery, index)
					if err != nil {
						log.Printf("[ERROR] [AMQP CONSUME] failed process receive message (%s)", err)
						if err := delivery.Reject(false); err != nil {
							log.Printf("[ERROR] [AMQP CONSUME] failed nack message (%s)", err)
						}
						continue
					}
					if err := delivery.Ack(false); err != nil {
						log.Printf("[ERROR] [AMQP CONSUME] failed to ack message (%s)", err)
						continue
					}
				}
			}
		}(i, c)
	}
	wg.Wait()
	log.Printf("[AMQP CONSUME] DONE (%s)", c.name)
	c.Done() <- true
	c.WaitClose() <- true
}

func (c *Consumer) Read() <-chan *amqp.Delivery {
	return c.read
}

func (c *Consumer) Connect() (<-chan amqp.Delivery, error) {
	deliveries, err := c.conn.AddConsumer(c)
	if err != nil {
		return nil, fmt.Errorf("[Rq][%s][run]: %s", c.config.Queue, err)
	}
	return deliveries, nil
}

func (c *Consumer) Stop(ctx context.Context) error {
	log.Printf("[AMQP CONSUME] STOP (%s)", c.config.Queue)
	close(c.read)
	return nil
}

func (c *Consumer) consume(ctx context.Context) error {
	var deliveries <-chan amqp.Delivery
	var wg sync.WaitGroup
	var err error
	for {
		if deliveries, err = c.Connect(); err != nil {
			log.Printf("[ERROR] [AMQP CONSUME] error connect (%s): %s", c.config.Queue, err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	log.Printf("[AMQP CONSUME] CONNECTED (%s)", c.config.Queue)

	wg.Add(1)
	pool := NewWorkerPool(c.config.Routines, c.config.Queue, deliveries)
	go pool.RunWorkerPool(ctx)

	go func() {
		defer wg.Done()
		for {
			select {
			case result, ok := <-pool.Results():
				if !ok {
					if c.conn.IsClosed() {
						log.Printf("[AMQP CONSUME] connection is close exit (%s)", c.config.Queue)
						return
					}
					log.Printf("[AMQP CONSUME] try to reconnect consumer (%s)", c.config.Queue)
					go c.consume(ctx)
					return
				}
				c.read <- result.Delivery
			}
		}
	}()
	wg.Wait()
	return err
}
