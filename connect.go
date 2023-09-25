package piper

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"time"
)

func NewConnection(dsn string, backoffPolicy []time.Duration) (*Connection, error) {
	conn := &Connection{
		dsn:           dsn,
		backoffPolicy: backoffPolicy,
	}
	return conn, nil
}

func (c *Connection) OriginalConn() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn
}

func (c *Connection) Channel() (*amqp.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channel, err := c.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "open a channel")
	}

	return channel, nil
}

func (c *Connection) Close(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isClosed = true

	for _, ch := range c.channelPool {
		if err := ch.Close(); err != nil {
			return errors.Wrap(err, "close rabbitMQ channel")
		}
	}

	if err := c.conn.Close(); err != nil {
		return errors.Wrap(err, "close rabbitMQ connection")
	}

	return nil
}

func (c *Connection) IsClosed() bool {
	return c.isClosed
}

func (c *Connection) connect() error {
	var err error
	if c.conn, err = amqp.Dial(c.dsn); err != nil {
		return errors.Wrap(err, "connect to rabbitMQ")
	}

	if c.serviceChannel, err = c.conn.Channel(); err != nil {
		return errors.Wrap(err, "create service rabbitMQ channel")
	}

	c.channelPool = make(map[ChannelPoolItemKey]*amqp.Channel)

	return nil
}

func (c *Connection) Connect() error {
	if !c.isClosed {
		if err := c.connect(); err != nil {
			return errors.Wrap(err, "connect")
		}
	}

	go func() {
		fmt.Println("starting connection notify")
		for {
			select {
			default:
				_, ok := <-c.conn.NotifyClose(make(chan *amqp.Error))
				if !ok {
					if c.isClosed {
						return
					}
					fmt.Println("rabbitMQ connection unexpected closed")

					c.mu.Lock()
					var connErr error
					for _, timeout := range c.backoffPolicy {
						if connErr := c.connect(); connErr != nil {
							fmt.Println("connection failed, trying to reconnect to rabbitMQ")
							time.Sleep(timeout)
							continue
						}
						break
					}
					if connErr != nil {
						panic("connection failed")
					}
					c.mu.Unlock()
				}
			}
		}
	}()
	return nil
}

func (c *Connection) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.serviceChannel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *Connection) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.serviceChannel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *Connection) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.serviceChannel.QueueBind(name, key, exchange, noWait, args)
}

func (c *Connection) Qos(routines, prefetch int, global bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.serviceChannel.Qos(routines, prefetch, global)
}

func (c *Connection) Consume(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ch, err := c.GetChannelFromPool("", "", queue, consumer)
	if err != nil {
		return nil, errors.Wrap(err, "get channel from pool")
	}

	return ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *Connection) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ch, err := c.GetChannelFromPool(exchange, key, "", "")
	if err != nil {
		return errors.Wrap(err, "get channel from pool")
	}

	return ch.Publish(exchange, key, mandatory, immediate, msg)
}

func (c *Connection) GetChannelFromPool(exchange, key, queue, consumer string) (*amqp.Channel, error) {
	c.channelPoolMu.Lock()
	defer c.channelPoolMu.Unlock()
	var err error
	poolKey := ChannelPoolItemKey{
		Exchange: exchange,
		Key:      key,
		Queue:    queue,
		Consumer: consumer,
	}
	ch, ok := c.channelPool[poolKey]
	if !ok {
		ch, err = c.conn.Channel()
		if err != nil {
			return nil, errors.Wrap(err, "create channel")
		}
		c.channelPool[poolKey] = ch
		c.channelNotifyHandler(poolKey)
	}
	return ch, nil
}

func (c *Connection) channelNotifyHandler(poolKey ChannelPoolItemKey) {
	ch := c.channelPool[poolKey]

	go func() {
		fmt.Println("starting channel watcher")

		for {
			select {
			default:
				_, ok := <-ch.NotifyClose(make(chan *amqp.Error))
				if !ok {
					if c.isClosed {
						return
					}
					fmt.Println("rabbitMQ channel unexpected closed")
					c.channelPoolMu.Lock()
					delete(c.channelPool, poolKey)
					c.channelPoolMu.Unlock()
					return
				}
			}
		}
	}()
}
