package piper

import (
	"context"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"sync"
	"time"
)

type Connection struct {
	dsn            string
	backoffPolicy  []time.Duration
	conn           *amqp.Connection
	serviceChannel *amqp.Channel
	mu             sync.RWMutex
	channelPool    map[ChannelPoolItemKey]*amqp.Channel
	consumers      map[ChannelPoolItemKey]*Consumer
	publishers     map[ChannelPoolItemKey]*Publisher
	channelPoolMu  sync.RWMutex
	consumersMu    sync.RWMutex
	publishersMu   sync.RWMutex
	isClosed       bool
}

func NewConnection(dsn string, backoffPolicy []time.Duration) (*Connection, error) {
	conn := &Connection{
		dsn:           dsn,
		consumers:     make(map[ChannelPoolItemKey]*Consumer),
		publishers:    make(map[ChannelPoolItemKey]*Publisher),
		backoffPolicy: backoffPolicy,
	}
	return conn, nil
}

func (c *Connection) NativeConn() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn
}

func (c *Connection) Channel() (*amqp.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn == nil {
		return nil, errors.New("connection is not defined")
	}
	channel, err := c.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "open a channel")
	}

	return channel, nil
}

func (c *Connection) Close(ctx context.Context) error {
	c.SetClosed(true)
	for _, consumer := range c.consumers {
		consumer.Stop(ctx)
		<-consumer.Done()
	}
	for _, publisher := range c.publishers {
		publisher.Stop(ctx)
		<-publisher.Done()
	}
	for _, ch := range c.channelPool {
		err := ch.Close()
		if err != nil {
			return errors.Wrap(err, "close rabbitMQ channel")
		}
	}

	if err := c.conn.Close(); err != nil {
		return errors.Wrap(err, "close rabbitMQ connection")
	}

	return nil
}

func (c *Connection) SetClosed(value bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isClosed = value
}

func (c *Connection) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
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

func (c *Connection) Connect(ctx context.Context) error {
	if !c.IsClosed() {
		if err := c.connect(); err != nil {
			return errors.Wrap(err, "connect")
		}
	}
	log.Printf("[AMQP CONNECT] starting connection watcher")
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = c.Close(ctx)
				log.Printf("[AMQP CONNECT] connection closed")
				return
			case _, ok := <-c.conn.NotifyClose(make(chan *amqp.Error)):
				if !ok {
					if c.IsClosed() {
						return
					}
					log.Printf("[AMQP CONNECT] connection unexpected closed")
					c.mu.Lock()
					var connErr error
					for _, timeout := range c.backoffPolicy {
						if connErr := c.connect(); connErr != nil {
							log.Printf("[AMQP CONNECT] connection failed, trying to reconnect to rabbitMQ")
							log.Println("connection failed, trying to reconnect to rabbitMQ", timeout)
							time.Sleep(timeout)
							continue
						}
						log.Printf("[AMQP CONNECT] reconnect to rabbitMQ %s", timeout)
						break
					}
					if connErr != nil {
						log.Printf("[AMQP CONNECT] error reconnect: %s", connErr)
						os.Exit(1)
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

func (c *Connection) addConsumerInPoll(poolKey ChannelPoolItemKey, consumer *Consumer) {
	c.consumersMu.Lock()
	defer c.consumersMu.Unlock()
	_, ok := c.consumers[poolKey]
	if !ok {
		c.consumers[poolKey] = consumer
	}
}

func (c *Connection) addPublisherInPoll(poolKey ChannelPoolItemKey, publisher *Publisher) {
	c.publishersMu.Lock()
	defer c.publishersMu.Unlock()
	_, ok := c.publishers[poolKey]
	if !ok {
		c.publishers[poolKey] = publisher
	}
}

func (c *Connection) AddConsumer(consumer *Consumer) (<-chan amqp.Delivery, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	poolKey := ChannelPoolItemKey{
		Name:       consumer.name,
		Type:       "consumer",
		Exchange:   consumer.config.Exchange,
		RoutingKey: consumer.config.RoutingKey,
		Queue:      consumer.config.Queue,
	}
	ch, err := c.GetChannelFromPool(poolKey)
	if err != nil {
		return nil, errors.Wrap(err, "get channel from pool")
	}
	if err := ch.Qos(consumer.config.Routines, 0, false); err != nil {
		return nil, err
	}
	c.addConsumerInPoll(poolKey, consumer)
	return ch.Consume(consumer.config.Queue, consumer.name, false, false, false, false, nil)
}

func (c *Connection) Publish(publisher *Publisher, msg amqp.Publishing) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	poolKey := ChannelPoolItemKey{
		Name:       publisher.name,
		Type:       "publisher",
		Exchange:   publisher.config.Exchange,
		RoutingKey: publisher.config.RoutingKey,
		Queue:      "",
	}
	ch, err := c.GetChannelFromPool(poolKey)
	if err != nil {
		return errors.Wrap(err, "get channel from pool")
	}
	c.addPublisherInPoll(poolKey, publisher)
	return ch.Publish(publisher.config.Exchange, publisher.config.RoutingKey, false, false, msg)
}

func (c *Connection) GetChannelFromPool(poolKey ChannelPoolItemKey) (*amqp.Channel, error) {
	c.channelPoolMu.Lock()
	defer c.channelPoolMu.Unlock()
	var err error

	ch, ok := c.channelPool[poolKey]
	if !ok {
		if c.conn == nil {
			return nil, errors.New("connection is not defined")
		}
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
		log.Printf("starting channel watcher on channel: %s \n", poolKey.Name)
		for {
			select {
			default:
				_, ok := <-ch.NotifyClose(make(chan *amqp.Error))
				if !ok {
					if c.isClosed {
						return
					}
					log.Println("rabbitMQ channel unexpected closed")
					c.channelPoolMu.Lock()
					delete(c.channelPool, poolKey)
					c.channelPoolMu.Unlock()

					c.consumersMu.Lock()
					delete(c.consumers, poolKey)
					c.consumersMu.Unlock()

					c.publishersMu.Lock()
					delete(c.publishers, poolKey)
					c.publishersMu.Unlock()
					return
				}
			}
		}
	}()
}
