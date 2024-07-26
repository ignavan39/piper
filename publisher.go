package piper

import (
	"context"
	"encoding/json"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"time"
)

type Publisher struct {
	conn           *Connection
	config         PublisherConfig
	isConnected    bool
	isClose        bool
	name           string
	muConn         sync.RWMutex
	muClose        sync.RWMutex
	messages       chan any
	reportMessages chan Report
	done           chan bool
}

func NewPublisher(config PublisherConfig, ch *Connection) (*Publisher, error) {
	if ch == nil {
		return nil, errors.New("connection is not defined")
	}
	publisher := &Publisher{
		config:         config,
		messages:       make(chan any),
		reportMessages: make(chan Report),
		conn:           ch,
		name:           config.Exchange + "_" + config.RoutingKey,
		done:           make(chan bool),
	}

	return publisher, nil
}
func (p *Publisher) Connect() error {
	p.muConn.Lock()
	defer p.muConn.Unlock()
	if p.isConnected {
		return nil
	}
	if err := p.conn.ExchangeDeclare(
		p.config.Exchange,
		p.config.ExchangeKind,
		true,
		false,
		false,
		false,
		nil); err != nil {
		return err
	}
	p.isConnected = true
	return nil
}
func (p *Publisher) IsClose() bool {
	p.muClose.RLock()
	defer p.muClose.RUnlock()
	return p.isClose
}
func (p *Publisher) Publish(ctx context.Context) chan any {
	p.muConn.RLock()
	defer p.muConn.RUnlock()
	if !p.isConnected {
		for {
			if err := p.Connect(); err != nil {
				log.Printf("[ERROR] [AMQP PUBLISHER] error declare exchange(%s) %s", p.config.Exchange, p.config.RoutingKey)
				time.Sleep(10 * time.Second)
				continue
			}
			log.Printf("[AMQP PUBLISHER] is connected (%s) %s", p.config.Exchange, p.config.RoutingKey)
			break
		}
	}
	return p.messages
}
func (p *Publisher) Done() chan bool {
	return p.done
}
func (p *Publisher) Stop(ctx context.Context) error {
	log.Printf("[AMQP PUBLISHER] STOP (%s) %s", p.config.Exchange, p.config.RoutingKey)
	close(p.reportMessages)
	close(p.messages)
	p.muClose.Lock()
	p.isClose = true
	p.muClose.Unlock()
	return nil
}

func (p *Publisher) Start(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case payload, ok := <-p.messages:
				if !ok {
					if p.conn.IsClosed() {
						log.Printf("[AMQP PUBLISHER] channel is closed (%s) %s", p.config.Exchange, p.config.RoutingKey)
						p.muClose.Lock()
						p.isClose = true
						p.muClose.Unlock()
					}
					return
				}
				if !p.isConnected {
					continue
				}
				buffer, err := json.Marshal(payload)
				if err != nil {
					log.Printf("[ERROR] [AMQP PUBLISHER] (%s) %s failed marshal: (%s) ", p.config.Exchange, p.config.RoutingKey, err)
					continue
				}
				err = p.conn.Publish(p, amqp.Publishing{
					Body:        buffer,
					ContentType: "application/json",
				})
				if err != nil {
					p.muConn.Lock()
					p.isConnected = false
					p.muConn.Unlock()
					log.Printf("[ERROR] [AMQP PUBLISHER] error publish: (%s) %s: %s", p.config.Exchange, p.config.RoutingKey, err)
				}
			}
		}
	}()
	wg.Wait()
	p.Done() <- true
	log.Printf("[AMQP PUBLISHER] DONE (%s)", p.name)
}
