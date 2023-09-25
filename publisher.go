package piper

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

func NewPublisher(config *PublisherConfig, ch *Connection) (*Publisher, error) {
	publisher := &Publisher{
		config:         config,
		messages:       make(chan Message),
		reportMessages: make(chan Report),
		conn:           ch,
		name:           config.Exchange + "_" + config.RoutingKey,
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
		false,
		false,
		false,
		false,
		nil); err != nil {
		return fmt.Errorf("[Pq][%s-%s][connect][declare exchange]\n", p.config.Exchange, p.config.RoutingKey, err)
	}
	p.isConnected = true
	fmt.Println("publish is connected")
	return nil
}

func (p *Publisher) Publish() chan Message {
	if !p.isConnected {
		for {
			if err := p.Connect(); err != nil {
				fmt.Printf("[Pq][%s-%s][Reconnect][declare exchange]\n", p.config.Exchange, p.config.RoutingKey)
				time.Sleep(10 * time.Second)
				continue
			}
			break
		}
	}
	return p.messages
}

func (p *Publisher) Run() {
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case payload, ok := <-p.messages:
				if !ok {
					fmt.Printf("[Pq][%s-%s][Run][channel closed]\n", p.config.Exchange, p.config.RoutingKey)
					return
				}
				if !p.isConnected {
					continue
				}
				buffer, err := json.Marshal(payload)
				if err != nil {
					fmt.Printf("[Pq][%s-%s] - failed marshal: %s", p.config.Exchange, p.config.RoutingKey, err)
					continue
				}
				err = p.conn.Publish(
					p.config.Exchange,
					p.config.RoutingKey,
					false,
					false,
					amqp.Publishing{
						Body:        buffer,
						ContentType: "application/json",
					})
				if err != nil {
					p.muConn.Lock()
					p.isConnected = false
					p.muConn.Unlock()
					fmt.Printf("[Pq][%s-%s][Publish][Error]: %s", p.config.Exchange, p.config.RoutingKey, err)
				}
			}
		}
	}()

	wg.Wait()
}
