package piper

import (
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type ConnectionOptions struct {
	Vhost    string
	Host     string
	Password string
	UserName string
	Port     int
}

type Connection struct {
	retryPeriod time.Duration
	maxWait     time.Duration
	options     ConnectionOptions
	amqpConn    *amqp.Connection
}

func NewConnection(options ConnectionOptions, retryPeriod time.Duration, maxWait time.Duration) (*Connection, error) {
	amqpConn, err := establishAmqpConnection(options, retryPeriod, maxWait)
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		retryPeriod: retryPeriod,
		amqpConn:    amqpConn,
		maxWait:     maxWait,
		options:     options,
	}

	go run(conn)

	return conn, nil
}

func run(c *Connection) {
	for {
		reason, ok := <-c.amqpConn.NotifyClose(make(chan *amqp.Error))
		if !ok {
			fmt.Errorf("[AMQP] connection closed")
		} else {
			fmt.Errorf("[AMQP] connection closed: %v", reason)
		}
		conn, err := establishAmqpConnection(c.options, c.retryPeriod, c.maxWait)
		if err != nil {
			return
		}
		//TODO make graceful
		c.amqpConn = conn
		go run(c)
		return
	}
}

func establishAmqpConnection(options ConnectionOptions, retryPeriod time.Duration, maxWait time.Duration) (*amqp.Connection, error) {
	failed := time.Now().Add(maxWait)
	var vhost string

	if options.Vhost != "/" {
		vhost = options.Vhost
	} else {
		vhost = ""
	}
	connectionStr := fmt.Sprintf("amqp://%s:%s@%s:%d%s",
		options.UserName,
		options.Password,
		options.Host,
		options.Port,
		vhost,
	)
	for time.Now().Before(failed) {

		connection, err := amqp.Dial(connectionStr)
		if err != nil {
			fmt.Errorf("[AMQP] connection attempt failed with %s: %s", connectionStr, err)
		} else {
			fmt.Println("[AMQP] connection established")
			return connection, nil
		}
		time.Sleep(retryPeriod)
	}
	return nil, errors.New(fmt.Sprintf("[AMQP] can't establish connection with given options: %s", connectionStr))

}
