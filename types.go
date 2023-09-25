package piper

import (
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type Message struct {
	UID     string `json:"uid"`
	Payload any    `json:"payload"`
}

type DoneReport struct {
	Status int `json:"status"`
}

type RejectReport struct {
	Status int    `json:"status"`
	Reason string `json:"reason"`
}

type FailReport struct {
	Status int    `json:"status"`
	Reason string `json:"reason"`
}

type Report struct {
	UID    string
	Done   *DoneReport   `json:"done,omitempty"`
	Reject *RejectReport `json:"reject,omitempty"`
	Fail   *FailReport   `json:"fail,omitempty"`
}

type QueueWorkerPool struct {
	workersCount int
	queue        string
	deliveries   <-chan amqp.Delivery
	results      chan ResultDelivery
}

type ResultDelivery struct {
	WorkerId int
	Delivery amqp.Delivery
}

type ChannelPoolItemKey struct {
	Queue    string
	Consumer string
	Exchange string
	Key      string
}

type Connection struct {
	dsn            string
	backoffPolicy  []time.Duration
	conn           *amqp.Connection
	serviceChannel *amqp.Channel
	mu             sync.RWMutex
	channelPool    map[ChannelPoolItemKey]*amqp.Channel
	channelPoolMu  sync.RWMutex
	isClosed       bool
}

type Consumer struct {
	conn   *Connection
	config *ConsumerConfig
	name   string
	read   chan Message
}

type ConsumerConfig struct {
	Exchange     string
	ExchangeKind string
	RoutingKey   string
	Routines     int
	Queue        string
}
type Publisher struct {
	conn           *Connection
	config         *PublisherConfig
	isConnected    bool
	name           string
	muConn         sync.Mutex
	messages       chan Message
	reportMessages chan Report
}
type PublisherConfig struct {
	Exchange     string
	ExchangeKind string
	RoutingKey   string
}
