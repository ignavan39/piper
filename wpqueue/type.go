package wpqueue

import "github.com/streadway/amqp"

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
