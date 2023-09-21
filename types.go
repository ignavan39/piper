package piper

import "github.com/streadway/amqp"

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

type WorkerPool struct {
	workersCount int
	deliveries   <-chan amqp.Delivery
	results      chan ResultDelivery
}

type ResultDelivery struct {
	Success  bool
	WorkerId int
	Delivery amqp.Delivery
}
