package piper

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
)

func NewWorkerPool(count int, queue string, deliveries <-chan amqp.Delivery) *QueueWorkerPool {
	return &QueueWorkerPool{
		workersCount: count,
		queue:        queue,
		deliveries:   deliveries,
		results:      make(chan ResultDelivery),
	}
}

func (wp QueueWorkerPool) RunWorkerPool(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(wp.workersCount)
	for i := 0; i < wp.workersCount; i++ {
		go wp.workerProcessing(ctx, i, &wg)
	}
	wg.Wait()
	close(wp.results)
}

func (wp QueueWorkerPool) Results() <-chan ResultDelivery {
	return wp.results
}

func (wp QueueWorkerPool) workerProcessing(ctx context.Context, numWorker int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case delivery, ok := <-wp.deliveries:
			if !ok {
				log.Printf("[AMQP WORKER POOL] workerProcessing is close exit (%s)", numWorker)
				return
			}
			wp.results <- ResultDelivery{
				WorkerId: numWorker,
				Delivery: &delivery,
			}
		}
	}
}
