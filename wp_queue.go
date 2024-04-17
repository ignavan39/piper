package piper

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
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
	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
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
				return
			}
			wp.results <- ResultDelivery{
				WorkerId: numWorker,
				Delivery: delivery,
			}
		}
	}
}
