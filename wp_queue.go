package piper

import (
	"context"
	"fmt"
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
	wg.Add(1)
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
		case <-ctx.Done():
			return
		case delivery, ok := <-wp.deliveries:
			if !ok {
				fmt.Printf("[Rq][%s][%d][Run][channel closed]\n", wp.queue, numWorker)
				return
			}
			wp.results <- ResultDelivery{
				WorkerId: numWorker,
				Delivery: delivery,
			}
		}
	}
}
