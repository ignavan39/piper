package wpqueue

import (
	"fmt"
	"github.com/streadway/amqp"
	"sync"
)

func NewWorkerPool(count int, queue string, deliveries <-chan amqp.Delivery) QueueWorkerPool {
	return QueueWorkerPool{
		workersCount: count,
		queue:        queue,
		deliveries:   deliveries,
		results:      make(chan ResultDelivery),
	}
}

func (wp QueueWorkerPool) RunWorkerPool() {
	var wg sync.WaitGroup
	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go wp.workerProcessing(i, &wg)
	}
	wg.Wait()
	close(wp.results)
}

func (wp QueueWorkerPool) Results() <-chan ResultDelivery {
	return wp.results
}

func (wp QueueWorkerPool) workerProcessing(numWorker int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
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
