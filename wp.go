package piper

import (
	"github.com/streadway/amqp"
	"sync"
)

func NewWorkerPool(count int, deliveries <-chan amqp.Delivery) WorkerPool {
	return WorkerPool{
		workersCount: count,
		deliveries:   deliveries,
		results:      make(chan ResultDelivery),
	}
}

func (wp WorkerPool) RunWorkerPool() {
	var wg sync.WaitGroup
	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		go workerProcessing(i, &wg, wp.deliveries, wp.results)
	}
	wg.Wait()
	close(wp.results)
}

func (wp WorkerPool) Results() <-chan ResultDelivery {
	return wp.results
}

func workerProcessing(numWorker int, wg *sync.WaitGroup, deliveries <-chan amqp.Delivery, results chan<- ResultDelivery) {
	defer wg.Done()
	for {
		select {
		case delivery, ok := <-deliveries:
			results <- ResultDelivery{
				Success:  ok,
				WorkerId: numWorker,
				Delivery: delivery,
			}
		}
	}
}
