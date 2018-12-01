package metric

import (
	"sync/atomic"
)

type OperationCounter struct {
	Count       uint64
	DeleteCount uint64
	InsertCount uint64
	SelectCount uint64
	UpdateCount uint64
	Duration    uint64
}

func NewtypeOperationCounter() *OperationCounter {
	return &OperationCounter{}
}
func (operationCounter *OperationCounter) AddDuration(v uint64) {
	atomic.AddUint64(&operationCounter.Duration, v)
}
func (operationCounter *OperationCounter) AddDeleteCount(v uint64) {
	atomic.AddUint64(&operationCounter.DeleteCount, v)
	atomic.AddUint64(&operationCounter.Count, v)
}
func (operationCounter *OperationCounter) AddInsertCount(v uint64) {
	atomic.AddUint64(&operationCounter.InsertCount, v)
	atomic.AddUint64(&operationCounter.Count, v)
}
func (operationCounter *OperationCounter) AddSelectCount(v uint64) {
	atomic.AddUint64(&operationCounter.SelectCount, v)
	atomic.AddUint64(&operationCounter.Count, v)
}

func (operationCounter *OperationCounter) AddUpdateCount(v uint64) {
	atomic.AddUint64(&operationCounter.UpdateCount, v)
	atomic.AddUint64(&operationCounter.Count, v)
}
