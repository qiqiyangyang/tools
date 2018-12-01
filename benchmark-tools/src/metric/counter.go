package metric

import (
	"sync/atomic"
)

type OperationCounter struct {
	Count       int64
	DeleteCount int64
	InsertCount int64
	SelectCount int64
	UpdateCount int64
	Duration    int64
}

func NewtypeOperationCounter() *OperationCounter {
	return &OperationCounter{}
}
func (operationCounter *OperationCounter) AddDuration(v int64) {
	atomic.AddInt64(&operationCounter.Duration, v)
}
func (operationCounter *OperationCounter) AddCount(v int64) {
	atomic.AddInt64(&operationCounter.Count, v)
}
func (operationCounter *OperationCounter) AddDeleteCount(v int64) {
	atomic.AddInt64(&operationCounter.DeleteCount, v)
}
func (operationCounter *OperationCounter) AddInsertCount(v int64) {
	atomic.AddInt64(&operationCounter.InsertCount, v)
}
func (operationCounter *OperationCounter) AddSelectCount(v int64) {
	atomic.AddInt64(&operationCounter.SelectCount, v)
}

func (operationCounter *OperationCounter) AddUpdateCount(v int64) {
	atomic.AddInt64(&operationCounter.UpdateCount, v)
}
