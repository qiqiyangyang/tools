package common

const (
	OperationCount = 4
)
const (
	MaxBatchSize = 256
)

type OperationCounter struct {
	Count       uint64
	DeleteCount uint64
	InsertCount uint64
	SelectCount uint64
	UpdateCount uint64
	Duration    uint64
}
