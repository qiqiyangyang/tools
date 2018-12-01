package common

import (
	"math/rand"
	"time"

	uuid "github.com/satori/go.uuid"
)

var (
	origin = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

const (
	OperationCount = 4
	MaxBatchSize   = 128
)

func GenUUid() string {
	b := uuid.Must(uuid.NewV4())
	return b.String()
}
func GenVarch(n uint32) string {
	b := make([]rune, n)
	for i := 0; i < int(n); i++ {
		b[i] = origin[rand.Intn(len(origin))]
	}
	return string(b)
}
func GenInt32(b bool) int32 {
	var v int32
	if b {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		v = r.Int31()
	} else {
		v = rand.Int31()
	}
	return v
}
func GenInt16(b bool) int16 {
	var v int16
	if b {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		v = int16(r.Int31n(int32(32767) - 1))
	} else {
		v = int16(rand.Int31n(int32(32767) - 1))
	}
	return v

}
func GenInt64(b bool) int64 {
	var v int64
	if b {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		v = r.Int63()
	} else {
		v = rand.Int63()
	}
	return v
}
