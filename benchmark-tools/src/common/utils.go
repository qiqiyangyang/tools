package common

import (
	"log"
	"math/rand"

	uuid "github.com/satori/go.uuid"
)

const (
	OperationCount = 2
)

var (
	origin = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
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
	log.Println("nil string:", string(b))
	return string(b)
}
func GenInt() int64 {
	return rand.Int63()
}
