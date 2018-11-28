package common

import (
	"math/rand"

	uuid "github.com/satori/go.uuid"
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
	return string(b)
}
func GenInt32() int32 {
	return rand.Int31()
}
func GenInt16() int16 {
	return int16(rand.Int31n(int32(32767) - 16))
}
func GenInt64() int64 {
	return rand.Int63()
}
