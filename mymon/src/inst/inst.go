package inst

type Metric struct {
	Key   string
	Value interface{}
}

type Loader struct {
	Name string
	Ms   []*Metric
}

func NewMetric(key string, val interface{}) *Metric {
	return &Metric{key, val}
}
