package memory

import (
	"github.com/shirou/gopsutil/mem"
	"inst"
	"strconv"
)

func NewMemLoader(name string) *inst.Loader {
	ml := &inst.Loader{}
	ml.Name = name
	ml.Ms = instance()
	return ml
}
func instance() []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	swap, err1 := mem.SwapMemory()
	mem0, err2 := mem.VirtualMemory()
	if err1 == nil && err2 == nil {
		ms = append(ms, inst.NewMetric("mem.total", strconv.FormatUint(mem0.Total/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("mem.used", strconv.FormatUint(mem0.Used/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("mem.available", strconv.FormatUint(mem0.Available/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("mem.used_pct", strconv.FormatFloat(mem0.UsedPercent, 'f', 1, 64)+"%"))
		ms = append(ms, inst.NewMetric("mem.free", strconv.FormatUint(mem0.Free/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("mem.buffers", strconv.FormatUint(mem0.Buffers/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("mem.cached", strconv.FormatUint(mem0.Cached/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("swap.total", strconv.FormatUint(swap.Total/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("swap.used", strconv.FormatUint(swap.Used/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("swap.free", strconv.FormatUint(swap.Free/1024/1024, 10)+"mb"))
		ms = append(ms, inst.NewMetric("swap.used_pct", strconv.FormatFloat(swap.UsedPercent, 'f', 1, 64)+"%"))
		ms = append(ms, inst.NewMetric("swap.sin", swap.Sin))
		ms = append(ms, inst.NewMetric("swap.sout", swap.Sout))
	}
	return ms
}
