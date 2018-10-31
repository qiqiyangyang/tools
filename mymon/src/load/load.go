package load

import (
	"github.com/shirou/gopsutil/load"
	"inst"
)

func NewWorkLoader(name string) *inst.Loader {
	wl := &inst.Loader{}
	wl.Name = name
	wl.Ms = instance()
	return wl
}
func instance() []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	avg, err1 := load.Avg()
	mis, err2 := load.Misc()
	if err1 == nil && err2 == nil {

		ms = append(ms, inst.NewMetric("load.1", avg.Load1))
		ms = append(ms, inst.NewMetric("load.5", avg.Load5))
		ms = append(ms, inst.NewMetric("load.15", avg.Load15))
		ms = append(ms, inst.NewMetric("procs.running", mis.ProcsRunning))
		ms = append(ms, inst.NewMetric("procs.blocked", mis.ProcsBlocked))
	}
	return ms
}
