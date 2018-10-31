package cpu

import (
	_ "fmt"
	"github.com/shirou/gopsutil/cpu"
	"inst"
	"strings"
)

func NewCpuLoader(name string) *inst.Loader {
	cl := &inst.Loader{}
	cl.Name = name
	cl.Ms = instance()
	return cl

}
func instance() []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	infos, err := cpu.Info()
	physical_cpu := 0
	logical_cpu := 1
	var cpu_str string
	if err == nil {
		for _, v := range infos {
			//fmt.Println("k:", k)
			//fmt.Println("v:", v)

			if len(cpu_str) == 0 {
				cpu_str = cpu_str + "," + v.PhysicalID
				physical_cpu = 1
				ms = append(ms, inst.NewMetric("cpu.version", v.ModelName))
				ms = append(ms, inst.NewMetric("cpu.mhz", v.Mhz))
			} else {
				if !strings.Contains(cpu_str, v.PhysicalID) {

					physical_cpu++
				}
				cpu_str = cpu_str + "," + v.PhysicalID
				logical_cpu++

			}
		}
		ms = append(ms, inst.NewMetric("cpu.physical.cores", physical_cpu))
		ms = append(ms, inst.NewMetric("cpu.logical.cores", logical_cpu))

	}
	cpus, err := cpu.Times(false)
	if err == nil {
		for _, v := range cpus {
			key := v.CPU
			ms = append(ms, inst.NewMetric(key+".user", v.User))
			ms = append(ms, inst.NewMetric(key+".system", v.System))
			ms = append(ms, inst.NewMetric(key+".idle", v.Idle))
			ms = append(ms, inst.NewMetric(key+".nice", v.Nice))
			ms = append(ms, inst.NewMetric(key+".iowait", v.Iowait))
			ms = append(ms, inst.NewMetric(key+".steal", v.Steal))
		}
	}
	return ms
}
