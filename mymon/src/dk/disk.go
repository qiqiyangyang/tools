package dk

import (
	"github.com/shirou/gopsutil/disk"
	"inst"
	"strconv"
	"strings"
)

func NewDiskLoader(name string) *inst.Loader {
	dl := &inst.Loader{}
	dl.Name = name
	dl.Ms = instance()
	return dl
}
func instance() []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	ret, err := disk.Partitions(false)
	if err == nil {
		for _, v0 := range ret {
			u, err := disk.Usage(v0.Mountpoint)
			if err == nil {
				key := strings.Replace(v0.Device, "/dev/", "", -1)
				mount := "mount:" + v0.Mountpoint + ",size:"
				//key := v0.Mountpoint
				ms = append(ms, inst.NewMetric(key+".total", mount+strconv.FormatUint(u.Total/1024/1024, 10)+"mb"))
				ms = append(ms, inst.NewMetric(key+".free", mount+strconv.FormatUint(u.Free/1024/1024, 10)+"mb"))
				ms = append(ms, inst.NewMetric(key+".used", mount+strconv.FormatUint(u.Used/1024/1024, 10)+"mb"))
			}
		}
	}
	return ms
}
