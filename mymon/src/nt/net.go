package nt

import (
	"github.com/shirou/gopsutil/net"
	"inst"
	"strconv"
)

func NewNetLoader(name string) *inst.Loader {
	var nl *inst.Loader
	ms := instance()
	if len(ms) > 0 {
		nl = &inst.Loader{}
		nl.Name = name
		nl.Ms = ms
	}
	return nl
}
func instance() []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	cs, err0 := net.IOCounters(false)
	ifs, err1 := net.Interfaces()
	if err0 == nil && err1 == nil {
		for _, v := range ifs {
			ms = append(ms, inst.NewMetric(v.Name+".mtu", v.MTU))
			ms = append(ms, inst.NewMetric(v.Name+".macaddr", v.HardwareAddr))

		}
		for _, v := range cs {
			ms = append(ms, inst.NewMetric(v.Name+".send_kb.size", strconv.FormatUint(v.BytesSent/1024, 10)+"kb"))
			ms = append(ms, inst.NewMetric(v.Name+".recv_kb.size", strconv.FormatUint(v.BytesRecv/1024, 10)+"kb"))
			ms = append(ms, inst.NewMetric(v.Name+".send_package.count", v.PacketsSent))
			ms = append(ms, inst.NewMetric(v.Name+".recv_package_count", v.PacketsRecv))
			ms = append(ms, inst.NewMetric(v.Name+".send_errors_count", v.Errin))
			ms = append(ms, inst.NewMetric(v.Name+".recv_errors_count", v.Errout))
		}
	}
	return ms
}
