package ht

import (
	"github.com/shirou/gopsutil/host"
	"inst"
	"net"
	"strconv"
)

func NewHostLoader(name string) *inst.Loader {
	hl := &inst.Loader{}
	hl.Name = name
	hl.Ms = instance()
	return hl
}

func instance() []*inst.Metric {
	var ips string
	ms := make([]*inst.Metric, 0)
	info, err0 := host.Info()
	//version, err0 := host.KernelVersion()
	//plate,famliy,ver,err4 := host.PlatformInformation()
	uptime, err2 := host.Uptime()
	addrs, err3 := net.InterfaceAddrs()
	if err3 == nil {
		for _, address := range addrs {
			// check the address type and if it is not a loopback the display it
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					ips = ips + ipnet.IP.String() + ","
				}
			}
		}
	}
	if err0 == nil && err2 == nil {
		ms = append(ms, inst.NewMetric("host.name", info.Hostname))
		ms = append(ms, inst.NewMetric("os.info", info.OS+","+info.KernelVersion))
		if len(ips) > 0 {
			ips = ips[:len(ips)-1]
			ms = append(ms, inst.NewMetric("host.ip", ips))
		}
		ms = append(ms, inst.NewMetric("uptime", strconv.FormatUint(uptime, 10)))
	}
	return ms
}
