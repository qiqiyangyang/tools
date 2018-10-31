package procs

import (
	"errors"
	"github.com/go-ini/ini"
	"github.com/shirou/gopsutil/process"
	"inst"
	"os"
	"strconv"
	"strings"
)

const (
	on = "on"
)

func NewProcessLoader(name string, sec *ini.Section) *inst.Loader {
	var pl *inst.Loader
	keys := sec.Keys()
	pids := make([]int32, 0)
	for _, k := range keys {
		pid, err := strconv.Atoi(k.Name())
		if err == nil && strings.Compare(k.Value(), on) == 0 {
			pids = append(pids, int32(pid))
		}
	}
	pl = &inst.Loader{}
	pl.Name = name
	pl.Ms = instance(pids)
	return pl
}
func isrunning(pid int32) error {
	path := "/proc/" + strconv.FormatInt(int64(pid), 10)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return errors.New("process not running,check process pid")
	}
	return nil
}
func instance(pids []int32) []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	if len(pids) == 0 {
		err := errors.New("no process id configure,such as {pid} = on")
		ms = append(ms, inst.NewMetric("err", err))
		return ms
	}
	for _, pid := range pids {
		err0 := isrunning(pid)
		if err0 == nil {
			p, err := process.NewProcess(pid)
			if err == nil {
				mem, err1 := p.MemoryInfo()
				name, err2 := p.Name()
				fds, err3 := p.NumFDs()
				io, err4 := p.IOCounters()
				thds, err5 := p.NumThreads()
				if err1 == nil && err2 == nil && err3 == nil && err5 == nil && err4 == nil {
					ms = append(ms, inst.NewMetric("pid", pid))
					ms = append(ms, inst.NewMetric("name", name))
					ms = append(ms, inst.NewMetric("thread.size", thds))
					ms = append(ms, inst.NewMetric("fd.size", fds))
					ms = append(ms, inst.NewMetric("process.rss", strconv.FormatUint(mem.RSS/1024/1024, 10)+"mb"))
					ms = append(ms, inst.NewMetric("process.vms", strconv.FormatUint(mem.VMS/1024/1024, 10)+"mb"))
					ms = append(ms, inst.NewMetric("process.swap", strconv.FormatUint(mem.Swap/1024/1024, 10)+"mb"))
					ms = append(ms, inst.NewMetric("io.readcount", io.ReadCount))
					ms = append(ms, inst.NewMetric("io.writecount", io.WriteCount))
					ms = append(ms, inst.NewMetric("io.readsize", strconv.FormatUint(io.ReadBytes/1024/1024, 10)+"mb"))
					ms = append(ms, inst.NewMetric("io.writesize", strconv.FormatUint(io.WriteBytes/1024/1024, 10)+"mb"))
					ms = append(ms, inst.NewMetric("end", "end"))
				}
			}

		} else {
			ms = append(ms, inst.NewMetric("err", err0))
		}
	}
	return ms
}
