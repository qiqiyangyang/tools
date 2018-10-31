package main

import (
	"cpu"
	"db"
	"dk"
	"fmt"
	"github.com/go-ini/ini"
	"ht"
	"inst"
	"load"
	"memory"
	"nt"
	"oom"
	"procs"
	"strings"
)

const (
	CfgName   = "./mymon.ini"
	sec_mysql = "mysql"
	sec_procs = "process"
	sec_oom   = "oom"
	end       = "end"
)

func format_print(name string, max int, ms []*inst.Metric) {
	line := "-----------------------------------------------"
	format := "%" + fmt.Sprintf("-%d", max) + "s[%s]%" + fmt.Sprintf("-%d", max) + "s\n"
	fmt.Printf(format, line, name, line)
	for _, v := range ms {
		if strings.Compare(v.Key, end) == 0 {
			fmt.Println("    ")
		} else {
			format := "%" + fmt.Sprintf("-%d", max*2) + "s|%v\n"
			//fmt.Println("format:", format)
			fmt.Printf(format, v.Key, v.Value)
		}
	}
}
func main() {
	loaders := make([]*inst.Loader, 0)
	cfg, err0 := ini.InsensitiveLoad(CfgName)
	if err0 != nil {
		fmt.Println("usage:")
		fmt.Println("        ./mymon")
		fmt.Println("  [error]:please check mymon.ini is exists")
		fmt.Println("--------write by perrynzhou(perrynzhou@gmail.com)--------")
		return
	}
	sec, err1 := cfg.GetSection(sec_procs)
	sec2, err2 := cfg.GetSection(sec_mysql)
	sec3, err3 := cfg.GetSection(sec_oom)
	loaders = append(loaders, ht.NewHostLoader("host"))
	loaders = append(loaders, cpu.NewCpuLoader("cpu"))
	loaders = append(loaders, memory.NewMemLoader("memory"))
	loaders = append(loaders, load.NewWorkLoader("workloader"))
	loaders = append(loaders, dk.NewDiskLoader("disk"))
	if err1 == nil {
		tmp_ := procs.NewProcessLoader("process", sec)
		if tmp_ != nil {
			loaders = append(loaders, tmp_)
		}
	}
	if err3 == nil {
		oomloader := oom.NewOomLoader("oom", sec3)
		if oomloader != nil {
			loaders = append(loaders, oomloader)
		}
	}
	netloader := nt.NewNetLoader("network")
	if netloader != nil {
		loaders = append(loaders, netloader)
	}
	if err2 == nil {
		mysqlloader := db.NewMysqlLoader("mysql", sec2)
		if mysqlloader != nil {
			loaders = append(loaders, mysqlloader)
		}
	}
	max := 25
	for _, v := range loaders {
		format_print(v.Name, max, v.Ms)
	}
}
