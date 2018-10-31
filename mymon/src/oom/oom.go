package oom

import (
	"bufio"
	"github.com/go-ini/ini"
	"inst"
	"os"
	"strings"
)

const (
	oom_log_key = "log"
	oom_key     = "kill"
)

func NewOomLoader(name string, sec *ini.Section) *inst.Loader {
	var ol *inst.Loader
	ms := instance(sec)
	if len(ms) > 0 {
		ol = &inst.Loader{}
		ol.Name = name
		ol.Ms = ms
	}
	return ol
}
func instance(sec *ini.Section) []*inst.Metric {
	ms := make([]*inst.Metric, 0)
	log_key, err := sec.GetKey(oom_log_key)
	if err == nil {
		oom_file := log_key.Value()
		file, err := os.Open(oom_file)
		if err == nil {
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				text := scanner.Text()
				if strings.Contains(text, oom_key) {
					ms = append(ms, inst.NewMetric("oom", text))
				}
			}
			if err := scanner.Err(); err != nil {
				ms = nil
			}
			file.Close()
		} else {
			ms = append(ms, inst.NewMetric("err", err))
		}
	} else {
		ms = append(ms, inst.NewMetric("err", err))

	}
	return ms
}
