package pg

import (
	"conf"
	"testing"

	_ "github.com/lib/pq"
)

func TestNewTable(t *testing.T) {
	config, _ := conf.NewConfig(configPath)
	pgCon, _ := NewPgConnection(config.PostgresqlConfig.ServerConfig)
	table, _ := NewTable(config.PostgresqlConfig)
	qps := uint64(0)
	table.Qps = &qps
	stop := make(chan struct{})
	table.Run(stop)
	<-stop
	pgCon.Close()
}
