package main

import (
	"common"
	"conf"
	"fmt"
	"os"
	"os/signal"
	"pg"
	"strings"
	"sync"
	"syscall"
	"time"
)

var configPath = "../../conf/test.json"

const (
	TableSizeFmt = "select pg_size_pretty(pg_relation_size('%s'))"
)

func Init(config *conf.Config, conn common.Connection) error {
	dropTableStmt := pg.PreDropTableStmtFmt
	if _, err := conn.Exec(strings.Replace(dropTableStmt, "?", config.PostgresqlConfig.TargetTable, -1)); err != nil {
		return err
	}
	//conn.Close()
	return nil
}
func GetTableSize(config *conf.Config, conn common.Connection) (info string) {
	rows, err := conn.Query(fmt.Sprintf(TableSizeFmt, config.PostgresqlConfig.TargetTable))
	if err == nil {
		for rows.Next() {
			err = rows.Scan(&info)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	return info
}
func main() {
	config, err := conf.NewConfig(configPath)
	if err != nil {
		panic(err)
	}
	pgCon, err := pg.NewPgConnection(config.PostgresqlConfig.ServerConfig)
	if err != nil {
		panic(err)
	}
	defer pgCon.Close()

	if err = Init(config, pgCon); err != nil {
		panic(err)
	}

	var tps uint64
	var duration uint64
	tables := make([]*pg.Table, config.PostgresqlConfig.MaxConnections)
	wg := &sync.WaitGroup{}
	wg.Add(config.PostgresqlConfig.MaxConnections)
	for i := 0; i < config.PostgresqlConfig.MaxConnections; i++ {
		table, err := pg.NewTable(config.PostgresqlConfig, &duration, &tps, wg)
		if err != nil {
			panic(err)
		}
		tables[i] = table
	}
	stop := make(chan struct{}, config.PostgresqlConfig.MaxConnections)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	for i := 0; i < config.PostgresqlConfig.MaxConnections; i++ {
		go tables[i].Run(stop)
	}
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	defer fmt.Println("..exit pg benchamrk...")
	defer wg.Wait()
	for {
		select {
		case <-sig:
			for i := 0; i < config.PostgresqlConfig.MaxConnections; i++ {
				stop <- struct{}{}
			}
			return
		case <-ticker.C:
			seconds := float64(duration) / 1000
			fmt.Printf("%s Size:%s,current QPS : %f\n", config.PostgresqlConfig.TargetTable, GetTableSize(config, pgCon), float64(tps)/seconds)
		}
	}
	defer pgCon.Close()
}
