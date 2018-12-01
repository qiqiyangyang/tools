package main

import (
	"common"
	"conf"
	"flag"
	"fmt"
	"metric"
	"os"
	"os/signal"
	"pg"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	configPath  = flag.String("config", "./conf.json", "config for benchmark tools")
	verbose     = flag.Bool("verbose", false, "verbose output")
	forceCreate = flag.Bool("force", false, "force create table")
)

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
	flag.Parse()
	config, err := conf.NewConfig(*configPath)
	if err != nil {
		panic(err)
	}
	pgCon, err := pg.NewPgConnection(config.PostgresqlConfig.ServerConfig)
	if err != nil {
		panic(err)
	}
	defer pgCon.Close()
	if *verbose {
		log.SetLevel(log.DebugLevel)
	}
	if *forceCreate {
		if err = Init(config, pgCon); err != nil {
			panic(err)
		}
	}
	fmt.Println(config.PostgresqlConfig)
	if config.PostgresqlConfig.DeleteBatchSize == 0 && config.PostgresqlConfig.UpdateBatchSize == 0 && config.PostgresqlConfig.QueryBatchSize == 0 && config.PostgresqlConfig.InsertBatchSize == 0 {
		log.Errorln("can't disable all operation")
		return
	}

	operationCounter := &metric.OperationCounter{}
	tables := make([]*pg.Table, config.PostgresqlConfig.MaxConnections)
	wg := &sync.WaitGroup{}
	wg.Add(config.PostgresqlConfig.MaxConnections)
	for i := 0; i < config.PostgresqlConfig.MaxConnections; i++ {
		table, err := pg.NewTable(config.PostgresqlConfig, operationCounter, wg)
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
	defer log.Infof("received stop signal ")
	defer wg.Wait()
	for {
		select {
		case <-sig:
			for i := 0; i < config.PostgresqlConfig.MaxConnections; i++ {
				stop <- struct{}{}
			}
			return
		case <-ticker.C:
			seconds := float64(operationCounter.Duration) / 1000
			fmt.Printf(" %-2s Size:%-8s  QPS:%-8f  Insert[%d]:%-8d  Delete[%d]:%-8d  Select[%d]:%-8d  Update[%d]:%-8d\n", config.PostgresqlConfig.TargetTable, GetTableSize(config, pgCon), float64(operationCounter.Count)/seconds, config.PostgresqlConfig.InsertBatchSize, operationCounter.InsertCount, config.PostgresqlConfig.DeleteBatchSize, operationCounter.DeleteCount, config.PostgresqlConfig.QueryBatchSize, operationCounter.SelectCount, config.PostgresqlConfig.UpdateBatchSize, operationCounter.UpdateCount)
		}
	}
	defer pgCon.Close()
}
