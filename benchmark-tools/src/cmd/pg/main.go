package main

import (
	"conf"
	"os"
	"os/signal"
	"pg"
	"syscall"
)

var configPath = "../../conf/test.json"

func main() {
	config, _ := conf.NewConfig(configPath)
	pgCon, _ := pg.NewPgConnection(config.PostgresqlConfig.ServerConfig)
	table, err := pg.NewTable(config.PostgresqlConfig)
	if err != nil {
		panic(err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	stop := make(chan struct{}, 1)
	go table.Run(stop)
	for {
		select {
		case <-sig:
			stop <- struct{}{}
			return
		}
	}
	defer pgCon.Close()
}
