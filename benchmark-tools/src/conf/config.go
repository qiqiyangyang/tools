package conf

import (
	"encoding/json"
	"io/ioutil"
	"time"
)

const (
	MinBatchSize = 1
	MaxBatchSize = 256
)

type Config struct {
	PostgresqlConfig *PgConfig
}
type PgServerConfig struct {
	Host     string
	Port     int
	User     string
	PassWord string
	DbName   string
}
type PgConfig struct {
	ServerConfig            *PgServerConfig
	TargetTable             string
	InsertBatchSize         int
	QueryBatchSize          int
	DeleteBatchSize         int
	UpdateBatchSize         int
	TargetTableFiledList    string
	MaxConnections          int
	TimeIntervalMilliSecond time.Duration
}

func NewConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}
	if config.PostgresqlConfig.InsertBatchSize > MaxBatchSize {
		config.PostgresqlConfig.InsertBatchSize = MaxBatchSize
	}
	if config.PostgresqlConfig.DeleteBatchSize > MaxBatchSize {
		config.PostgresqlConfig.DeleteBatchSize = MaxBatchSize
	}
	if config.PostgresqlConfig.UpdateBatchSize > MaxBatchSize {
		config.PostgresqlConfig.UpdateBatchSize = MaxBatchSize
	}
	if config.PostgresqlConfig.QueryBatchSize > MaxBatchSize {
		config.PostgresqlConfig.QueryBatchSize = MaxBatchSize
	}
	return config, err
}
