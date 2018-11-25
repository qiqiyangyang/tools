package conf

import (
	"encoding/json"
	"io/ioutil"
	"time"
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
	MaxBatchSize            int
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
	return config, err
}
