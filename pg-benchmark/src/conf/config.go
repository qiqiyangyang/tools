package conf

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	PgConfig          PgServerConfig
	CreateStatement   string
	MaxConnections    int
	UpdateColumnCount int
	SelectColumnCount int
	DeleteColumnCount int
}
type PgServerConfig struct {
	Host     string
	Port     int
	User     string
	PassWord string
	DbName   string
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
