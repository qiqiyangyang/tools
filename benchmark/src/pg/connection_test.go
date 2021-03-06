package pg

import (
	"conf"
	"encoding/json"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var configPath = "../conf/test.json"

func TestNewPgConnection(t *testing.T) {
	config, err := conf.NewConfig(configPath)
	assert.Nil(t, err)
	configJsonData, err := json.MarshalIndent(config, " ", " ")
	assert.Nil(t, err)
	fmt.Printf("Config:%s\n", configJsonData)
	pgCon, err := NewPgConnection(config.PostgresqlConfig.ServerConfig)
	assert.NotNil(t, pgCon)
	assert.Nil(t, err)
	pgCon.Close()
}
