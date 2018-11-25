package pg

import (
	"conf"
	"log"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	config, err := conf.NewConfig(configPath)
	assert.Nil(t, err)
	pgCon, err := NewPgConnection(config.PostgresqlConfig.ServerConfig)
	log.Println(err)
	assert.NotNil(t, pgCon)
	assert.Nil(t, err)
	table, err := NewTable(config.PostgresqlConfig)
	assert.NotNil(t, table)
	assert.Nil(t, err)
	pgCon.Close()
}
