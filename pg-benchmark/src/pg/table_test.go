package pg

import (
	"conf"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	config, err := conf.NewConfig(configPath)
	assert.Nil(t, err)
	pgCon, err := NewPgConnection(&config.PgConfig)
	assert.NotNil(t, pgCon)
	assert.Nil(t, err)
	table, err := NewTable("u_order", pgCon)
	assert.NotNil(t, table)
	assert.Nil(t, err)
	assert.NotEqual(t, len(table.ColumnInfo), 0)
	pgCon.Close()
}
