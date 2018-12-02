package conf

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var configPath = "./test.json"

func TestNewConfig(t *testing.T) {
	config, err := NewConfig(configPath)
	assert.Nil(t, err)
	configJsonData, err := json.MarshalIndent(config, " ", " ")
	assert.Nil(t, err)
	fmt.Printf("Config:%s\n", configJsonData)
}
