package storage

import (
	"fmt"

	"github.com/endeveit/go-snippets/cli"
	"github.com/endeveit/go-snippets/config"
)

const (
	OP_INSERT string = "i"
	OP_UPDATE string = "u"
	OP_DELETE string = "d"
	OP_NOOP   string = "n"
)

type IElastic interface {
	Insert(data map[string]interface{}) error
	Update(id string, data map[string]interface{}) error
	Remove(id string) error
	GetLastTs() float64
	SetLastTs(lastTs float64) error
	GetSetting(key string) interface{}
	SetSetting(key string, value interface{}) error
}

type Elastic struct {
	IElastic
}

func NewElastic() IElastic {
	esVer, err := config.Instance().Int("elastic", "version")
	cli.CheckError(err)

	switch esVer {
	case 1:
		return NewElasticV1()
	case 2:
		return NewElasticV2()
	case 3:
		return NewElasticV5()
	default:
		panic(fmt.Sprintf("Unknown Elasticsearch version: %d", esVer))
	}
}
