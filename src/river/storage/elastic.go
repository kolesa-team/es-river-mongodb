package storage

import (
	"strings"

	"../logger"
	"../schema"

	log "github.com/Sirupsen/logrus"
	"github.com/endeveit/go-snippets/cli"
	"github.com/endeveit/go-snippets/config"
	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
)

const (
	OP_INSERT string = "i"
	OP_UPDATE string = "u"
	OP_DELETE string = "d"
	OP_NOOP   string = "n"
)

type Elastic struct {
	client                *elastic.Client
	indexName, recordType string
}

func NewElastic() *Elastic {
	var err error

	e := Elastic{}

	elasticUrl, err := config.Instance().String("elastic", "url")
	cli.CheckFatalError(err)

	e.indexName, err = config.Instance().String("elastic", "index")
	cli.CheckFatalError(err)

	e.recordType, err = config.Instance().String("elastic", "type")
	cli.CheckFatalError(err)

	logger.Instance().WithFields(log.Fields{
		"url":   elasticUrl,
		"index": e.indexName,
		"type":  e.recordType,
	}).Debug("Config")

	e.client, err = elastic.NewClient(elastic.SetURL(strings.Split(elasticUrl, ";")...))
	cli.CheckFatalError(err)

	_, err = e.client.IndexExists(e.indexName).Do(context.Background())
	cli.CheckFatalError(err)

	return &e
}

// Insert operation handler
func (e *Elastic) Insert(data map[string]interface{}) error {
	id := data["_id"].(string)

	body, err := schema.MarshalQueryObject(data)
	if err != nil {
		return err
	}

	obj, err := e.client.
		Index().
		Index(e.indexName).
		Type(e.recordType).
		Id(id).
		BodyString(string(body)).
		Do(context.Background())
	if err != nil {
		return err
	}

	logger.Instance().WithFields(log.Fields{
		"id":    obj.Id,
		"index": obj.Index,
		"type":  obj.Type,
	}).Debug("Indexed advert")

	return nil
}

// Update operation handler
func (e *Elastic) Update(id string, data map[string]interface{}) error {
	body, err := schema.MarshalQueryObject(data)
	if err != nil {
		return err
	}

	obj, err := e.client.
		Index().
		Index(e.indexName).
		Type(e.recordType).
		Id(id).
		BodyString(string(body)).
		Do(context.Background())
	if err != nil {
		return err
	}

	logger.Instance().WithFields(log.Fields{
		"id":    obj.Id,
		"index": obj.Index,
		"type":  obj.Type,
	}).Debug("Updated advert")

	return nil
}

// Delete operation handler
func (e *Elastic) Remove(id string) error {
	obj, err := e.client.
		Delete().
		Index(e.indexName).
		Type(e.recordType).
		Id(id).
		Do(context.Background())
	if err != nil {
		return err
	}

	logger.Instance().WithFields(log.Fields{
		"id":    obj.Id,
		"index": obj.Index,
		"type":  obj.Type,
	}).Debug("Deleted advert")

	return nil
}

// Return last operation timestamp
func (e *Elastic) GetLastTs() int64 {
	if val := e.getSetting("last_ts"); val != nil {
		return val.(int64)
	}

	return 0
}

// Sets last operation timestamp
func (e *Elastic) SetLastTs(lastTs int64) error {
	_, err := e.client.
		Update().
		Index(e.indexName).
		Type("river").
		Id("settings").
		Doc(map[string]interface{}{"last_ts": lastTs}).
		DocAsUpsert(true).
		Do(context.Background())

	if err != nil {
		logger.Instance().WithFields(log.Fields{
			"error": err,
		}).Debug("An error occurred while saving last ts")
	}

	return err
}

// Returns setting for key
func (e *Elastic) getSetting(key string) interface{} {
	obj, err := e.client.
		Get().
		Index(e.indexName).
		Type("river").
		Id("settings").
		Do(context.Background())

	if err != nil {
		if val, found := obj.Fields[key]; found {
			return val
		}
	}

	return nil
}
