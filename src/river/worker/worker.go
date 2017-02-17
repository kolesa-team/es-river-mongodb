package worker

import (
	"fmt"

	"../logger"
	"../schema"
	"../storage"

	log "github.com/Sirupsen/logrus"
	"github.com/endeveit/go-snippets/cli"
	"github.com/endeveit/go-snippets/config"
	"labix.org/v2/mgo/bson"
)

const (
	OP_INSERT string = "i"
	OP_UPDATE string = "u"
	OP_DELETE string = "d"
	OP_NOOP   string = "n"
)

type Worker struct {
	mongo                           *storage.MongoDB
	elastic                         *storage.Elastic
	database, collection, namespace string
}

func NewWorker() *Worker {
	var err error

	w := Worker{}

	w.database, err = config.Instance().String("mongodb", "database")
	cli.CheckError(err)

	w.collection, err = config.Instance().String("mongodb", "collection")
	cli.CheckError(err)

	w.mongo = storage.NewMongoDB()
	w.elastic = storage.NewElastic()

	w.namespace = fmt.Sprintf("%s.%s", w.database, w.collection)

	return &w
}

func (w *Worker) InitialImport() {
	var record map[string]interface{}

	iterator := w.mongo.
		GetSession().
		DB(w.database).
		C(w.collection).
		Find(nil).
		Iter()

	for iterator.Next(&record) {
		logger.Instance().WithFields(log.Fields{
			"record": record,
		}).Debug("Got collection record")

		record["_id"] = record["_id"].(bson.ObjectId).String()

		if err := w.elastic.Insert(record); err != nil {
			logger.Instance().WithFields(log.Fields{
				"record": record,
				"error":  err,
			}).Debug("An error occurred while indexing MongoDB collection record")
		}
	}
}

func (w *Worker) ListenOplog() {
	var (
		oplog schema.Oplog
	)

	iterator := w.mongo.
		GetSession().
		DB("local").
		C("oplog.rs").
		Find(bson.M{"fromMigrate": bson.M{"$exists": false}}).
		Tail(-1)

	for iterator.Next(&oplog) {
		logger.Instance().WithFields(log.Fields{
			"record": oplog,
		}).Debug("Got oplog record")

		if err := w.processOplog(oplog); err != nil {
			logger.Instance().WithFields(log.Fields{
				"record": oplog,
				"error":  err,
			}).Debug("An error occurred while processing MongoDB oplog record")
		}
	}
}

func (w *Worker) processOplog(oplog schema.Oplog) error {
	if w.namespace != oplog.Namespace {
		logger.Instance().WithFields(log.Fields{
			"expected": w.namespace,
			"actual":   oplog.Namespace,
		}).Debug("Unknown namespace")

		return nil
	}

	switch oplog.Operation {
	case OP_INSERT:
		oplog.Object["_id"] = oplog.Object["_id"].(bson.ObjectId).String()

		logger.Instance().WithFields(log.Fields{
			"id": oplog.Object["_id"],
		}).Debug("Received insert op")

		return w.elastic.Insert(oplog.Object)
	case OP_UPDATE:
		oplog.QueryObject["_id"] = oplog.QueryObject["_id"].(bson.ObjectId).String()

		logger.Instance().WithFields(log.Fields{
			"id": oplog.QueryObject["_id"].(string),
		}).Debug("Received update op")

		return w.elastic.Update(oplog.QueryObject["_id"].(string), oplog.Object)
	case OP_DELETE:
		oplog.QueryObject["_id"] = oplog.QueryObject["_id"].(bson.ObjectId).String()

		logger.Instance().WithFields(log.Fields{
			"id": oplog.Object["_id"].(string),
		}).Debug("Received delete op")

		return w.elastic.Remove(oplog.Object["_id"].(string))
	default:
		return nil
	}

	return fmt.Errorf("Unknown error occurred")
}
