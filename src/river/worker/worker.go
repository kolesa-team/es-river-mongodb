package worker

import (
	"fmt"
	"time"

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

	STATE_ACTIVE  = "active"
	STATE_STOPPED = "stopped"
)

type Worker struct {
	mongo                           *storage.MongoDB
	elastic                         storage.IElastic
	skipInitImport                  bool
	database, collection, namespace string
	state                           string
	isInitialImportActive           bool
	isOplogImportActive             bool
}

func NewWorker() *Worker {
	var err error

	w := Worker{}

	w.database, err = config.Instance().String("mongodb", "database")
	cli.CheckError(err)

	w.collection, err = config.Instance().String("mongodb", "collection")
	cli.CheckError(err)

	w.skipInitImport, err = config.Instance().Bool("river", "skip_initial_import")
	if err != nil {
		w.skipInitImport = true
	}

	w.mongo = storage.NewMongoDB()
	w.elastic = storage.NewElastic()
	w.namespace = fmt.Sprintf("%s.%s", w.database, w.collection)
	w.state = STATE_STOPPED

	return &w
}

func (w *Worker) Start() *Worker {
	if !w.skipInitImport {
		w.isInitialImportActive = true
	}

	w.isOplogImportActive = true
	w.state = STATE_ACTIVE

	return w
}

func (w *Worker) Stop() *Worker {
	w.isInitialImportActive = false
	w.isOplogImportActive = false
	w.state = STATE_STOPPED

	return w
}

func (w *Worker) Do() *Worker {
	if !w.skipInitImport {
		go w.initialImport()
	}

	go w.listenOplog()

	return w
}

func (w *Worker) State() string {
	return w.state
}

func (w *Worker) initialImport() {
	var record map[string]interface{}

	defer logger.Instance().Info("Initial import complete")

	for {
		if w.isInitialImportActive {
			iterator := w.mongo.
				GetSession().
				DB(w.database).
				C(w.collection).
				Find(nil).
				Iter()

			for w.isInitialImportActive && iterator.Next(&record) {
				logger.Instance().WithFields(log.Fields{
					"record": record,
				}).Debug("Got collection record")

				record["_id"] = w.objectIdString(record["_id"])

				if err := w.elastic.Insert(record); err != nil {
					logger.Instance().WithFields(log.Fields{
						"record": record,
						"error":  err,
					}).Debug("An error occurred while indexing MongoDB collection record")
				}
			}
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func (w *Worker) listenOplog() {
	var (
		oplog  schema.Oplog
		lastTs float64 = w.elastic.GetLastTs()
	)

	logger.Instance().WithFields(log.Fields{
		"since": lastTs,
	}).Info("Listening for MongoDB oplog.rs")

	for {
		if w.isOplogImportActive {
			iterator := w.mongo.
				GetSession().
				DB("local").
				C("oplog.rs").
				Find(bson.M{
				"fromMigrate": bson.M{"$exists": false},
				"ns":          w.namespace,
				"ts":          bson.M{"$gte": bson.MongoTimestamp(lastTs)},
			}).Tail(-1)

			for w.isOplogImportActive && iterator.Next(&oplog) {
				logger.Instance().WithFields(log.Fields{
					"record": oplog,
				}).Debug("Got oplog record")

				if err := w.processOplog(oplog); err != nil {
					logger.Instance().WithFields(log.Fields{
						"record": oplog,
						"error":  err,
					}).Debug("An error occurred while processing MongoDB oplog record")
				} else {
					w.elastic.SetLastTs(float64(oplog.Timestamp))
				}
			}
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func (w *Worker) GetMasterInfo() (info schema.MasterInfo) {
	if id := w.elastic.GetSetting("master_id"); id != nil {
		info.Id = id.(string)
	}

	if s := w.elastic.GetSetting("master_since"); s != nil {
		if since, err := time.Parse(time.RFC3339, s.(string)); err == nil {
			info.Since = since
		} else {
			info.Since = time.Time{}
		}
	}

	return
}

func (w *Worker) SetMasterInfo(info schema.MasterInfo) (err error) {
	w.elastic.SetSetting("master_id", info.Id)
	w.elastic.SetSetting("master_since", info.Since.Format(time.RFC3339))

	return nil
}

func (w *Worker) processOplog(oplog schema.Oplog) error {
	switch oplog.Operation {
	case OP_INSERT:
		oplog.Object["_id"] = w.objectIdString(oplog.Object["_id"])

		logger.Instance().WithFields(log.Fields{
			"id": oplog.Object["_id"],
		}).Debug("Received insert op")

		return w.elastic.Insert(oplog.Object)
	case OP_UPDATE:
		oplog.QueryObject["_id"] = w.objectIdString(oplog.QueryObject["_id"])

		logger.Instance().WithFields(log.Fields{
			"id": oplog.QueryObject["_id"].(string),
		}).Debug("Received update op")

		return w.elastic.Update(oplog.QueryObject["_id"].(string), oplog.Object)
	case OP_DELETE:
		oplog.QueryObject["_id"] = w.objectIdString(oplog.QueryObject["_id"])

		logger.Instance().WithFields(log.Fields{
			"id": oplog.Object["_id"].(string),
		}).Debug("Received delete op")

		return w.elastic.Remove(oplog.Object["_id"].(string))
	default:
		return nil
	}

	return fmt.Errorf("Unknown error occurred")
}

// Converts mongodb objectId to string
func (w *Worker) objectIdString(id interface{}) string {
	switch id.(type) {
	default:
		return id.(bson.ObjectId).Hex()
	case string:
		objectId := bson.ObjectIdHex(id.(string))

		return string(objectId.Hex())
	}
}
