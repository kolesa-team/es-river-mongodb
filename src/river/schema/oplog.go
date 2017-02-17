package schema

import (
	"encoding/json"

	"labix.org/v2/mgo/bson"
)

type Oplog struct {
	Source       string              `bson:"-" json:"src"`
	Timestamp    bson.MongoTimestamp `bson:"ts" json:"ts"`
	HistoryID    int64               `bson:"h" json:"h"`
	MongoVersion int                 `bson:"v" json:"v"`
	Operation    string              `bson:"op" json:"op"`
	Namespace    string              `bson:"ns" json:"ns"`
	Object       bson.M              `bson:"o" json:"o"`
	QueryObject  bson.M              `bson:"o2" json:"o2"`
}

func MarshalQueryObject(obj map[string]interface{}) ([]byte, error) {
	delete(obj, "_id")

	return json.Marshal(obj)
}
