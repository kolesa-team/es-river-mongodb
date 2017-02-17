package storage

import (
	"sync"
	"time"

	"../logger"

	log "github.com/Sirupsen/logrus"
	"github.com/endeveit/go-snippets/cli"
	"github.com/endeveit/go-snippets/config"
	"labix.org/v2/mgo"
)

type MongoDB struct {
	connectMutex sync.RWMutex
	once         sync.Once
	session      *mgo.Session
}

func NewMongoDB() *MongoDB {
	a := MongoDB{}

	return &a
}

func (m *MongoDB) GetSession() *mgo.Session {
	m.init()

	return m.session
}

func (m *MongoDB) init() {
	m.once.Do(func() {
		var (
			err      error
			mongoUrl string
		)

		if mongoUrl, err = config.Instance().String("mongodb", "address"); err != nil {
			logger.Instance().WithFields(log.Fields{
				"error": err,
			}).Error("Error occurred while fetching MongoDB url")
			cli.CheckError(err)
		}

		m.connectMutex.Lock()
		defer m.connectMutex.Unlock()

		if m.session, err = mgo.DialWithInfo(&mgo.DialInfo{
			Addrs:   []string{mongoUrl},
			Timeout: 15 * time.Minute,
		}); err != nil {
			logger.Instance().WithFields(log.Fields{
				"error": err,
			}).Error("Error occurred while connecting to MongoDB")
			cli.CheckError(err)
		}
	})
}
