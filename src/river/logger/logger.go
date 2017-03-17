package logger

import (
	"log"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/endeveit/go-snippets/config"
	"github.com/gemnasium/logrus-graylog-hook"
)

var (
	once   sync.Once
	logger *logrus.Logger
)

type nullFormatter struct {
}

func initLogger() {
	once.Do(func() {
		// Logging setup
		logger = logrus.New()

		addr, err := config.Instance().String("graylog", "addr")
		if err == nil {
			logger.Hooks.Add(graylog.NewGraylogHook(addr, map[string]interface{}{}))
		}

		log.SetOutput(logger.Writer())
	})
}

func Instance() *logrus.Logger {
	initLogger()

	return logger
}

// Don't pass logs to stdout
func (nullFormatter) Format(e *logrus.Entry) ([]byte, error) {
	return []byte{}, nil
}
