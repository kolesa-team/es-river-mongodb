package main

import (
	"os"
	"syscall"
	"time"

	"./river/logger"
	w "./river/worker"

	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/endeveit/go-snippets/config"
	gd "github.com/sevlyar/go-daemon"
)

var (
	stop = make(chan struct{})
	done = make(chan struct{})
)

func main() {
	app := cli.NewApp()

	app.Name = "es-river-mongodb"
	app.Usage = "Indexes MongoDB to Elasticsearch"
	app.Version = "0.0.5"
	app.Author = "Igor Borodikhin"
	app.Email = "iborodikhin@gmail.com"
	app.Action = actionRun
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "daemon, d",
			Usage: "If provided, the service will be launched as daemon",
		},
		cli.BoolFlag{
			Name:  "debug, b",
			Usage: "If provided, the service will be launched in debug mode",
		},
		cli.StringFlag{
			Name:  "config, c",
			Value: "/etc/es-river-mongodb/config.cfg",
			Usage: "Path to the configuration file",
		},
		cli.StringFlag{
			Name:  "pid, p",
			Value: "/var/run/es-river-mongodb.pid",
			Usage: "Path to the file where PID will be stored",
		},
	}

	app.Run(os.Args)
}

// Главный обработчик, в котором происходит запуск демона
func actionRun(c *cli.Context) {
	isDaemon := c.Bool("daemon")
	pidfile := c.String("pid")
	config.Instance(c.String("config"))

	if c.Bool("debug") {
		logger.Instance().Level = log.DebugLevel
	}

	if !isDaemon {
		runDaemon(pidfile)
	} else {
		gd.SetSigHandler(termHandler, syscall.SIGTERM)

		dmn := &gd.Context{
			PidFileName: pidfile,
			PidFilePerm: 0644,
			WorkDir:     "/",
			Umask:       027,
		}

		child, err := dmn.Reborn()
		if err != nil {
			logger.Instance().WithFields(log.Fields{
				"error": err,
			}).Error("An error occured while trying to reborn daemon")
		}

		if child != nil {
			return
		}

		defer dmn.Release()

		go runDaemon(pidfile)
		go func() {
			for {
				time.Sleep(time.Second)
				if _, ok := <-stop; ok {
					logger.Instance().Info("Terminating daemon")
				}
			}
		}()

		err = gd.ServeSignals()
		if err != nil {
			logger.Instance().WithFields(log.Fields{
				"error": err,
			}).Error("An error occured while serving signals")
		}
	}
}

// Запуск сервера
func runDaemon(pidfile string) {
	logger.Instance().Info("Starting daemon")

	worker := w.NewWorker()

	skipInitImport, err := config.Instance().Bool("river", "skip_initial_import")
	if err != nil || skipInitImport == false {
		go worker.InitialImport()
	}

	worker.ListenOplog()

	done <- struct{}{}
}

// Обработчик SIGTERM
func termHandler(sig os.Signal) error {
	stop <- struct{}{}

	if sig == syscall.SIGTERM {
		<-done
	}

	return gd.ErrStop
}
