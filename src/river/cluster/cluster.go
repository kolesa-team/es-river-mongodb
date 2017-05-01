package cluster

import (
	"crypto/sha1"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"../logger"
	"../worker"
)

type Cluster struct {
	identity   string
	worker     *worker.Worker
	once       sync.Once
	memberType string
	mutex      sync.RWMutex
}

func NewCluster(w *worker.Worker) *Cluster {
	c := Cluster{
		worker:   w,
		identity: Identity(),
	}

	return &c
}

func (c *Cluster) Start() {
	c.worker.Do()

	ch := c.Loop()

	for {
		select {
		case b := <-ch:
			if b {
				c.worker.Start()
			} else {
				c.worker.Stop()
			}
		}
	}
}

func (c *Cluster) Loop() chan bool {
	ch := make(chan bool)

	// Sleep for random interval 100..300ms
	time.Sleep(time.Duration(100+rand.Int63n(200)) * time.Millisecond)

	go func(channel chan bool, t *time.Ticker) {
		for range t.C {
			master := c.worker.GetMasterInfo()

			// Master info ttl is 10s, if it's outdated we're new master
			if time.Since(master.Since) > 10*time.Second {
				logger.Instance().Info("I am new master!")

				master.Id = c.identity
			}

			// If we're master now write info to ES
			if master.Id == c.identity {
				master.Since = time.Now()

				c.worker.SetMasterInfo(master)
			}

			channel <- master.Id == c.identity
		}
	}(ch, time.NewTicker(1*time.Second))

	return ch
}

func Identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	fmt.Fprint(h, rand.Int())

	return fmt.Sprintf("%x", h.Sum(nil))
}
