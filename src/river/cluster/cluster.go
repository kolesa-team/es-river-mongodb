package cluster

import (
	"strings"
	"sync"
	"time"

	"../worker"

	"github.com/endeveit/go-snippets/cli"
	"github.com/endeveit/go-snippets/config"
	"net/http"
	"io"
)

type Cluster struct {
	identity           string
	leader             string
	worker             *worker.Worker
	once               sync.Once
	port               string
	neighbours         []string
	pingInterval       time.Duration
	pingTimeout        time.Duration
	lastSuccessfulPing time.Time
	pingFailsTimes     int
	memberType         string
	currentVote        Vote
	mutex              sync.RWMutex
}

type Vote struct {
	term uint64
	vote string
}

const (
	TYPE_LEADER = "leader"
	TYPE_CANDIDATE = "candidate"
	TYPE_FOLLOWER = "follower"
)

func NewCluster(w *worker.Worker) *Cluster {
	c := Cluster{
		worker: w,
	}

	return &c
}

func (c *Cluster) Start() {
	c.once.Do(func() {
		var (
			err        error
			neighbours string
		)

		c.port, err = config.Instance().String("cluster", "port")
		cli.CheckError(err)

		neighbours, err = config.Instance().String("cluster", "neighbours")
		cli.CheckError(err)

		c.neighbours = strings.Split(neighbours, ";")
	})

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

	return ch
}

func (c *Cluster) handleId(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, c.identity)
}

func (c *Cluster) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	term := uint64(r.FormValue("term"))
	vote := r.FormValue("vote")

	if term < c.currentVote.term {

	}

	stepDown := false
	if term > c.currentVote.term {
		c.currentVote.term = term
		c.currentVote.vote = ""
		c.leader = ""
		stepDown = true
	}

	if c.memberType == TYPE_LEADER && !stepDown {

	}

	if c.currentVote.vote != "" && c.currentVote.vote != vote {

	}

	c.currentVote.vote = vote
}