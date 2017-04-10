package cluster

import (
	"strings"
	"sync"
	"time"

	"../worker"

	"github.com/endeveit/go-snippets/cli"
	"github.com/endeveit/go-snippets/config"
	"io"
	"net/http"
	"strconv"
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
	TYPE_LEADER    = "leader"
	TYPE_CANDIDATE = "candidate"
	TYPE_FOLLOWER  = "follower"
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
	vote := r.FormValue("vote")
	term, err := strconv.ParseUint(r.FormValue("term"), 10, 64)
	if err != nil {
		term = uint64(1)
	}

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
	// reset election timeout
}

func (c *Cluster) handleAppendNode(w http.ResponseWriter, r *http.Request) {
	leader := r.FormValue("leader")
	term, err := strconv.ParseUint(r.FormValue("term"), 10, 64)
	if err != nil {
		term = uint64(1)
	}

	if term < c.currentVote.term {

	}

	if term > c.currentVote.term {
		c.currentVote.term = term
		c.currentVote.vote = ""
	}

	if c.memberType == TYPE_CANDIDATE && c.leader != leader && term >= c.currentVote.term {
		c.currentVote.term = term
		c.currentVote.vote = ""
	}

	// reset election timeout

}