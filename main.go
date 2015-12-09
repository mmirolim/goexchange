package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/parser"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/source"
)

type Currency struct {
	Abbr string
	Name string
}

type Job struct {
	From           string
	To             string
	FailCounter    int
	SuccessCounter int
}

type ExchangeData struct {
	Job
	Rate      string // rate in string format with 2 decimal numbers
	CreatedAt int64  `bson:"created_at" json:"created_at"` // timestamp
}

const (
	BEAN_ADDR  = "challenge.aftership.net:11300"
	TUBE_NAME  = "mmirolim"
	MONGO_HOST = "ds027415.mongolab.com:27415"
	DB_NAME    = "exchange-data"
	COLL_NAME  = "rates"
)

var (
	numberOfJobs    = flag.Int("n", 10, "number of jobs to generate")
	numberOfWorkers = flag.Int("wn", 10, "number of workers in pool")
)

func init() {
	flag.Parse()
	flag.Usage()
}

func main() {
	// queue of computed exchange rates data
	out := make(chan ExchangeData, 1000)
	queue, err := beanstalk.Dial("tcp", BEAN_ADDR)
	if err != nil {
		log.Fatal("err during beanstalk ", err)
	}
	// start pool of workers
	for i := 0; i < *numberOfWorkers; i++ {
		go worker(queue, TUBE_NAME, out)
	}
	// pull exchange data from chan and store in mongo
	for v := range out {
		fmt.Printf("exchange data %+v\n", v)

	}

}

var (
	limitOnFail       = 3
	limitOnSuccess    = 10
	delayOnGetRateErr = 3 * time.Second
	delayOnSuccess    = 5 * time.Second
)

func worker(queue *beanstalk.Conn, tubeName string, out chan ExchangeData) {
	var (
		id   uint64
		job  Job
		rate float64
		err  error
	)

	jobTube := NewJobTube(queue, tubeName)
	// start inf loop to process jobs
	for {
		// get and reserve job
		id, job, err = jobTube.Reserve(time.Second)
		if err != nil {
			// TODO handle error
			log.Println("job tube reserve ", err)
			continue
		}

		rate, err = parser.GetRate(source.XE_COM, job.From, job.To)
		if err != nil {
			log.Println("job id ", id, " GetRate err ", err)
			// and increment fail counter
			job.FailCounter++
			if job.FailCounter > limitOnFail {
				// bury job
				queue.Bury(id, 1)
				// and go to beginning of the loop for new job
				continue
			}
			// put back job with a delay
			id, err = jobTube.PutBack(id, job, delayOnGetRateErr)
			log.Println("job id ", id, "failed put it back with delay ", delayOnGetRateErr)
			continue
		}

		// on sucess incr success counter
		job.SuccessCounter++
		// condition on success counter
		if job.SuccessCounter == limitOnSuccess {
			// delete currency conversion job
			err = queue.Delete(id)
			if err != nil {
				log.Println("on delete ", err)
			}
			continue
		}
		// put job back with incr counter
		id, err = jobTube.PutBack(id, job, delayOnSuccess)
		// send result to chan
		out <- ExchangeData{
			Job:       job,
			Rate:      strconv.FormatFloat(rate, 'f', 2, 64),
			CreatedAt: time.Now().UTC().Unix(),
		}

	}

}

type Tube struct {
	q              *beanstalk.Conn
	t              *beanstalk.Tube
	ts             *beanstalk.TubeSet
	delayOnSuccess time.Duration
	delayOnErr     time.Duration
}

func NewJobTube(conn *beanstalk.Conn, tube string) *Tube {
	return &Tube{
		q:  conn,
		t:  &beanstalk.Tube{conn, tube},
		ts: beanstalk.NewTubeSet(conn, tube),
	}
}

func (tb *Tube) Reserve(timeout time.Duration) (uint64, Job, error) {
	var job Job
	id, data, err := tb.ts.Reserve(timeout)
	if err != nil {
		return 0, job, err
	}
	err = json.Unmarshal(data, &job)
	if err != nil {
		return 0, job, err
	}

	return id, job, nil
}

func (tb *Tube) PutBack(id uint64, job Job, delay time.Duration) (uint64, error) {
	data, err := json.Marshal(job)
	if err != nil {
		return 0, err
	}

	// swap jobs by deleting previous and putting new one
	err = tb.q.Delete(id)
	if err != nil {
		return 0, err
	}

	return tb.t.Put(data, 1, delay, time.Minute)
}
