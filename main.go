package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/datastore"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/parser"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/source"
)

// Job in beanstalkd
type Job struct {
	From           string
	To             string
	FailCounter    int `bson:"-"` // ignore in mongo
	SuccessCounter int `bson:"-"` // ignore in mongo
}

// ExchangeData is result for a job
// TODO add source to job (where to get data)
type ExchangeData struct {
	Job
	Rate      string // rate in string format with 2 decimal numbers
	CreatedAt int64  `bson:"created_at" json:"created_at"` // timestamp
}

const (
	// beanstalk server addr
	BEAN_ADDR = "challenge.aftership.net:11300"
	// tube name in beanstalk
	TUBE_NAME = "mmirolim"
	// dsn
	MONGO_HOST = "mongodb://admin:admin@ds027415.mongolab.com:27415/exchange-data"
	// db in mongo
	DB_NAME = "exchange-data"
	// collection name
	COLL_NAME = "rates"
)

var (
	numberOfJobs    = flag.Int("n", 10, "number of jobs to generate")
	numberOfWorkers = flag.Int("wn", 10, "number of workers in pool")

	// TODO make configurable
	limitOnFail       = 3
	limitOnSuccess    = 10
	delayOnGetRateErr = 3 * time.Second
	delayOnSuccess    = 60 * time.Second
)

func init() {
	// parse cmd line flags
	flag.Parse()
	flag.Usage()
}

func main() {
	// connect to mongo
	mongo, err := datastore.Connect(MONGO_HOST, DB_NAME, 2*time.Second)
	if err != nil {
		log.Fatal("mongo ", err)
	}

	// queue of computed exchange rates data
	out := make(chan ExchangeData, 1000)

	// connect to beanstalk
	queue, err := beanstalk.Dial("tcp", BEAN_ADDR)
	if err != nil {
		log.Fatal("err during beanstalk ", err)
	}

	// start pool of workers
	// for parallel job processing
	for i := 0; i < *numberOfWorkers; i++ {
		go worker(queue, TUBE_NAME, mongo, source.XE_COM, out)
	}

	// pull exchange data from chan to process further if needed
	for v := range out {
		log.Printf("exchange data %+v\n", v)

	}

}

// job consumer get job from queue, get exchange rate from defined source and output result
func worker(queue *beanstalk.Conn, tubeName string, db *datastore.DB, src parser.ExchangeSource, out chan ExchangeData) {

	jobTube := NewJobTube(queue, tubeName)
	// start inf loop to process jobs
	for {
		// get and reserve job
		id, job, err := jobTube.Reserve(10 * time.Millisecond)
		if err != nil {
			// TODO handle error
			// get another job
			continue
		}

		rate, err := parser.GetRate(src, job.From, job.To)
		if err != nil {
			log.Println("job id ", id, " GetRate err ", err)
			// and increment fail counter
			job.FailCounter++
			fmt.Println("fail counter incr", job)
			if job.FailCounter == limitOnFail {
				// bury job
				queue.Bury(id, 1)
				fmt.Println("bury job after 3 fails")
				// and go to beginning of the loop for new job
				continue
			}
			// put back job with a delay
			id, err = jobTube.PutBack(id, job, delayOnGetRateErr)
			if err != nil {
				log.Println("job id ", id, "failed put it back with delay ", delayOnGetRateErr)
			}

			continue
		}

		// on sucess incr success counter
		job.SuccessCounter++
		log.Println("succes counter incr", job)
		// save to mongo
		err = db.Save(job, COLL_NAME)
		if err != nil {
			// log error but continue working
			log.Println("mongo insert err ", err)
		}
		// condition on success counter
		if job.SuccessCounter == limitOnSuccess {
			// delete currency conversion job
			err = queue.Delete(id)
			fmt.Println("delete job after 10 success")
			if err != nil {
				log.Println("on delete ", err)
			}
			continue
		}
		// put job back with incr counter
		id, err = jobTube.PutBack(id, job, delayOnSuccess)
		if err != nil {
			log.Println("put back err ", err)
			continue
		}
		fmt.Println("put job back with success delay", job)
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
