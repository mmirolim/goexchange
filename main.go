package main

import (
	"flag"
	"log"
	"strconv"
	"time"

	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/datastore"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/parser"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/queue"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/source"
)

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
	limitOnFail    = 3
	limitOnSuccess = 10
	delayOnErr     = 3 * time.Second
	delayOnSuccess = 6 * time.Second
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
	out := make(chan datastore.ExchangeData, 1000)

	// connect to beanstalk
	tube, err := queue.Connect("tcp", BEAN_ADDR, TUBE_NAME)
	if err != nil {
		log.Fatal("err during beanstalk ", err)
	}

	// start pool of workers
	// for parallel job processing
	for i := 0; i < *numberOfWorkers; i++ {
		go worker(tube, source.XE_COM, out)
	}

	// pull exchange data from chan to process further if needed
	for v := range out {
		// save to mongo
		err = mongo.Save(v, COLL_NAME)
		if err != nil {
			// log error but continue working
			log.Println("mongo insert failed ", err)
		}
		log.Printf("job saved to mongo %+v\n", v)

	}

}

// job consumer get job from queue, get exchange rate from defined source and output result
func worker(tube *queue.Tube, src parser.ExchangeSource, out chan datastore.ExchangeData) {
	var (
		id    uint64
		job   queue.Job
		rate  float64
		delay time.Duration
		err   error
	)

getJob:
	// get and reserve job
	id, job, err = tube.Reserve(10 * time.Millisecond)
	if err != nil {
		// get another job
		goto getJob
	}

	rate, err = parser.GetRate(src, job.From, job.To)
	if err != nil {
		log.Println("job id ", id, " GetRate err ", err)
		goto onErr
	}
	goto onSuccess

onErr:
	job.Failed()
	if job.FailCounter == limitOnFail {
		// bury job
		if err := tube.Bury(id); err != nil {
			log.Println("bury failed", err)
		}
		log.Println("job is buried ", job)
		goto getJob

	}

	goto putBack

onSuccess:
	job.Succeed()
	// send result to chan
	out <- datastore.ExchangeData{
		Job:       job,
		Rate:      strconv.FormatFloat(rate, 'f', 2, 64),
		CreatedAt: time.Now().UTC().Unix(),
	}

	// condition on success counter
	if job.SuccessCounter == limitOnSuccess {
		// delete currency conversion job
		err = tube.Delete(id)
		if err != nil {
			log.Println("on delete ", err)
		}
		log.Println("job is deleted ", job)
		goto getJob
	}
	goto putBack

putBack:
	if job.Successful {
		delay = delayOnSuccess
	} else {
		delay = delayOnErr
	}
	log.Println("put job back ", job, " with delay ", delay)
	// put back job with a delay
	id, err = tube.PutBack(id, job, delay)
	if err != nil {
		log.Println("put job back failed", job)
	}
	goto getJob

}
