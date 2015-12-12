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
		log.Printf("got exchange data %+v\n", v)
		// save to mongo
		err = mongo.Save(v, COLL_NAME)
		if err != nil {
			// log error but continue working
			log.Println("mongo insert failed ", err)
		}
		log.Println("saved to mongo")

	}

}

// job consumer get job from queue, get exchange rate from defined source and output result
func worker(tube *queue.Tube, src parser.ExchangeSource, out chan datastore.ExchangeData) {

	for {
		// get and reserve job
		id, job, err := tube.Reserve(10 * time.Millisecond)
		if err != nil {
			// get another job
			continue
		}

		rate, err := parser.GetRate(src, job.From, job.To)
		if err != nil {
			log.Println("job id ", id, " GetRate err ", err)

			// and increment fail counter
			job.FailCounter++
			if job.FailCounter == limitOnFail {
				// bury job
				if err := tube.Bury(id); err != nil {
					log.Println("bury failed", err)
					continue
				}

				log.Println("job is buried ", job)
				continue
			}
			// put back job with a delay
			id, err = tube.PutBack(id, job, delayOnGetRateErr)
			if err != nil {
				log.Println("put back job failed ", job)
			}

			// go get another job
			continue
		}

		// on sucess incr success counter
		job.SuccessCounter++
		// condition on success counter
		if job.SuccessCounter == limitOnSuccess {
			// delete currency conversion job
			err = tube.Delete(id)
			if err != nil {
				log.Println("on delete ", err)
			}
		} else {
			// put job back with incr counter
			id, err = tube.PutBack(id, job, delayOnSuccess)
			if err != nil {
				log.Println("put back failed ", err)
			}
		}
		// send result to chan
		out <- datastore.ExchangeData{
			Job:       job,
			Rate:      strconv.FormatFloat(rate, 'f', 2, 64),
			CreatedAt: time.Now().UTC().Unix(),
		}

	}

}
