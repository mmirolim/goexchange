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
	From        string
	To          string
	FailCounter int
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
	getRateRetry               = 3
	getRateDelay time.Duration = 3 * time.Second
)

func worker(queue *beanstalk.Conn, tubeName string, out chan ExchangeData) {
	var job Job
	var rate float64
	tube := beanstalk.Tube{queue, tubeName}
	ts := beanstalk.NewTubeSet(queue, tubeName)
	for {
		id, data, err := ts.Reserve(time.Second)
		if err != nil {
			// TODO handle error
			log.Println(err)
			continue

		}
		err = json.Unmarshal(data, &job)
		if err != nil {
			log.Println(err)
			continue
		}

		rate, err = parser.GetRate(source.XE_COM, job.From, job.To)
		if err != nil {
			log.Println("GetRate err ", err)
			// put back job with a delay
			// and increment fail counter
			job.FailCounter++
			data, err = json.Marshal(&job)
			if err != nil {
				log.Println("job marshal err ", err)
				continue
			}
			id, err = tube.Put(data, 1, getRateDelay, time.Minute)
			if err != nil {
				log.Println("put job with delay err ", err)
			}
			log.Println("job failed put it back with delay ", getRateDelay)
			continue
		}

		// if sucess put job with 60s delay
		queue.Delete(id)
		// send result to chan
		out <- ExchangeData{
			Job:       job,
			Rate:      strconv.FormatFloat(rate, 'f', 2, 64),
			CreatedAt: time.Now().UTC().Unix(),
		}

	}

}
