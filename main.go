package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mgutz/ansi"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/datastore"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/parser"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/queue"
	"github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/source"
)

var (
	file            = flag.String("conf", "conf.toml", "config file in toml format")
	numberOfWorkers = flag.Int("wn", 10, "number of workers to process jobs")
	colorRed        = ansi.ColorFunc("red")
	colorGreen      = ansi.ColorFunc("green")
)

type AppConf struct {
	Queue struct {
		Protocol string
		Host     string
		Tube     string
	}

	Mongo struct {
		DSN      string
		DB       string
		CollName string
		Timeout  int
		ErrLimit int
	}

	Job struct {
		FailLimit      int
		SuccessLimit   int
		DelayOnErr     int
		DelayOnSuccess int
	}
}

func init() {
	// parse cmd line flags
	flag.Parse()
	flag.Usage()
}

func main() {
	var appCfg AppConf
	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}

	_, err = toml.DecodeReader(f, &appCfg)
	if err != nil {
		log.Fatal(err)
	}

	// connect to mongo
	mongo, err := datastore.Connect(
		appCfg.Mongo.DSN,
		appCfg.Mongo.DB,
		time.Duration(appCfg.Mongo.Timeout)*time.Second,
	)
	if err != nil {
		log.Fatal("mongo ", err)
	}

	// queue of computed exchange rates data
	out := make(chan datastore.ExchangeData, 1000)

	// connect to beanstalk
	tube, err := queue.Connect(
		appCfg.Queue.Protocol,
		appCfg.Queue.Host,
		appCfg.Queue.Tube,
		appCfg.Job.FailLimit,
		appCfg.Job.SuccessLimit,
		time.Duration(appCfg.Job.DelayOnErr)*time.Second,
		time.Duration(appCfg.Job.DelayOnSuccess)*time.Second,
	)

	if err != nil {
		log.Fatal(err)
	}

	// start pool of workers
	// for parallel job processing
	for i := 0; i < *numberOfWorkers; i++ {
		go worker(tube, source.XE_COM, out)
	}

	// pull exchange data from chan to process further if needed
loop:
	for v := range out {
		// save to mongo
		err = mongo.Save(v, appCfg.Mongo.CollName)
		if err != nil {
			log.Println(colorRed("[ERR] on mongo insert "), err)
			// handle connection error
			if e := handleMongoErr(mongo, appCfg.Mongo.ErrLimit); e != nil {
				log.Println(colorRed(e.Error()))
				break loop
			}
			continue
		}

		log.Printf(colorGreen("[SUCC] job saved to mongo %+v\n"), v)

	}

	log.Println(colorRed("Exiting Bye"))

}

// job consumer get job from queue, get exchange rate from defined source and output result
func worker(tube *queue.Tube, src parser.ExchangeSource, out chan datastore.ExchangeData) {
	var (
		id   uint64
		job  queue.Job
		rate float64
		err  error
	)

	for {
		// get and reserve job
		id, job, err = tube.Reserve(10 * time.Millisecond)
		if err != nil {
			// get another job
			continue
		}

		rate, err = parser.GetRate(src, job.From, job.To)
		if err != nil {
			log.Println(colorRed("[ERR] on GetRate "), err, job)
			job.Failed()
		} else {
			job.Succeed()
			// send result to chan
			out <- datastore.ExchangeData{
				Job:       job,
				Rate:      strconv.FormatFloat(rate, 'f', 2, 64),
				CreatedAt: time.Now().UTC().Unix(),
			}
		}
		// put job pack
		id, err = tube.PutBack(id, job)
		if err != nil {
			log.Println(colorRed("[ERR] on put job back"), job)
		}
		log.Println(colorGreen("[SUCC] put job back "), job)
	}

}

// handle mongo err, try ping it number of times and wait
func handleMongoErr(db *datastore.DB, limitTries int) error {
	// back off and check connection
	for i := 0; i <= limitTries; i++ {
		// exit outer loop and exit application
		if i == limitTries {
			return errors.New("[ERR] too many mongo connection errors")
		}
		log.Println("back off and wait and Ping mongo after", i, "s")
		time.Sleep(time.Duration(i) * time.Second)
		if err := db.Ping(); err != nil {
			// on err wait and ping again
			continue
		} else {
			// on success continue job
			break
		}

	}

	return nil
}
