package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/kr/beanstalk"
	"github.com/leesper/go_rng"
)

type Currency struct {
	Abbr string
	Name string
}

type Job struct {
	From string
	To   string
}

const (
	BEAN_ADDR = "challenge.aftership.net:11300"
	TUBE_NAME = "mmirolim"
)

var (
	numberOfJobs = flag.Int("n", 1, "number of jobs to generate")
	fromRate     = flag.String("f", "", "from currency")
	toRate       = flag.String("t", "USD", "to currency")
	// currency rates
	curs = []Currency{
		{"USD", "US Dollar"},
		{"EUR", "Euro"},
		{"GBP", "British Pound"},
		{"INR", "Indian Rupee"},
		{"AUD", "Australian Dollar"},
		{"CAD", "Canadian Dollar"},
		{"SGD", "Singapore Dollar"},
		{"CHF", "Swiss Franc"},
		{"MYR", "Malaysian Ringgit"},
		{"JPY", "Japanese Yen"},
		{"CNY", "Chinese Yuan Renminbi"},
		{"NZD", "New Zealand Dollar"},
		{"THB", "Thai Baht"},
		{"HUF", "Hungarian Forint"},
		{"AED", "Emirati Dirham"},
		{"HKD", "Hong Kong Dollar"},
		{"RUB", "Russian Ruble"},
		{"DKK", "Danish Krone"},
		{"PKR", "Pakistani Rupee"},
		{"ILS", "Israeli Shekel"},
		{"TWD", "Taiwan New Dollar"},
		{"UZS", "Uzbekistani Som"},
	}
)

func init() {
	flag.Parse()
	flag.Usage()
}

func main() {
	var job Job
	conn, err := beanstalk.Dial("tcp", BEAN_ADDR)
	if err != nil {
		log.Fatal("err during beanstalk ", err)
	}
	tube := &beanstalk.Tube{conn, TUBE_NAME}
	uniProb := rng.NewUniformGenerator(time.Now().UnixNano())
	for i := 0; i < *numberOfJobs; i++ {
		if *fromRate != "" {
			job = Job{
				From: *fromRate,
				To:   *toRate,
			}
		} else {

			job = Job{
				From: curs[uniProb.Int64n(int64(len(curs)))].Abbr,
				To:   curs[uniProb.Int64n(int64(len(curs)))].Abbr,
			}
		}
		fmt.Printf("job produced %+v\n", job)
		data, err := json.Marshal(job)
		if err != nil {
			log.Println("json marshal err ", err)
			continue
		}
		id, err := tube.Put(data, 1, 0, time.Minute)
		if err != nil {
			log.Println("something went wrong on tube put ", err)
		}
		fmt.Println("job", id)
	}

}
