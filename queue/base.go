package queue

import (
	"encoding/json"
	"time"

	"github.com/kr/beanstalk"
)

// Job in beanstalkd
type Job struct {
	From           string
	To             string
	FailCounter    int `bson:"-"` // ignore in mongo
	SuccessCounter int `bson:"-"` // ignore in mongo
}

// Tube decorator to simplify our use case
type Tube struct {
	q  *beanstalk.Conn
	t  *beanstalk.Tube
	ts *beanstalk.TubeSet
}

func Connect(protocol, addr, tube string) (*Tube, error) {
	conn, err := beanstalk.Dial(protocol, addr)
	if err != nil {
		return nil, err
	}
	return &Tube{
		q:  conn,
		t:  &beanstalk.Tube{conn, tube},
		ts: beanstalk.NewTubeSet(conn, tube),
	}, nil
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
