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
	Successful     bool
	FailCounter    int `bson:"-"` // ignore in mongo
	SuccessCounter int `bson:"-"` // ignore in mongo
}

func (j *Job) Failed() {
	j.FailCounter++
	j.Successful = false
}

func (j *Job) Succeed() {
	j.SuccessCounter++
	j.Successful = true
}

// define interface for queue tube
type tubeSet interface {
	Reserve(time.Duration) (uint64, []byte, error)
}

type tube interface {
	Put([]byte, uint32, time.Duration, time.Duration) (uint64, error)
}

type conn interface {
	Delete(uint64) error
	Bury(uint64, uint32) error
}

// Tube decorator to simplify our use case
/*type Tube struct {
	jobErrLimit     int
	jobSuccessLimit int
	delayOnErr      time.Duration
	delayOnSuccess  time.Duration
	q               *beanstalk.Conn
	t               *beanstalk.Tube
	ts              *beanstalk.TubeSet
}
*/
// Tube decorator to simplify our use case
type Tube struct {
	jobErrLimit     int
	jobSuccessLimit int
	delayOnErr      time.Duration
	delayOnSuccess  time.Duration
	q               conn
	t               tube
	ts              tubeSet
}

func Connect(protocol, addr, tube string, jobErrLimit, jobSuccessLimit int, delayOnErr, delayOnSuccess time.Duration) (*Tube, error) {
	conn, err := beanstalk.Dial(protocol, addr)
	if err != nil {
		return nil, err
	}
	return &Tube{
		jobErrLimit:     jobErrLimit,
		jobSuccessLimit: jobSuccessLimit,
		delayOnErr:      delayOnErr,
		delayOnSuccess:  delayOnSuccess,
		q:               conn,
		t:               &beanstalk.Tube{conn, tube},
		ts:              beanstalk.NewTubeSet(conn, tube),
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

// PutBack job with delay depending on success or failure of job
// if err/success reached limits then bury/delete job
func (tb *Tube) PutBack(id uint64, job Job) (uint64, error) {
	if job.FailCounter == tb.jobErrLimit {
		// bury job
		return id, tb.Bury(id)
	}
	if job.SuccessCounter == tb.jobSuccessLimit {
		// delete job
		return id, tb.Delete(id)
	}

	data, err := json.Marshal(job)
	if err != nil {
		return 0, err
	}

	// swap jobs by deleting previous and putting new one
	err = tb.q.Delete(id)
	if err != nil {
		return 0, err
	}
	var delay time.Duration
	if job.Successful {
		delay = tb.delayOnSuccess
	} else {
		delay = tb.delayOnErr
	}

	return tb.t.Put(data, 1, delay, time.Minute)
}

// Bury job with default priority 1 if not defined
func (tb *Tube) Bury(id uint64, pr ...uint32) error {
	if len(pr) == 1 {
		return tb.q.Bury(id, pr[0])
	}
	return tb.q.Bury(id, 1)
}

// Delete job from tube
func (tb *Tube) Delete(id uint64) error {
	return tb.q.Delete(id)
}
