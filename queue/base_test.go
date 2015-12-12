package queue

import (
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	jobErrLimit     = 3
	jobSuccessLimit = 10
	delayOnErr      = 3 * time.Millisecond
	delayOnSuccess  = 10 * time.Millisecond
)

type mockQueue struct {
	m sync.Mutex
	q map[uint64][]byte
}

func (m *mockQueue) Delete(id uint64) error {
	m.m.Lock()
	delete(m.q, id)
	m.m.Unlock()
	return nil
}

func (m *mockQueue) Bury(id uint64, pr uint32) error {
	return m.Delete(id)
}

func (m *mockQueue) Reserve(t time.Duration) (uint64, []byte, error) {
	m.m.Lock()
	defer m.m.Unlock()
	for id, _ := range m.q {
		return id, m.q[id], nil
	}

	return uint64(0), []byte(""), nil
}

func (m *mockQueue) Put(data []byte, pr uint32, delay, ttr time.Duration) (uint64, error) {
	m.m.Lock()
	// get random number, check if job not exist already add it and return
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		id := uint64(r.Int63())
		if _, ok := m.q[id]; !ok {
			m.m.Unlock()
			go func() {
				time.Sleep(delay)
				m.m.Lock()
				m.q[id] = data
				m.m.Unlock()
			}()
			return id, nil
		}
	}

}

func TestJobFailed(t *testing.T) {
	var job Job
	job.Failed()
	job.Failed()
	if job.FailCounter != 2 {
		t.Errorf("got job fail counter %d, want %d", job.FailCounter, 2)
	}

	if job.Successful {
		t.Errorf("got job status %s, want %s", job.Successful, false)
	}
}

func TestJobSucceed(t *testing.T) {
	var job Job
	job.Succeed()
	job.Succeed()
	if job.SuccessCounter != 2 {
		t.Errorf("got job fail counter %d, want %d", job.FailCounter, 2)
	}

	if !job.Successful {
		t.Errorf("got job status %s, want %s", job.Successful, false)
	}
}

func TestReserve(t *testing.T) {
	_, tube, jobOne, err := createMockQueue(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, job, err := tube.Reserve(time.Millisecond)
	if err != nil {
		t.Error(err)
		return
	}

	if job.From != jobOne.From {
		t.Errorf("got job %v, want %v", job, jobOne)
	}

}

func TestPutBackOnSuccess(t *testing.T) {
	var id uint64 = 10
	mq, tube, _, err := createMockQueue(id)
	if err != nil {
		t.Error(err)
		return
	}
	// put some successful job
	job := Job{
		From:           "PUTBACK",
		To:             "PUTITBACK",
		FailCounter:    2,
		SuccessCounter: 3,
		Successful:     true,
	}

	id, err = tube.PutBack(id, job)
	if _, ok := mq.q[id]; ok {
		t.Errorf("job should be delay for %v before requeue", delayOnSuccess)
	}
	// wait till delay on success
	time.Sleep(delayOnSuccess)
	// extra wait
	time.Sleep(time.Microsecond)
	if _, ok := mq.q[id]; !ok {
		t.Errorf("job with id %d should be in queue ", id)
	}
}

func TestPutBackOnErr(t *testing.T) {
	var id uint64 = 10
	mq, tube, _, err := createMockQueue(id)
	if err != nil {
		t.Error(err)
		return
	}

	// put some failed job
	job := Job{
		From:           "PUTBACK",
		To:             "PUTITBACK",
		FailCounter:    2,
		SuccessCounter: 3,
		Successful:     false,
	}

	id, err = tube.PutBack(id, job)
	if _, ok := mq.q[id]; ok {
		t.Errorf("job should be delay for %v before requeue", delayOnErr)
	}
	// wait till delay on success
	time.Sleep(delayOnErr)
	// extra wait
	time.Sleep(time.Microsecond)
	if _, ok := mq.q[id]; !ok {
		t.Errorf("job with id %d should be in queue ", id)
	}
}

func TestPutBackOnSuccessLimit(t *testing.T) {
	var id uint64 = 10
	mq, tube, _, err := createMockQueue(id)
	if err != nil {
		t.Error(err)
		return
	}

	// put some successful job which reached limit
	job := Job{
		From:           "PUTBACK",
		To:             "PUTITBACK",
		FailCounter:    2,
		SuccessCounter: jobSuccessLimit,
		Successful:     true,
	}

	id, err = tube.PutBack(id, job)
	if _, ok := mq.q[id]; ok {
		t.Errorf("job %v with id %d should not be in queue", job, id)
	}
}

func TestPutBackOnErrLimit(t *testing.T) {
	var id uint64 = 10
	mq, tube, _, err := createMockQueue(id)
	if err != nil {
		t.Error(err)
		return
	}

	// put some failed job which reached limit
	job := Job{
		From:           "PUTBACK",
		To:             "PUTITBACK",
		FailCounter:    jobErrLimit,
		SuccessCounter: 8,
		Successful:     false,
	}

	id, err = tube.PutBack(id, job)
	if _, ok := mq.q[id]; ok {
		t.Errorf("job %v with id %d should not be in queue", job, id)
	}

}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func createMockQueue(firstId uint64) (*mockQueue, *Tube, Job, error) {
	var mq mockQueue
	// init map as queue
	mq.q = make(map[uint64][]byte)
	// put job
	jobOne := Job{From: "SMT", To: "SMTELSE"}
	data, err := json.Marshal(&jobOne)
	if err != nil {
		return nil, nil, jobOne, err
	}
	mq.q[firstId] = data
	fakeTube := Tube{
		jobErrLimit:     jobErrLimit,
		jobSuccessLimit: jobSuccessLimit,
		delayOnErr:      delayOnErr,
		delayOnSuccess:  delayOnSuccess,
		q:               &mq,
		t:               &mq,
		ts:              &mq,
	}

	return &mq, &fakeTube, jobOne, nil
}
