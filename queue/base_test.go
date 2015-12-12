package queue

import "testing"

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
