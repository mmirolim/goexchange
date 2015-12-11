package datastore

import "github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ/queue"

// ExchangeData is result for a job
// TODO add source to job (where to get data)
type ExchangeData struct {
	queue.Job
	Rate      string // rate in string format with 2 decimal numbers
	CreatedAt int64  `bson:"created_at" json:"created_at"` // timestamp
}
