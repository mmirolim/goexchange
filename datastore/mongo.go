package datastore

import (
	"time"

	"gopkg.in/mgo.v2"
)

// DB wraps mongo db connection
type DB struct {
	db string       // db name
	m  *mgo.Session // mongo active session
}

// mongo initialize connection pool to db with timeout
func Connect(host, db string, timeout time.Duration) (*DB, error) {
	// start pool of connectin to mongo db with 1 second timeout
	m, err := mgo.DialWithTimeout(host, timeout)
	if err != nil {
		return nil, err
	}
	return &DB{db, m}, nil
}

// save documents to collection
func (d *DB) Save(doc interface{}, collName string) error {
	s := d.m.Copy()
	defer s.Close()
	return d.m.DB(d.db).C(collName).Insert(doc)
}

// TODO add history exchage rates
