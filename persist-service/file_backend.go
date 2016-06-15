package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"sync"
)

type FileBackend struct {
	db     *bolt.DB
	bucket []byte
}

func (b FileBackend) Put(k []byte, v []byte) (bool, error) {
	// Wait group
	var wg sync.WaitGroup
	wg.Add(1)

	// Start
	var err error = nil
	b.db.Update(func(tx *bolt.Tx) error {
		bu := tx.Bucket(b.bucket)
		err = bu.Put(k, v)
		wg.Done()
		return err
	})

	// Wait
	wg.Wait()

	return err == nil, err
}

func (b FileBackend) Get(k []byte) ([]byte, error) {
	// Wait group
	var wg sync.WaitGroup
	wg.Add(1)

	// Start
	var err error = nil
	var by []byte = nil
	b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		v := b.Get(k)
		by = v
		wg.Done()
		return nil
	})

	// Wait
	wg.Wait()

	return by, err
}

func newFileBackend(conf *Conf) IBackend {
	b := FileBackend{
		bucket: []byte("store"),
	}

	// Open db
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	b.db = db

	// Db bucket
	b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(b.bucket)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	return b
}
