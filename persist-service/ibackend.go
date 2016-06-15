package main

type IBackend interface {
	// Put, key => value
	Put([]byte, []byte) (bool, error)

	// Get, key
	Get([]byte) ([]byte, error)
}

func newBackend(conf *Conf) IBackend {
	return newFileBackend(conf)
}
