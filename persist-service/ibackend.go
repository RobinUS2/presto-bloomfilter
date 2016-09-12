package main

type BackendType int

const (
	BackendFile BackendType = iota
	BackendCassandra
)

type IBackend interface {
	// Put, key => value
	Put([]byte, []byte) (bool, error)

	// Get, key
	Get([]byte) ([]byte, error)
}

func newBackend(conf *Conf) IBackend {
	var backend IBackend;
	switch conf.Backend {
	case BackendCassandra:
		backend = newCassandraBackend(conf)
		break
	case BackendFile:
	default:
		backend = newFileBackend(conf)
		break
	}
	return backend
}
