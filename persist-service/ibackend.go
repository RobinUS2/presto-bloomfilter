package main

type BackendType string

const (
	BackendFile BackendType = "file"
	BackendCassandra = "cassandra"
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
		backend = newFileBackend(conf)
		break
	}
	return backend
}
