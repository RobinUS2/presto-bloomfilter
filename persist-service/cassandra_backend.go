package main

import (
	"github.com/gocql/gocql"
	"fmt"
)

type CassandraConf struct {
	ProtoVersion int
	Keyspace string
	Table string
	Hosts []string
	Consistency gocql.Consistency
}

type CassandraBackend struct {
	session *gocql.Session
	config CassandraConf
}

func (b CassandraBackend) Put(k []byte, v []byte) (bool, error) {
	err := b.session.Query(fmt.Sprintf(`INSERT INTO %s (key, value) VALUES (?, ?)`, b.config.Table), k, v).Exec()
	return true, err;
}

func (b CassandraBackend) Get(k []byte) ([]byte, error) {
	var v []byte;
	err := b.session.Query(fmt.Sprintf(`SELECT value FROM %s WHERE key = ? LIMIT 1`, b.config.Table), k).Scan(&v)
	return v, err
}

func newCassandraBackend(conf *Conf) IBackend  {
	// Connect to the cluster
	cluster := gocql.NewCluster(conf.Cassandra.Hosts...)
	cluster.Keyspace = conf.Cassandra.Keyspace
	cluster.Consistency = conf.Cassandra.Consistency
	cluster.ProtoVersion = conf.Cassandra.ProtoVersion
	session, _ := cluster.CreateSession()

	return CassandraBackend{
		session: session,
		config: conf.Cassandra,
	}
}
