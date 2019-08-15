#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export GOPATH=$DIR
mkdir -p bin
export GOBIN=$DIR/bin
go get -v "github.com/qiangxue/fasthttp-routing"
go get -v "github.com/valyala/fasthttp"
go get -v "github.com/boltdb/bolt"
go get -v "github.com/gocql/gocql"
echo "Changing directory to $DIR"
cd $DIR
go build -v .

