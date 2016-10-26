#!/bin/bash
echo "Starting tests"

echo "Installing plugin"
./mvnw clean install -Denforcer.skip=true

PRESTO_VERSION=`cat pom.xml | grep '<version>' | head -n 1 | cut -f2 -d">"|cut -f1 -d"<"`
echo "Presto version $PRESTO_VERSION"
