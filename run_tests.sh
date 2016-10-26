#!/bin/bash
echo "test2"
./mvnw help:effective-pom | grep -A 10 -B 10 jcenter
./mvnw clean install
