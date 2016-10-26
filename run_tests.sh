#!/bin/bash
echo "test2"
cat ~/.m2/settings.xml
git clone https://github.com/Baqend/Orestes-Bloomfilter.git
cd Orestes-Bloomfilter
mvn clean install
cd ..
./mvnw clean install
