
#!/bin/bash
echo "Starting tests"

# Install plugin
echo "Installing plugin"
./mvnw clean install -Denforcer.skip=true

# Determine presto parent from pom
PRESTO_VERSION=`cat pom.xml | grep '<version>' | head -n 1 | cut -f2 -d">"|cut -f1 -d"<"`
echo "Presto version $PRESTO_VERSION"

# Download presto
cd ~
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/$PRESTO_VERSION/presto-server-$PRESTO_VERSION.tar.gz
tar -zxvf presto-server-$PRESTO_VERSION.tar.gz
PRESTO_FOLDER=presto-server-$PRESTO_VERSION
echo "Entering $PRESTO_FOLDER"
cd $PRESTO_FOLDER
mkdir etc
ls -lah

# Presto conf node
mkdir -p /tmp/presto/data
echo 'node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/tmp/presto/data' > etc/node.properties

# Presto conf jvm
echo '-server
-Xmx16G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p' > etc/jvm.config

# Presto conf service
echo 'coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=5GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://example.net:8080' > etc/config.properties

# Logging
echo 'com.facebook.presto=DEBUG' > etc/log.properties

# TPCH connector
mkdir etc/catalog
echo 'connector.name=tpch' > etc/catalog/tpch.properties

# Move bloomfilter to plugin
cp ~/.m2/repository/com/facebook/presto/presto-bloomfilter/$PRESTO_VERSION/presto-bloomfilter-$PRESTO_VERSION.zip plugin/
cd plugin
unzip presto-bloomfilter-$PRESTO_VERSION.zip
ls -lah 
rm *.zip
cd ..

# Start presto
bin/launcher start

# Short wait
sleep 1
bin/launcher status
ps aux | grep presto

# Wait until startup
i="0"
while [ $i -lt 60 ]
do
	# Started?
	LINES_MATCHED=`cat /tmp/presto/data/var/log/server.log | grep -i 'server started' | wc -l`
	if [ "$LINES_MATCHED" -eq "1" ]; then 
		echo "Server started"
		break
	fi

	# Not started, wait
	sleep 1
	i=$[$i+1]
done
tail -n 500 /tmp/presto/data/var/log/server.log

# Cli
cd ~ 
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/$PRESTO_VERSION/presto-cli-$PRESTO_VERSION-executable.jar
CLI=presto-cli-$PRESTO_VERSION-executable.jar
chmod +x $CLI

# Wait a bit more
RES=`./$CLI --server http://localhost:8080 --catalog tpch --schema tiny --execute 'WITH input AS (SELECT DISTINCT name FROM nation LIMIT 3), a AS (SELECT bloom_filter(name) AS bf FROM input LIMIT 3) SELECT count(1) FROM nation, a WHERE bloom_filter_contains(a.bf, nation.name)' --output-format TSV`
echo $RES
if [ "$RES" == "3" ]; then
	echo "Test passed"
else
	echo "Test ERROR"
	exit 1
fi
