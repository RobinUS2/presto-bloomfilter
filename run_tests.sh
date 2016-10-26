
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

# Move bloomfilter to plugin
cp ~/.m2/repository/com/facebook/presto/presto-bloomfilter/$PRESTO_VERSION/presto-bloomfilter-$PRESTO_VERSION.zip plugin/
unzip plugin/presto-bloomfilter-$PRESTO_VERSION.zip
rm plugin/*.zip
ls -lah plugin/

# Start presto
bin/launcher start

# Wait
sleep 10

# Started?
bin/launcher status
ps aux | grep presto
