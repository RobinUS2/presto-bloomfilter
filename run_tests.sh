
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
ls -lah presto-server*

# Presto conf node
echo 'node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/var/presto/data' > etc/node.properties

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

# Start presto
bin/launcher start

# Wait
sleep 10

# Started?
bin/launcher status
ps aux | grep presto
