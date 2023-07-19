=============== KAFKA ================

### ZOOKEEPER
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties

### BROKER
kafka-server-start /opt/homebrew/etc/kafka/server.properties

### CREATE TOPIC
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

### DELETE TOPIC
kafka-topics --bootstrap-server localhost:9092 --delete --topic sub1_physiological

### LIST TOPICS
kafka-topics --bootstrap-server=localhost:9092 --list

### PRODUCER
kafka-console-producer --broker-list localhost:9092 --topic test

### CONSUMER
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning

=============== /KAFKA ================









=============== SIGNAL ================

- PPG is the method that can extract BVP for example

=============== /SIGNAL ================









=============== DOCKER ================

### Minio Client
docker run -p 9001:9000 -p 9091:9090 --name minio -v ~/minio_data:/data -e "MINIO_ROOT_USER=admin" -e "MINIO_ROOT_PASSWORD=admin1234" quay.io/minio/minio server /data --console-address ":9090"

### DELTA SHARING SERVER
docker run -p 8080:8080 -d --mount type=bind,source=/Users/cunha/Desktop/Dissertação/Dissertacao/code/delta-sharing/delta-sharing-server.yaml,target=config/delta-sharing-server-config.yaml deltaio/delta-sharing-server:0.6.2 -- --config /config/delta-sharing-server-config.yaml

docker run -p 8080:8080 -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin --mount type=bind,source=/Users/cunha/Desktop/Dissertação/Dissertacao/code/delta-sharing/delta-sharing-server.yaml,target=/config/delta-sharing-server-config.yaml deltaio/delta-sharing-server:0.6.2 -- --config /config/delta-sharing-server-config.yaml


#### NOTE FOR DELTA SHARING SERVER:
Need to add file core-site-xml (/opt/docker/conf):
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
 <property>
  <name>fs.s3a.endpoint</name>
  <value>http://192.168.1.114:9001</value>
 </property>
</configuration>

=============== /DOCKER ================
