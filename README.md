Spark Streaming application for OpenStack logs from Kafka
====

## Requirement

 * Apache Maven >= 3

## Build
```
$ git clone https://github.com/wtakase/spark-openstackstreaming
$ cd spark-openstackstreaming
$ mvn clean package
```

## Run
Run the following command with Kafka server (i.e. 192.168.1.250:9092) and topic_id (i.e. openstack) and Elasticsearch node (i.e. 192.168.1.250:9200):
```
$ spark-submit --master local[*] --class jp.kek.spark.openstackstreaming.OpenStackStreaming target/openstackstreaming-0.0.1-jar-with-dependencies.jar 192.168.1.250:9092 openstack 192.168.1.250:9200
```
