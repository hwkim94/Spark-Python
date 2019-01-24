#!/bin/sh

cd hadoop
./sbin/start-all.sh
cd ~

cd zookeeper
./bin/zkServer.sh start
cd ~

cd kafka
./bin/kafka-server-start.sh ./config/server.properties &
cd ~

pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 
