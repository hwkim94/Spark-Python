#!/bin/sh

cd kafka
./bin/kafka-server-stop.sh
cd ~

cd zookeeper
./bin/zkServer.sh stop
cd ~

cd hadoop
./sbin/stop-all.sh
cd ~
