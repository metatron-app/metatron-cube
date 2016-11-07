#!/bin/sh

root_dir=$(pwd)
cd distribution/target/druid-0.9.1-SNAPSHOT
mkdir logs

java -Xdebug -Xrunjdwp:transport=dt_socket,address=0.0.0.0:8881,server=y,suspend=n `cat conf-quickstart/druid/coordinator/jvm.config | xargs` -cp conf-quickstart/druid:lib/*:/Users/racoon/hadoop/etc/hadoop io.druid.cli.Main server coordinator > ./logs/druid-coordinator.log 2>&1 &

java -Xdebug -Xrunjdwp:transport=dt_socket,address=0.0.0.0:8882,server=y,suspend=n `cat conf-quickstart/druid/historical/jvm.config | xargs` -cp conf-quickstart/druid:lib/*:/Users/racoon/hadoop/etc/hadoop io.druid.cli.Main server historical > ./logs/druid-historical.log 2>&1 &

java -Xdebug -Xrunjdwp:transport=dt_socket,address=0.0.0.0:8883,server=y,suspend=n `cat conf-quickstart/druid/broker/jvm.config | xargs` -cp conf-quickstart/druid:lib/*:/Users/racoon/hadoop/etc/hadoop io.druid.cli.Main server broker > ./logs/druid-broker.log 2>&1 &

java -Xdebug -Xrunjdwp:transport=dt_socket,address=0.0.0.0:8884,server=y,suspend=n `cat conf-quickstart/druid/overlord/jvm.config | xargs` -cp conf-quickstart/druid:lib/*:/Users/racoon/hadoop/etc/hadoop io.druid.cli.Main server overlord > ./logs/druid-overlord.log 2>&1 &

java -Xdebug -Xrunjdwp:transport=dt_socket,address=0.0.0.0:8885,server=y,suspend=n `cat conf-quickstart/druid/middleManager/jvm.config | xargs` -cp conf-quickstart/druid:lib/*:/Users/racoon/hadoop/etc/hadoop io.druid.cli.Main server middleManager > ./logs/druid-middleManager.log 2>&1 &

cd $root_dir

ps -ef | grep io.druid.cli.Main | grep -v grep

echo "coordinator : localhost:8081"
echo "broker : localhost:8082"
echo "historical : localhost:8083"
echo "realtime : localhost:8084"
echo "overlord : localhost:8090"

