#!/bin/sh
root_dir=$(pwd)
cd distribution/target
tar xvzf druid-0.9.1-SNAPSHOT-bin.tar.gz
cd druid-0.9.1-SNAPSHOT 
mkdir -p log
mkdir -p logs
cd $root_dir
cp -a ./conf-quickstart distribution/target/druid-0.9.1-SNAPSHOT 

mkdir distribution/target/druid-0.9.1-SNAPSHOT/extensions/orc-extension
cp -a extensions-contrib/orc-extensions/target/druid-orc-extensions-0.9.1-SNAPSHOT.jar distribution/target/druid-0.9.1-SNAPSHOT/extensions/orc-extension/
cp -a ../apache-hive-2.0.0-bin/lib/* distribution/target/druid-0.9.1-SNAPSHOT/extensions/orc-extension/

