#!/bin/sh
root_dir=$(pwd)
druid_version=0.9.1-SNAPSHOT
cd distribution/target
tar xvzf druid-${druid_version}-bin.tar.gz
cd druid-${druid_version}

#java -classpath "./lib/*" io.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client:2.6.2
cp extensions/druid-hdfs-storage/druid-hdfs-storage-${druid_version}.jar ./
rm extensions/druid-hdfs-storage/*
cp ./druid-hdfs-storage-${druid_version}.jar extensions/druid-hdfs-storage/
cp ${root_dir}/hadoop-client/2.6.2/* extensions/druid-hdfs-storage/
rm extensions/druid-hdfs-storage/guava-11.0.2.jar

cd $root_dir
cp -a ./conf-quickstart distribution/target/druid-${druid_version}

mkdir distribution/target/druid-${druid_version}/extensions/druid-orc-extensions
cp -a extensions-contrib/orc-extensions/target/druid-orc-extensions-${druid_version}.jar distribution/target/druid-${druid_version}/extensions/druid-orc-extensions/
cp -a ./quickstart/hive-exec-2.0.0.jar distribution/target/druid-${druid_version}/extensions/druid-orc-extensions/
