#!/bin/sh
root_dir=$(pwd)
druid_release=druid-0.9.1-SNAPSHOT 
target_dir=${root_dir}/distribution/target
git_revision=`git rev-parse HEAD`
git_origin=`git config --get remote.origin.url`
date=`LC_ALL=C date`
date_hour=`LC_ALL=C date +%Y%m%d%H%M`
druid_release_dir="${druid_release}.${date_hour}.${git_revision}"
druid_release_tar="${druid_release_dir}-bin.tar.gz"

echo "clean up"
rm -rf ${target_dir}/${druid_release}

set -e
cd ${target_dir}
tar xvzf ${druid_release}-bin.tar.gz

cd ${druid_release}
mv conf conf.bak

echo "make build info file"
printf "date:${date}\norigin:${git_origin}\nrevision:${git_revision}" > BUILD.INFO

cd ${root_dir}
echo "copy configuration and scripts"
cp -a ./bdd/conf-dev ${target_dir}/${druid_release}/conf
cp -a ./bdd/*.sh ${target_dir}/${druid_release}

echo "copy extensions-contrib"
echo "copy kafka-eight-simple-consumer"
mkdir -p ${target_dir}/${druid_release}/extensions/druid-kafka-eight-simple-consumer/
cp ./extensions-contrib/kafka-eight-simpleConsumer/target/druid-kafka-eight-simple-consumer-0.9.1-SNAPSHOT.jar ${target_dir}/${druid_release}/extensions/druid-kafka-eight-simple-consumer/
cp -a ./bdd/deps/kafka-eight-deps/* ${target_dir}/${druid_release}/extensions/druid-kafka-eight-simple-consumer

echo "hive jdbc deps"
cp -a ./bdd/deps/hive-deps/* ${target_dir}/${druid_release}/lib/

echo "copy orc extension"
mkdir -p ${target_dir}/${druid_release}/extensions/druid-orc-extensions
cp ./extensions-contrib/orc-extensions/target/druid-orc-extensions-0.9.1-SNAPSHOT.jar ${target_dir}/${druid_release}/extensions/druid-orc-extensions/

echo "copy t-digest extensions"
mkdir -p ${target_dir}/${druid_release}/extensions/t-digest-median
cp extensions-contrib/median/target/druid-median-0.9.1-SNAPSHOT.jar ${target_dir}/${druid_release}/extensions/t-digest-median

cd ${target_dir}
echo "copy mysql metadata module"
tar xvzf mysql-metadata-storage-0.9.1-SNAPSHOT.tar.gz
cp -a ./mysql-metadata-storage ${druid_release}/extensions/

set +e
echo "finalize the artifact"
rm "${druid_release_tar}"
mv ${druid_release} ${druid_release_dir}
tar cvzf "${druid_release_tar}" ${druid_release_dir}
mv ${druid_release_dir} ${druid_release}

echo "done check for ${root_dir}/${druid_release_tar}"
cp -f "${druid_release_tar}" ${root_dir}
