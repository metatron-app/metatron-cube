#!/bin/sh
root_dir=$(pwd)
druid_release=druid-0.9.0-SNAPSHOT 
target_dir=${root_dir}/distribution/target
git_revision=`git rev-parse HEAD`
git_origin=`git config --get remote.origin.url`
date=`LC_ALL=C date`
druid_release_dir="${druid_release}.${git_revision}"
druid_release_tar="${druid_release_dir}-bin.tar.gz"

rm -rf ${target_dir}/${druid_release}

cd ${target_dir}
tar xvzf ${druid_release}-bin.tar.gz

cd ${druid_release}
mv conf conf.bak
printf "date:${date}\norigin:${git_origin}\nrevision:${git_revision}" > BUILD.INFO

cd $root_dir/bdd
cp -a ./conf-dev ${target_dir}/${druid_release}/conf
cp -a ./*.sh ${target_dir}/${druid_release}

cd ${target_dir}
tar xvzf mysql-metadata-storage-bin.tar.gz
cp -a ./mysql-metadata-storage ${druid_release}/extensions/
rm "${druid_release_tar}"
mv ${druid_release} ${druid_release_dir}
tar cvzf "${druid_release_tar}" ${druid_release_dir}
mv ${druid_release_dir} ${druid_release}
cp -f "${druid_release_tar}" ${root_dir}
