#!/bin/sh
# see https://tde.sktelecom.com/wiki/display/BDD/Druid
for i in `cat slaves`
do
  ssh $i mkdir /data1/druid/tmp
  ssh $i mkdir -p /home/hadoop/server/druid/var
  ssh $i "cd /home/hadoop/server/druid; ./bin/init"
done
