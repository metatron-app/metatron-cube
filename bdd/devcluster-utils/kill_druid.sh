#!/bin/sh

for i in `cat slaves`
do
  ssh $i "ps -ef | grep io.druid.cli.Main | grep -v grep | awk '{print \$2}' | xargs kill -9"
done
