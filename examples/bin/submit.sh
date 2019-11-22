#!/bin/bash

TASK_FILE=$1

if [[ -z $2 ]] ; then
  HOST='localhost:8090'
else
  HOST=$2
fi

curl -X POST -H "Content-Type: application/json" -d @$TASK_FILE "http://@$HOST/druid/indexer/v1/task"
