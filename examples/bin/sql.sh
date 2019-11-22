#!/bin/bash

QUERY_FILE=$1

if [[ -z $2 ]] ; then
  HOST='localhost:8082'
else
  HOST=$2
fi

curl -X POST -H "Content-Type: application/json" -d @$QUERY_FILE "http://@$HOST/druid/v2/sql"
