#!/bin/bash

function join { local IFS="&"; echo "$*"; }

QUERY_FILE=$1

shift

if [[ -z $1 || $1 == *'='* ]] ; then
  HOST='localhost:8082'
else
  HOST=$1
  shift
fi

curl -X POST -H "Content-Type: text/plain" -d @$QUERY_FILE "http://@$HOST/druid/v2/sql?$(join $@)"
