#!/bin/sh
curl -X 'POST' -H 'Content-Type:application/json' localhost:8090/druid/indexer/v1/task -d @$1
