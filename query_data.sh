#!/bin/sh
curl -X 'POST' -H 'Content-Type:application/json' http://localhost:8082/druid/v2/?pretty -d @$1
