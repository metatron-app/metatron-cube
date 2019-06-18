#!/bin/bash -eu
# Licensed to SK Telecom Co., LTD. (SK Telecom) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  SK Telecom licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# Create kafka topic and execute supervisor for the metric.
# - Set KAFKA_HOME, ZK_HOST at prepare-metrics.sh and execute prepare-metrics.sh
# - Set the bootstrap.servers at metric-supervisor.json and execute supervisor spec
#     curl -XPOST -H'Content-Type: application/json' -d @metric/metric-supervisor.json http://localhost:8090/druid/indexer/v1/supervisor
#

usage="Usage: Set KAFKA_HOME and ZK_HOST at prepare-metric.sh"
echo $usage
echo ""

# The kafka home directory
KAFKA_HOME=/usr/local/kafka
echo "KAFKA_HOME=$KAFKA_HOME"

# The zookeeper host used for the kafka
ZK_HOST="localhost:2181"
echo "ZK_HOST=$ZK_HOST"

echo "Trying to create topic druid-metric"
command="$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZK_HOST --create --replication-factor 1 --partitions 1 --topic druid-metric"
(exec $command)
sleep 1

echo "Trying to create topic druid-alert"
command="$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZK_HOST --create --replication-factor 1 --partitions 1 --topic druid-alert"
(exec $command)
sleep 1

echo "Done"

