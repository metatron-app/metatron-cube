/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public interface ServiceTypes
{
  String COORDINATOR = "coordinator";
  String BROKER = "broker";
  String HISTORICAL = "historical";
  String OVERLORD = "overlord";
  String MIDDLE_MANAGER = "middleManager";
  String REALTIME = "realtime";
  String PEON = "peon";
  String ROUTER = "router";
  String HADOOP_INDEXING = "hadoop-indexing";
  String INTERNAL_HADOOP_INDEXER = "internal-hadoop-indexer";

  Map<String, String> TYPE_TO_RESOURCE = ImmutableMap.<String, String>of(
      OVERLORD, "indexer",
      MIDDLE_MANAGER, "worker"
  );
}
