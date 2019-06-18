/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;

import java.util.Map;

/**
 */
public class ForeverLoadRule extends LoadRule.Always
{
  public static LoadRule of(int replicant)
  {
    return new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, replicant));
  }

  private final Map<String, Integer> tieredReplicants;

  @JsonCreator
  public ForeverLoadRule(
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants
  )
  {
    this.tieredReplicants = tieredReplicants == null ? ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS) : tieredReplicants;
    validateTieredReplicants(this.tieredReplicants);
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadForever";
  }

  @Override
  @JsonProperty
  public Map<String, Integer> getTieredReplicants()
  {
    return tieredReplicants;
  }

  @Override
  public int getExpectedReplicants(String tier)
  {
    return tieredReplicants.getOrDefault(tier, 0);
  }
}
