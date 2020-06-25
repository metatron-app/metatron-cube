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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.collections.String2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

import java.util.Map;
import java.util.function.Function;

/**
 */
public class CoordinatorStats implements Function<String, String2LongMap>
{
  public static final CoordinatorStats EMPTY = new CoordinatorStats(ImmutableMap.of(), new String2LongMap());

  private final Map<String, String2LongMap> perTierStats;
  private final String2LongMap globalStats;

  public CoordinatorStats()
  {
    this(Maps.newHashMap(), new String2LongMap());
  }

  public CoordinatorStats(
      Map<String, String2LongMap> perTierStats,
      String2LongMap globalStats
  )
  {
    this.perTierStats = perTierStats;
    this.globalStats = globalStats;
  }

  public Map<String, String2LongMap> getPerTierStats()
  {
    return perTierStats;
  }

  public String2LongMap getGlobalStats()
  {
    return globalStats;
  }

  public void addToTieredStat(String statName, String tier, long value)
  {
    perTierStats.computeIfAbsent(statName, this).addTo(tier, value);
  }

  public void addToGlobalStat(String statName, long value)
  {
    globalStats.addTo(statName, value);
  }

  public CoordinatorStats accumulate(CoordinatorStats stats)
  {
    for (Map.Entry<String, String2LongMap> entry : stats.perTierStats.entrySet()) {
      String statName = entry.getKey();
      String2LongMap theStat = perTierStats.computeIfAbsent(statName, this);
      for (Object2LongMap.Entry<String> tiers : entry.getValue().object2LongEntrySet()) {
        theStat.addTo(tiers.getKey(), tiers.getLongValue());
      }
    }
    for (Object2LongMap.Entry<String> entry : stats.globalStats.object2LongEntrySet()) {
      globalStats.addTo(entry.getKey(), entry.getLongValue());
    }
    return this;
  }

  @Override
  public String2LongMap apply(String s)
  {
    return new String2LongMap();
  }
}
