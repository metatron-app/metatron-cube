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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class JoinQueryConfig
{
  @JsonProperty
  private int maxOutputRow = 1_000_000;

  @JsonProperty
  private int hashJoinThreshold = 300_000;

  @JsonProperty
  private int semiJoinThreshold = 20_000;

  @JsonProperty
  private int broadcastJoinThreshold = 5_000;

  @JsonProperty
  private int bloomFilterThreshold = 500_000;

  @JsonProperty
  private int forcedFilterHugeThreshold = 10_000_000;

  @JsonProperty
  private int forcedFilterTinyThreshold = 10_000;

  public int getMaxOutputRow()
  {
    return maxOutputRow;
  }

  public int getMaxOutputRow(int userConf)
  {
    return maxOutputRow <= 0 ? userConf : userConf <= 0 ? maxOutputRow : Math.min(maxOutputRow, userConf);
  }

  public void setMaxOutputRow(int maxOutputRow)
  {
    this.maxOutputRow = maxOutputRow;
  }

  public int getHashJoinThreshold()
  {
    return hashJoinThreshold;
  }

  public void setHashJoinThreshold(int hashJoinThreshold)
  {
    this.hashJoinThreshold = hashJoinThreshold;
  }

  public int getSemiJoinThreshold()
  {
    return semiJoinThreshold;
  }

  public void setSemiJoinThreshold(int semiJoinThreshold)
  {
    this.semiJoinThreshold = semiJoinThreshold;
  }

  public int getBroadcastJoinThreshold()
  {
    return broadcastJoinThreshold;
  }

  public void setBroadcastJoinThreshold(int broadcastJoinThreshold)
  {
    this.broadcastJoinThreshold = broadcastJoinThreshold;
  }

  public int anyMinThreshold()
  {
    int threshold = -1;
    if (hashJoinThreshold > 0) {
      threshold = hashJoinThreshold;
    }
    if (semiJoinThreshold > 0) {
      threshold = threshold < 0 ? semiJoinThreshold : Math.min(threshold, semiJoinThreshold);
    }
    if (broadcastJoinThreshold > 0) {
      threshold = threshold < 0 ? broadcastJoinThreshold : Math.min(threshold, broadcastJoinThreshold);
    }
    return threshold;
  }

  public int getBloomFilterThreshold()
  {
    return bloomFilterThreshold;
  }

  public void setBloomFilterThreshold(int bloomFilterThreshold)
  {
    this.bloomFilterThreshold = bloomFilterThreshold;
  }

  public int getForcedFilterHugeThreshold()
  {
    return forcedFilterHugeThreshold;
  }

  public void setForcedFilterHugeThreshold(int forcedFilterHugeThreshold)
  {
    this.forcedFilterHugeThreshold = forcedFilterHugeThreshold;
  }

  public int getForcedFilterTinyThreshold()
  {
    return forcedFilterTinyThreshold;
  }

  public void setForcedFilterTinyThreshold(int forcedFilterTinyThreshold)
  {
    this.forcedFilterTinyThreshold = forcedFilterTinyThreshold;
  }
}
