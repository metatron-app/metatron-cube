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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class GroupByQueryConfig
{
  @JsonProperty
  private boolean singleThreaded = false;

  @JsonProperty
  private int maxIntermediateRows = 50000;

  @JsonProperty
  private int maxResults = 500000;

  @JsonProperty
  private boolean sortOnTime = true;

  @JsonProperty
  private int maxMergeParallelism = 8;

  @JsonProperty
  private boolean convertTimeseries = false;

  @JsonProperty
  private boolean limitPushdown = false;

  @JsonProperty
  private int limitPushdownThreshold = maxIntermediateRows << 2;

  @JsonProperty
  private boolean mergeSimple = true; // todo

  @JsonProperty
  private boolean compactTransfer = false;

  public boolean isSingleThreaded()
  {
    return singleThreaded;
  }

  public void setSingleThreaded(boolean singleThreaded)
  {
    this.singleThreaded = singleThreaded;
  }

  public int getMaxIntermediateRows()
  {
    return maxIntermediateRows;
  }

  public void setMaxIntermediateRows(int maxIntermediateRows)
  {
    this.maxIntermediateRows = maxIntermediateRows;
  }

  public int getMaxResults()
  {
    return maxResults;
  }

  public void setMaxResults(int maxResults)
  {
    this.maxResults = maxResults;
  }

  public boolean isSortOnTime()
  {
    return sortOnTime;
  }

  public void setSortOnTime(boolean sortOnTime)
  {
    this.sortOnTime = sortOnTime;
  }

  public int getMaxMergeParallelism()
  {
    return maxMergeParallelism;
  }

  public void setMaxMergeParallelism(int maxMergeParallelism)
  {
    this.maxMergeParallelism = maxMergeParallelism;
  }

  public boolean isConvertTimeseries()
  {
    return convertTimeseries;
  }

  public void setConvertTimeseries(boolean convertTimeseries)
  {
    this.convertTimeseries = convertTimeseries;
  }

  public boolean isLimitPushdown()
  {
    return limitPushdown;
  }

  public void setLimitPushdown(boolean limitPushdown)
  {
    this.limitPushdown = limitPushdown;
  }

  public int getLimitPushdownThreshold()
  {
    return limitPushdownThreshold;
  }

  public void setLimitPushdownThreshold(int limitPushdownThreshold)
  {
    this.limitPushdownThreshold = limitPushdownThreshold;
  }

  public boolean isMergeSimple()
  {
    return mergeSimple;
  }

  public void setMergeSimple(boolean mergeSimple)
  {
    this.mergeSimple = mergeSimple;
  }

  public boolean isCompactTransfer()
  {
    return compactTransfer;
  }

  public void setCompactTransfer(boolean compactTransfer)
  {
    this.compactTransfer = compactTransfer;
  }
}
