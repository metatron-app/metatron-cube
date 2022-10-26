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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.Min;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class CoordinatorDynamicConfig
{
  public static final CoordinatorDynamicConfig DEFAULT = new Builder().build();

  public static final String CONFIG_KEY = "coordinator.config";

  @JsonProperty
  private long millisToWaitBeforeDeleting = 15 * 60 * 1000L;

  @JsonProperty
  private long mergeBytesLimit = 524288000L;

  @JsonProperty
  private int mergeSegmentsLimit = 100;

  @JsonProperty
  private int mergeTaskLimit = 0; // unlimited

  @JsonProperty
  private int maxSegmentsToMove = -1;

  @JsonProperty
  private int maxPendingSegmentsToLoad = -1;

  @JsonProperty
  private int replicantLifetime = 10;

  @JsonProperty
  private int balancerComputeThreads = 1;

  @JsonProperty
  private boolean emitBalancingStats;

  @JsonProperty
  private Set<String> killDataSourceWhitelist;


  // The pending segments of the dataSources in this list are not killed.
  @JsonProperty
  private Set<String> killPendingSegmentsSkipList;

  @JsonProperty
  @Min(1)
  private int minimumServersForCoordination = 1;

  public CoordinatorDynamicConfig()
  {
    this(15 * 60 * 1000L, 524288000L, 100, 0, -1, -1, 15, 10, 1, false, null, null, null);
  }

  @JsonCreator
  public CoordinatorDynamicConfig(
      @JsonProperty("millisToWaitBeforeDeleting") long millisToWaitBeforeDeleting,
      @JsonProperty("mergeBytesLimit") long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") int mergeSegmentsLimit,
      @JsonProperty("mergeTaskLimit") int mergeTaskLimit,
      @JsonProperty("maxSegmentsToMove") int maxSegmentsToMove,
      @JsonProperty("maxPendingSegmentsToLoad") int maxPendingSegmentsToLoad,
      @JsonProperty("replicantLifetime") int replicantLifetime,
      @JsonProperty("replicationThrottleLimit") int replicationThrottleLimit,
      @JsonProperty("balancerComputeThreads") int balancerComputeThreads,
      @JsonProperty("emitBalancingStats") boolean emitBalancingStats,

      // Type is Object here so that we can support both string and list as
      // coordinator console can not send array of strings in the update request.
      // See https://github.com/druid-io/druid/issues/3055
      @JsonProperty("killDataSourceWhitelist") Object killDataSourceWhitelist,
      @JsonProperty("killPendingSegmentsSkipList") Object killPendingSegmentsSkipList,
      @JsonProperty("minimumServersForCoordination") Integer minimumServersForCoordination
  )
  {
    this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeTaskLimit = mergeTaskLimit;
    this.maxSegmentsToMove = maxSegmentsToMove;
    this.maxPendingSegmentsToLoad = maxPendingSegmentsToLoad;
    this.replicantLifetime = replicantLifetime;
    this.emitBalancingStats = emitBalancingStats;
    this.balancerComputeThreads = Math.max(balancerComputeThreads, 1);

    this.killDataSourceWhitelist = parseJsonStringOrArray(killDataSourceWhitelist);
    this.killPendingSegmentsSkipList = parseJsonStringOrArray(killPendingSegmentsSkipList);
    if (minimumServersForCoordination != null && minimumServersForCoordination > 0) {
      this.minimumServersForCoordination = minimumServersForCoordination;
    }
  }

  private static Set<String> parseJsonStringOrArray(Object jsonStringOrArray)
  {
    if (jsonStringOrArray instanceof String) {
      String[] list = ((String) jsonStringOrArray).split(",");
      Set<String> result = new HashSet<>();
      for (String item : list) {
        String trimmed = item.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }
      return result;
    } else if (jsonStringOrArray instanceof Collection) {
      return ImmutableSet.copyOf(((Collection) jsonStringOrArray));
    } else {
      return ImmutableSet.of();
    }
  }

  @JsonProperty
  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  @JsonProperty
  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  @JsonProperty
  public boolean emitBalancingStats()
  {
    return emitBalancingStats;
  }

  @JsonProperty
  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
  }

  @JsonProperty
  public int getMergeTaskLimit()
  {
    return mergeTaskLimit;
  }

  @JsonProperty
  public int getMaxSegmentsToMove()
  {
    return maxSegmentsToMove;
  }

  @JsonProperty
  public int getMaxPendingSegmentsToLoad()
  {
    return maxPendingSegmentsToLoad;
  }

  @JsonProperty
  public int getReplicantLifetime()
  {
    return replicantLifetime;
  }

  @JsonProperty
  public int getBalancerComputeThreads()
  {
    return balancerComputeThreads;
  }

  @JsonProperty
  public Set<String> getKillDataSourceWhitelist()
  {
    return killDataSourceWhitelist;
  }

  @JsonProperty
  public Set<String> getKillPendingSegmentsSkipList()
  {
    return killPendingSegmentsSkipList;
  }

  @JsonProperty
  public int getMinimumServersForCoordination()
  {
    return minimumServersForCoordination;
  }

  @Override
  public String toString()
  {
    return "CoordinatorDynamicConfig{" +
           "millisToWaitBeforeDeleting=" + millisToWaitBeforeDeleting +
           ", mergeBytesLimit=" + mergeBytesLimit +
           ", mergeSegmentsLimit=" + mergeSegmentsLimit +
           ", mergeTaskLimit=" + mergeTaskLimit +
           ", maxSegmentsToMove=" + maxSegmentsToMove +
           ", maxPendingSegmentsToLoad=" + maxPendingSegmentsToLoad +
           ", replicantLifetime=" + replicantLifetime +
           ", balancerComputeThreads=" + balancerComputeThreads +
           ", emitBalancingStats=" + emitBalancingStats +
           ", killDataSourceWhitelist=" + killDataSourceWhitelist +
           ", minimumServersForCoordination=" + minimumServersForCoordination +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoordinatorDynamicConfig that = (CoordinatorDynamicConfig) o;

    if (millisToWaitBeforeDeleting != that.millisToWaitBeforeDeleting) {
      return false;
    }
    if (mergeBytesLimit != that.mergeBytesLimit) {
      return false;
    }
    if (mergeSegmentsLimit != that.mergeSegmentsLimit) {
      return false;
    }
    if (mergeTaskLimit != that.mergeTaskLimit) {
      return false;
    }
    if (maxSegmentsToMove != that.maxSegmentsToMove) {
      return false;
    }
    if (maxPendingSegmentsToLoad != that.maxPendingSegmentsToLoad) {
      return false;
    }
    if (replicantLifetime != that.replicantLifetime) {
      return false;
    }
    if (balancerComputeThreads != that.balancerComputeThreads) {
      return false;
    }
    if (minimumServersForCoordination != that.minimumServersForCoordination) {
      return false;
    }
    if (emitBalancingStats != that.emitBalancingStats) {
      return false;
    }
    if (!Objects.equals(killDataSourceWhitelist, that.killDataSourceWhitelist)) {
      return false;
    }

    return Objects.equals(killPendingSegmentsSkipList, that.killPendingSegmentsSkipList);
  }

  @Override
  public int hashCode()
  {
    int result = (int) (millisToWaitBeforeDeleting ^ (millisToWaitBeforeDeleting >>> 32));
    result = 31 * result + (int) (mergeBytesLimit ^ (mergeBytesLimit >>> 32));
    result = 31 * result + mergeSegmentsLimit;
    result = 31 * result + mergeTaskLimit;
    result = 31 * result + maxSegmentsToMove;
    result = 31 * result + maxPendingSegmentsToLoad;
    result = 31 * result + replicantLifetime;
    result = 31 * result + balancerComputeThreads;
    result = 31 * result + minimumServersForCoordination;
    result = 31 * result + (emitBalancingStats ? 1 : 0);
    result = 31 * result + (killDataSourceWhitelist != null ? killDataSourceWhitelist.hashCode() : 0);
    result = 31 * result + (killPendingSegmentsSkipList != null ? killPendingSegmentsSkipList.hashCode() : 0);
    return result;
  }

  public static class Builder
  {
    private long millisToWaitBeforeDeleting;
    private long mergeBytesLimit;
    private int mergeSegmentsLimit;
    private int maxSegmentsToMove;
    private int maxPendingSegmentsToLoad;
    private int mergeTaskLimit;
    private int replicantLifetime;
    private int replicationThrottleLimit;
    private boolean emitBalancingStats;
    private int balancerComputeThreads;
    private Set<String> killDataSourceWhitelist;
    private Object killPendingSegmentsSkipList;
    private Integer minimumServersForCoordination;

    public Builder()
    {
      this(15 * 60 * 1000L, 524288000L, 100, 5, -1, 15, 10, 1, false, null, null);
    }

    private Builder(
        long millisToWaitBeforeDeleting,
        long mergeBytesLimit,
        int mergeSegmentsLimit,
        int maxSegmentsToMove,
        int maxPendingSegmentsToLoad,
        int replicantLifetime,
        int replicationThrottleLimit,
        int balancerComputeThreads,
        boolean emitBalancingStats,
        Set<String> killDataSourceWhitelist,
        Object killPendingSegmentsSkipList
    )
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.maxPendingSegmentsToLoad = maxPendingSegmentsToLoad;
      this.replicantLifetime = replicantLifetime;
      this.replicationThrottleLimit = replicationThrottleLimit;
      this.emitBalancingStats = emitBalancingStats;
      this.balancerComputeThreads = balancerComputeThreads;
      this.killDataSourceWhitelist = killDataSourceWhitelist;
      this.killPendingSegmentsSkipList = killPendingSegmentsSkipList;
    }

    public Builder withMillisToWaitBeforeDeleting(long millisToWaitBeforeDeleting)
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      return this;
    }

    public Builder withMergeBytesLimit(long mergeBytesLimit)
    {
      this.mergeBytesLimit = mergeBytesLimit;
      return this;
    }

    public Builder withMergeSegmentsLimit(int mergeSegmentsLimit)
    {
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      return this;
    }

    public Builder withMergeTaskLimit(int mergeTaskLimit)
    {
      this.mergeTaskLimit = mergeTaskLimit;
      return this;
    }

    public Builder withMaxSegmentsToMove(int maxSegmentsToMove)
    {
      this.maxSegmentsToMove = maxSegmentsToMove;
      return this;
    }

    public Builder withMaxPendingSegmentsToLoad(int maxPendingSegmentsToLoad)
    {
      this.maxPendingSegmentsToLoad = maxPendingSegmentsToLoad;
      return this;
    }

    public Builder withReplicantLifetime(int replicantLifetime)
    {
      this.replicantLifetime = replicantLifetime;
      return this;
    }

    public Builder withReplicationThrottleLimit(int replicationThrottleLimit)
    {
      this.replicationThrottleLimit = replicationThrottleLimit;
      return this;
    }

    public Builder withBalancerComputeThreads(int balancerComputeThreads)
    {
      this.balancerComputeThreads = balancerComputeThreads;
      return this;
    }

    public Builder withKillDataSourceWhitelist(Set<String> killDataSourceWhitelist)
    {
      this.killDataSourceWhitelist = killDataSourceWhitelist;
      return this;
    }

    public CoordinatorDynamicConfig build()
    {
      return new CoordinatorDynamicConfig(
          millisToWaitBeforeDeleting,
          mergeBytesLimit,
          mergeSegmentsLimit,
          mergeTaskLimit,
          maxSegmentsToMove,
          maxPendingSegmentsToLoad,
          replicantLifetime,
          replicationThrottleLimit,
          balancerComputeThreads,
          emitBalancingStats,
          killDataSourceWhitelist,
          killPendingSegmentsSkipList,
          minimumServersForCoordination
      );
    }
  }
}
