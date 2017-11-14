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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

public class UniformGranularitySpec implements GranularitySpec
{
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = QueryGranularities.DAY;
  private static final Granularity DEFAULT_QUERY_GRANULARITY = QueryGranularities.NONE;

  private final Granularity segmentGranularity;
  private final Granularity queryGranularity;
  private final Boolean rollup;
  private final Boolean append;
  private final List<Interval> intervals;

  private transient ArbitraryGranularitySpec normalized;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("append") Boolean append,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.segmentGranularity = segmentGranularity == null ? DEFAULT_SEGMENT_GRANULARITY : segmentGranularity;
    this.queryGranularity = queryGranularity == null ? DEFAULT_QUERY_GRANULARITY : queryGranularity;
    this.rollup = rollup == null ? Boolean.TRUE : rollup;
    this.append = append == null ? Boolean.FALSE: append;
    this.intervals = intervals;
  }

  public UniformGranularitySpec(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> inputIntervals
  )
  {
    this(segmentGranularity, queryGranularity, true, false, inputIntervals);
  }

  @Override
  public Optional<SortedSet<Interval>> bucketIntervals()
  {
    if (intervals == null) {
      return Optional.absent();
    } else {
      return normalize().bucketIntervals();
    }
  }

  private ArbitraryGranularitySpec normalize()
  {
    if (normalized == null) {
      List<Interval> granularIntervals = Lists.newArrayList();
      for (Interval inputInterval : intervals) {
        Iterables.addAll(granularIntervals, segmentGranularity.getIterable(inputInterval));
      }
      normalized = new ArbitraryGranularitySpec(
          queryGranularity,
          rollup,
          append,
          ImmutableList.copyOf(granularIntervals)
      );
    }
    return normalized;
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    if (intervals == null) {
      return Optional.absent();
    } else {
      return normalize().bucketInterval(dt);
    }
  }

  @Override
  @JsonProperty("segmentGranularity")
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Override
  @JsonProperty("rollup")
  public boolean isRollup()
  {
    return rollup;
  }

  @Override
  @JsonProperty("append")
  public boolean isAppending()
  {
    return append;
  }

  @Override
  @JsonProperty("queryGranularity")
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
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

    UniformGranularitySpec that = (UniformGranularitySpec) o;

    if (segmentGranularity != that.segmentGranularity) {
      return false;
    }
    if (!queryGranularity.equals(that.queryGranularity)) {
      return false;
    }
    if (!rollup.equals(that.rollup)) {
      return false;
    }
    return Objects.equals(intervals, that.intervals);

  }

  @Override
  public int hashCode()
  {
    int result = segmentGranularity.hashCode();
    result = 31 * result + queryGranularity.hashCode();
    result = 31 * result + rollup.hashCode();
    result = 31 * result + (intervals != null ? intervals.hashCode() : 0);
    return result;
  }
}
