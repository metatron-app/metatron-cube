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

package io.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.JodaUtils;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 */
public class MultipleIntervalSegmentSpec implements QuerySegmentSpec
{
  public static MultipleIntervalSegmentSpec of(Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  private final List<Interval> intervals;

  @JsonCreator
  public MultipleIntervalSegmentSpec(
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "intervals=" + intervals +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MultipleIntervalSegmentSpec that = (MultipleIntervalSegmentSpec) o;

    return Objects.equals(intervals, that.intervals);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(intervals);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.appendIntervals(intervals);
  }
}
