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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.query.BySegmentResultValue;
import io.druid.query.Result;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class BySegmentTopNResultValue extends TopNResultValue implements BySegmentResultValue<Result<TopNResultValue>>
{
  private final List<Result<TopNResultValue>> results;
  private final String segmentId;
  private final Interval interval;

  @JsonCreator
  public BySegmentTopNResultValue(
      @JsonProperty("results") List<Result<TopNResultValue>> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") Interval interval
  )
  {
    super(null);

    this.results = results;
    this.segmentId = segmentId;
    this.interval = interval;
  }

  @Override
  @JsonValue(false)
  public List<Map<String, Object>> getValue()
  {
    throw new UnsupportedOperationException();
  }


  @Override
  @JsonProperty("results")
  public List<Result<TopNResultValue>> getResults()
  {
    return results;
  }

  @Override
  @JsonProperty("segment")
  public String getSegmentId()
  {
    return segmentId;
  }

  @Override
  @JsonProperty("interval")
  public Interval getInterval()
  {
    return interval;
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
    if (!super.equals(o)) {
      return false;
    }
    BySegmentTopNResultValue maps = (BySegmentTopNResultValue) o;
    return Objects.equals(results, maps.results) &&
           Objects.equals(segmentId, maps.segmentId) &&
           Objects.equals(interval, maps.interval);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(super.hashCode(), results, segmentId, interval);
  }

  @Override
  public String toString()
  {
    return "BySegmentTopNResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", interval='" + interval.toString() + '\'' +
           '}';
  }
}
