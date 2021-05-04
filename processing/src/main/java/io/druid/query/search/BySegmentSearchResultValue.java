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

package io.druid.query.search;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.BySegmentResultValue;
import io.druid.query.Result;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class BySegmentSearchResultValue implements BySegmentResultValue<Result<SearchResultValue>>
{
  private final List<Result<SearchResultValue>> results;
  private final String segmentId;
  private final Interval interval;

  public BySegmentSearchResultValue(
      @JsonProperty("results") List<Result<SearchResultValue>> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") Interval interval
  )
  {
    this.results = results;
    this.segmentId = segmentId;
    this.interval = interval;
  }

  @Override
  @JsonProperty("results")
  public List<Result<SearchResultValue>> getResults()
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

  @JsonIgnore
  public int countAll()
  {
    return results.stream().mapToInt(r -> r.getValue().size()).sum();
  }

  @Override
  public BySegmentSearchResultValue withResult(List<Result<SearchResultValue>> result)
  {
    return new BySegmentSearchResultValue(result, getSegmentId(), getInterval());
  }

  @Override
  public String toString()
  {
    return "BySegmentSearchResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", interval='" + interval.toString() + '\'' +
           '}';
  }
}
