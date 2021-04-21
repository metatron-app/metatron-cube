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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

public class BySegmentSchemaValue implements BySegmentResultValue<Schema>
{
  private final Schema schema;    // by segment, per se
  private final String segmentId;
  private final Interval interval;

  public BySegmentSchemaValue(
      @JsonProperty("results") Schema schema,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") Interval interval
  )
  {
    this.schema = schema;
    this.segmentId = segmentId;
    this.interval = interval;
  }

  @Override
  @JsonIgnore
  public List<Schema> getResults()
  {
    return Arrays.asList(schema);
  }

  @JsonProperty("results")
  public Schema getSchema()
  {
    return schema;
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
  public BySegmentResultValue<Schema> withResult(List<Schema> result)
  {
    return new BySegmentSchemaValue(Iterables.getOnlyElement(result), segmentId, interval);
  }

  @Override
  public String toString()
  {
    return "BySegmentSchemaValue{" +
           "results=" + schema +
           ", segmentId='" + segmentId + '\'' +
           (interval == null ? ""  : ", interval='" + interval.toString() + '\'') +
           '}';
  }
}
