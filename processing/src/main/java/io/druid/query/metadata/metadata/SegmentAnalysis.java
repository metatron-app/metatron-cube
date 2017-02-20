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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SegmentAnalysis implements Comparable<SegmentAnalysis>
{
  private final String id;
  private final List<Interval> interval;
  private final Map<String, ColumnAnalysis> columns;
  private final long size;
  private final long serializedSize;
  private final long numRows;
  private final long ingestedNumRows;
  private final Map<String, AggregatorFactory> aggregators;
  private final QueryGranularity queryGranularity;

  @JsonCreator
  public SegmentAnalysis(
      @JsonProperty("id") String id,
      @JsonProperty("intervals") List<Interval> interval,
      @JsonProperty("columns") Map<String, ColumnAnalysis> columns,
      @JsonProperty("size") long size,
      @JsonProperty("serializedSize") long serializedSize,
      @JsonProperty("numRows") long numRows,
      @JsonProperty("ingestedNumRows") long ingestedNumRows,
      @JsonProperty("aggregators") Map<String, AggregatorFactory> aggregators,
      @JsonProperty("queryGranularity") QueryGranularity queryGranularity
  )
  {
    this.id = id;
    this.interval = interval;
    this.columns = columns;
    this.size = size;
    this.serializedSize = serializedSize;
    this.numRows = numRows;
    this.ingestedNumRows = ingestedNumRows;
    this.aggregators = aggregators;
    this.queryGranularity = queryGranularity;
  }

  public SegmentAnalysis(
      String id,
      List<Interval> interval,
      Map<String, ColumnAnalysis> columns,
      long size,
      long numRows,
      Map<String, AggregatorFactory> aggregators,
      QueryGranularity queryGranularity
  )
  {
    this(id, interval, columns, size, 0L, numRows, -1L, aggregators, queryGranularity);
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return interval;
  }

  @JsonProperty
  public Map<String, ColumnAnalysis> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  public long getSerializedSize()
  {
    return serializedSize;
  }

  @JsonProperty
  public long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public long getIngestedNumRows()
  {
    return ingestedNumRows;
  }

  @JsonProperty
  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Map<String, AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  public SegmentAnalysis withIngestedNumRows(long ingestedNumRows)
  {
    return new SegmentAnalysis(
        id,
        interval,
        columns,
        size,
        serializedSize,
        numRows,
        ingestedNumRows,
        aggregators,
        queryGranularity
    );
  }

  @Override
  public String toString()
  {
    return "SegmentAnalysis{" +
           "id='" + id + '\'' +
           ", interval=" + interval +
           ", columns=" + columns +
           ", size=" + size +
           ", serializedSize=" + serializedSize +
           ", numRows=" + numRows +
           ", ingestedNumRows=" + ingestedNumRows +
           ", aggregators=" + aggregators +
           ", queryGranularity=" + queryGranularity +
           '}';
  }

  /**
   * Best-effort equals method; relies on AggregatorFactory.equals, which is not guaranteed to be sanely implemented.
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentAnalysis that = (SegmentAnalysis) o;
    return size == that.size &&
           serializedSize == that.serializedSize &&
           numRows == that.numRows &&
           ingestedNumRows == that.ingestedNumRows &&
           Objects.equals(id, that.id) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(columns, that.columns) &&
           Objects.equals(aggregators, that.aggregators) &&
           Objects.equals(queryGranularity, that.queryGranularity);
  }

  /**
   * Best-effort hashCode method; relies on AggregatorFactory.hashCode, which is not guaranteed to be sanely
   * implemented.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(
        id,
        interval,
        columns,
        size,
        serializedSize,
        numRows,
        ingestedNumRows,
        aggregators,
        queryGranularity
    );
  }

  @Override
  public int compareTo(SegmentAnalysis rhs)
  {
    // Nulls first
    if (rhs == null) {
      return 1;
    }
    return id.compareTo(rhs.getId());
  }
}
