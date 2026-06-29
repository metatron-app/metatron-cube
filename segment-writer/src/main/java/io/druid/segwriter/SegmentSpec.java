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

package io.druid.segwriter;

import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;

/**
 * Describes the segment to build: its datasource, dimensions, metric aggregators, roll-up
 * granularity, and which column in each input row holds the timestamp (epoch millis).
 */
public class SegmentSpec
{
  private final String dataSource;
  private final List<String> dimensions;
  private final AggregatorFactory[] metrics;
  private final Granularity queryGranularity;
  private final boolean rollup;
  private final String timestampColumn;

  public SegmentSpec(
      String dataSource,
      List<String> dimensions,
      AggregatorFactory[] metrics,
      Granularity queryGranularity,
      boolean rollup,
      String timestampColumn
  )
  {
    this.dataSource = dataSource;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.queryGranularity = queryGranularity;
    this.rollup = rollup;
    this.timestampColumn = timestampColumn;
  }

  public String getDataSource() { return dataSource; }
  public List<String> getDimensions() { return dimensions; }
  public AggregatorFactory[] getMetrics() { return metrics; }
  public Granularity getQueryGranularity() { return queryGranularity; }
  public boolean isRollup() { return rollup; }
  public String getTimestampColumn() { return timestampColumn; }
}
