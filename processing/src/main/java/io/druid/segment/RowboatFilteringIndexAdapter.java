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

package io.druid.segment;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class RowboatFilteringIndexAdapter implements IndexableAdapter
{
  private final IndexableAdapter baseAdapter;
  private final Predicate<Rowboat> filter;

  public RowboatFilteringIndexAdapter(IndexableAdapter baseAdapter, Predicate<Rowboat> filter)
  {
    this.baseAdapter = baseAdapter;
    this.filter = filter;
  }

  @Override
  public Interval getInterval()
  {
    return baseAdapter.getInterval();
  }

  @Override
  public int getNumRows()
  {
    return baseAdapter.getNumRows();
  }

  @Override
  public Indexed<String> getDimensionNames()
  {
    return baseAdapter.getDimensionNames();
  }

  @Override
  public Indexed<String> getMetricNames()
  {
    return baseAdapter.getMetricNames();
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    return baseAdapter.getDimValueLookup(dimension);
  }

  @Override
  public Iterable<Rowboat> getRows(List<String> mergedDimensions, List<String> mergedMetrics)
  {
    return Iterables.filter(baseAdapter.getRows(mergedDimensions, mergedMetrics), filter);
  }

  @Override
  public ValueDesc getMetricType(String metric)
  {
    return baseAdapter.getMetricType(metric);
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return baseAdapter.getCapabilities(column);
  }

  @Override
  public BitmapProvider getBitmaps(String dimension)
  {
    return baseAdapter.getBitmaps(dimension);
  }

  @Override
  public Metadata getMetadata()
  {
    return baseAdapter.getMetadata();
  }
}
