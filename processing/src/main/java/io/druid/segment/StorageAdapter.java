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

import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
public interface StorageAdapter extends CursorFactory
{
  String getSegmentIdentifier();
  Interval getInterval();
  Interval getTimeMinMax();
  Indexed<String> getAvailableDimensions();
  Iterable<String> getAvailableMetrics();

  int getNumRows();
  Metadata getMetadata();
  Capabilities getCapabilities();

  /**
   * Returns the number of distinct values for the given dimension column
   * -1 for not existing or non-dimensional column (including time column)
   *
   * @param column
   * @return
   */
  int getDimensionCardinality(String column);
  ColumnCapabilities getColumnCapabilities(String column);
  ColumnMeta getColumnMeta(String columnName);
  long getSerializedSize(String column);
  long getSerializedSize();

  default ValueDesc getType(String column)
  {
    return getColumnCapabilities(column) == null ? null : getColumnCapabilities(column).getTypeDesc();
  }

  default Map<String, String> getColumnDescriptor(String column)
  {
    final ColumnMeta columnMeta = getColumnMeta(column);
    return columnMeta == null ? null : columnMeta.getDescs();
  }

  default Map<String, Object> getColumnStats(String column)
  {
    final ColumnMeta columnMeta = getColumnMeta(column);
    return columnMeta == null ? null : columnMeta.getStats();
  }
}
