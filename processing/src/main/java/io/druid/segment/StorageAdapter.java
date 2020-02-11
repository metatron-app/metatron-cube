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

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
public interface StorageAdapter extends CursorFactory
{
  String getSegmentIdentifier();
  Interval getInterval();
  Indexed<String> getAvailableDimensions();
  Iterable<String> getAvailableMetrics();

  DateTime getMinTime();
  DateTime getMaxTime();

  int getNumRows();
  Metadata getMetadata();
  Capabilities getCapabilities();
  DateTime getMaxIngestedEventTime();

  /**
   * Returns the number of distinct values for the given dimension column
   * For dimensions of unknown cardinality, e.g. __time this currently returns
   * Integer.MAX_VALUE
   *
   * @param column
   * @return
   */
  int getDimensionCardinality(String column);
  ColumnCapabilities getColumnCapabilities(String column);
  Map<String, String> getColumnDescriptor(String column);
  long getSerializedSize(String column);
  float getAverageSize(String column);
}
