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

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 * An adapter to an index
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  Indexed<String> getDimensionNames();

  Indexed<String> getMetricNames();

  Indexed<String> getDimValueLookup(String dimension);

  Iterable<Rowboat> getRows(int indexNum);

  Iterable<Rowboat> getRows(int indexNum, List<String> mergedDimensions, List<String> mergedMetrics);

  @Nullable
  ImmutableBitmap getBitmap(String dimension, int dictId);

  ValueDesc getMetricType(String metric);

  ColumnCapabilities getCapabilities(String column);

  Metadata getMetadata();
}
