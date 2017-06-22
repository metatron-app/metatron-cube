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

package io.druid.segment;

import com.google.common.collect.Maps;
import io.druid.math.expr.ExprType;
import io.druid.segment.data.Indexed;

import java.util.Map;

/**
 */
public class Segments
{
  public static Indexed<String> getAvailableDimensions(Segment segment)
  {
    if (segment.asQueryableIndex(false) != null) {
      return segment.asQueryableIndex(false).getAvailableDimensions();
    }
    return segment.asStorageAdapter(false).getAvailableDimensions();
  }

  public static int getNumRows(Segment segment)
  {
    if (segment.asQueryableIndex(false) != null) {
      return segment.asQueryableIndex(false).getNumRows();
    }
    return segment.asStorageAdapter(false).getNumRows();
  }

  public static Map<String, String> toTypeMap(Segment segment, VirtualColumns virtualColumns)
  {
    StorageAdapter adapter = segment.asStorageAdapter(false);
    Map<String, String> suppliers = Maps.newLinkedHashMap();
    for (String dimension : adapter.getAvailableDimensions()) {
      suppliers.put(dimension, ExprType.STRING.name());
    }
    for (String metric : adapter.getAvailableMetrics()) {
      suppliers.put(metric, adapter.getColumnTypeName(metric));
    }
    for (VirtualColumn virtualColumn : virtualColumns) {
      virtualColumns.resolveTypes(suppliers, virtualColumn);
    }
    return suppliers;
  }
}
