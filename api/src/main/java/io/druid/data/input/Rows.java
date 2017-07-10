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

package io.druid.data.input;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class Rows
{
  public static Function rowToMap(final String timestampColumn)
  {
    return new Function()
    {
      @Override
      public Map<String, Object> apply(Object input)
      {
        Row row = (Row) input;
        if (row instanceof MapBasedRow) {
          Map<String, Object> event = ((MapBasedRow) row).getEvent();
          if (MapBasedRow.supportInplaceUpdate(event)) {
            event.put(timestampColumn, row.getTimestamp());
            return event;
          }
        }
        Map<String, Object> event = Maps.newLinkedHashMap();
        for (String column : row.getColumns()) {
          event.put(column, row.getRaw(column));
        }
        event.put(timestampColumn, row.getTimestamp());
        return event;
      }
    };
  }

  public static Row.Updatable toUpdatable(Row row)
  {
    if (row instanceof Row.Updatable && ((Row.Updatable) row).isUpdatable()) {
      return (Row.Updatable) row;
    }
    return MapBasedRow.copyOf(row);
  }

  public static Row.Updatable toUpdatable(InputRow row)
  {
    if (row instanceof Row.Updatable && ((Row.Updatable) row).isUpdatable()) {
      return (Row.Updatable) row;
    }
    return MapBasedInputRow.copyOf(row);
  }

  public static InputRow toCaseInsensitiveInputRow(final Row row, final List<String> dimensions)
  {
    if (row instanceof MapBasedRow) {
      MapBasedRow mapBasedRow = (MapBasedRow) row;

      TreeMap<String, Object> caseInsensitiveMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
      caseInsensitiveMap.putAll(mapBasedRow.getEvent());
      return new MapBasedInputRow(
          mapBasedRow.getTimestamp(),
          dimensions,
          caseInsensitiveMap
      );
    }
    throw new ISE("Can only convert MapBasedRow objects because we are ghetto like that.");
  }

  /**
   * @param timeStamp rollup up timestamp to be used to create group key
   * @param inputRow input row
   * @return groupKey for the given input row
   */
  public static List<Object> toGroupKey(long timeStamp, InputRow inputRow)
  {
    final Map<String, Set<String>> dims = Maps.newTreeMap();
    for (final String dim : inputRow.getDimensions()) {
      final Set<String> dimValues = ImmutableSortedSet.copyOf(inputRow.getDimension(dim));
      if (dimValues.size() > 0) {
        dims.put(dim, dimValues);
      }
    }
    return ImmutableList.of(
        timeStamp,
        dims
    );
  }

  public static HashCode toGroupHash(Hasher hasher, long timeStamp, InputRow inputRow, List<String> partitionDimensions)
  {
    hasher.putLong(timeStamp);
    for (final String dim : partitionDimensions) {
      Object value = inputRow.getRaw(dim);
      if (value != null) {
        hasher.putBytes(StringUtils.toUtf8(String.valueOf(value)));
      }
    }
    return hasher.hash();
  }
}
