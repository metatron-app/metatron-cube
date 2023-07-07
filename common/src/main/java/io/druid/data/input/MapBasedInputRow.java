/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.common.DateTimes;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class MapBasedInputRow extends MapBasedRow implements InputRow
{
  public static MapBasedInputRow copyOf(InputRow row)
  {
    Map<String, Object> event = Maps.newLinkedHashMap();
    for (String column : row.getColumns()) {
      event.put(column, row.getRaw(column));
    }
    return new MapBasedInputRow(row.getTimestamp(), row.getDimensions(), event);
  }

  private final List<String> dimensions;

  public MapBasedInputRow(
      long timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  public MapBasedInputRow(
      DateTime timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public String toString()
  {
    return "MapBasedInputRow{" +
           "timestamp=" + DateTimes.utc(getTimestampFromEpoch()) +
           ", event=" + getEvent() +
           ", dimensions=" + dimensions +
           '}';
  }

  public static class Nested extends MapBasedInputRow {

    public Nested(long timestamp, List<String> dimensions, Map<String, Object> event)
    {
      super(timestamp, dimensions, event);
    }

    public Nested(DateTime timestamp, List<String> dimensions, Map<String, Object> event)
    {
      super(timestamp, dimensions, event);
    }

    @Override
    public Object getRaw(String column)
    {
      int ix = column.indexOf('.');
      if (ix < 0) {
        return super.getRaw(column);
      }
      int prev = 0;
      Object nested = super.getRaw(column.substring(prev, ix));
      for (ix = column.indexOf('.', prev = ix + 1); ix > 0 && nested != null;
           ix = column.indexOf('.', prev = ix + 1)) {
        nested = unnest(nested, column.substring(prev, ix));
      }
      if (nested instanceof Map) {
        return ((Map) nested).get(column.substring(prev));
      }
      return null;
    }

    private Object unnest(Object nested, String name)
    {
      if (nested instanceof Map) {
        return ((Map) nested).get(name);
      }
      if (nested instanceof List) {
        Integer x = Ints.tryParse(name);
        if (x != null) {
          return ((List) nested).get(x);
        }
      }
      return null;
    }
  }
}
