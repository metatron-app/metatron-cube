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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 */
public class MapBasedRow extends AbstractRow implements Row.Updatable
{
  public static MapBasedRow copyOf(Row row)
  {
    Map<String, Object> event = Maps.newLinkedHashMap();
    for (String column : row.getColumns()) {
      event.put(column, row.getRaw(column));
    }
    return new MapBasedRow(row.getTimestamp(), event);
  }

  public static boolean supportInplaceUpdate(Map event)
  {
    Class<? extends Map> clazz = event.getClass();
    return clazz == HashMap.class || clazz == LinkedHashMap.class || clazz == TreeMap.class;
  }

  public static Map<String, Object> toUpdatable(Map<String, Object> event)
  {
    return supportInplaceUpdate(event) ? event : Maps.newLinkedHashMap(event);
  }

  private final DateTime timestamp;
  private final Map<String, Object> event;

  @JsonCreator
  public MapBasedRow(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("event") Map<String, Object> event
  )
  {
    this.timestamp = timestamp;
    this.event = event;
  }

  public MapBasedRow(long timestamp, Map<String, Object> event)
  {
    this(DateTimes.utc(timestamp), event);
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return timestamp == null ? JodaUtils.MIN_INSTANT : timestamp.getMillis();
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Map<String, Object> getEvent()
  {
    return event;
  }

  @Override
  public Object getRaw(String dimension)
  {
    return event.get(dimension);    // todo: returning timestamp for '__time' makes it regarded as a dimension, failing some tests
  }

  @Override
  public Collection<String> getColumns()
  {
    return event.keySet();
  }

  @Override
  public boolean isUpdatable()
  {
    return supportInplaceUpdate(event);
  }

  @Override
  public void set(String column, Object value)
  {
    event.put(column, value);
  }

  @Override
  public Object remove(String column)
  {
    return event.remove(column);
  }

  public MapBasedRow withDateTime(DateTime dateTime)
  {
    return new MapBasedRow(dateTime, event);
  }

  @Override
  public String toString()
  {
    return "MapBasedRow{" +
           "timestamp=" + timestamp +
           ", event=" + event +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MapBasedRow that = (MapBasedRow) o;

    if (!event.equals(that.event)) {
      return false;
    }
    if (!Objects.equals(timestamp, that.timestamp)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(event, timestamp);
  }

  @Override
  public int compareTo(Row o)
  {
    return Long.compare(getTimestampFromEpoch(), o.getTimestampFromEpoch());
  }
}
