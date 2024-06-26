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
package io.druid.data.input.avro;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.input.InputRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericRecordAsMap implements Map<String, Object>
{
  private final GenericRecord record;
  private final boolean fromPigAvroStorage;
  private final Map<String, Schema.Field> schema = Maps.newHashMap();
  private final Map<String, Object> partitions;

  private static final Function<Object, String> PIG_AVRO_STORAGE_ARRAY_TO_STRING_INCLUDING_NULL = new Function<Object, String>()
  {
    @Nullable
    @Override
    public String apply(Object input)
    {
      return String.valueOf(((GenericRecord) input).get(0));
    }
  };

  public GenericRecordAsMap(GenericRecord record, boolean fromPigAvroStorage)
  {
    this.record = record;
    this.fromPigAvroStorage = fromPigAvroStorage;
    for (Schema.Field field : record.getSchema().getFields()) {
      schema.put(field.name(), field);
    }
    Map<String, Object> partitions = InputRow.CURRENT_PARTITION.get();
    if (partitions != null && !partitions.isEmpty()) {
      for (String key : schema.keySet()) {
        partitions.remove(key);
      }
    }
    this.partitions = partitions == null ? Collections.<String, Object>emptyMap() : partitions;
  }

  @Override
  public int size()
  {
    return schema.size() + partitions.size();
  }

  @Override
  public boolean isEmpty()
  {
    return schema.isEmpty() && partitions.isEmpty();
  }

  @Override
  public boolean containsKey(Object key)
  {
    return schema.containsKey(key) || partitions.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * When used in MapBasedRow, field in GenericRecord will be interpret as follows:
   * <ul>
   * <li> avro schema type -> druid dimension:</li>
   * <ul>
   * <li>null, boolean, int, long, float, double, string, Records, Enums, Maps, Fixed -> String, using String.valueOf</li>
   * <li>bytes -> Arrays.toString() </li>
   * <li>Arrays -> List&lt;String&gt;, using Lists.transform(&lt;List&gt;dimValue, TO_STRING_INCLUDING_NULL)</li>
   * </ul>
   * <li> avro schema type -> druid metric:</li>
   * <ul>
   * <li>null -> 0F/0L</li>
   * <li>int, long, float, double -> Float/Long, using Number.floatValue()/Number.longValue()</li>
   * <li>string -> Float/Long, using Float.valueOf()/Long.valueOf()</li>
   * <li>boolean, bytes, Arrays, Records, Enums, Maps, Fixed -> ParseException</li>
   * </ul>
   * </ul>
   */
  @Override
  public Object get(Object key)
  {
    Object field = partitions.get(key);
    if (field != null) {
      return field;
    }
    field = record.get(key.toString());
    if (fromPigAvroStorage && field instanceof GenericData.Array) {
      return Lists.transform((List) field, PIG_AVRO_STORAGE_ARRAY_TO_STRING_INCLUDING_NULL);
    }
    if (field instanceof ByteBuffer) {
      return Arrays.toString(((ByteBuffer) field).array());
    }
    return field;
  }

  @Override
  public Object put(String key, Object value)
  {
    final Object prev = record.get(key);
    record.put(key, value);
    return prev;
  }

  @Override
  public Object remove(Object key)
  {
    if (key instanceof String) {
      final String k = (String) key;
      final Object prev = record.get(k);
      record.put(k, null);
      return prev;
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends String, ?> m)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet()
  {
    return Sets.union(schema.keySet(), partitions.keySet());
  }

  @Override
  public Collection<Object> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<String, Object>> entrySet()
  {
    throw new UnsupportedOperationException();
  }
}
