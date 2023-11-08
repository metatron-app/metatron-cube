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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnSerializer;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.MapColumnPartSerde;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MapColumnSerializer implements MetricColumnSerializer
{
  public static MapColumnSerializer create(
      String metric,
      ValueDesc type,
      CompressionStrategy compression,
      BitmapSerdeFactory bitmap
  ) throws IOException
  {
    Preconditions.checkArgument(type.isMap());
    String[] description = TypeUtils.splitDescriptiveType(type);
    if (description == null || description.length < 3) {
      throw new IAE("", type);
    }
    ValueDesc valueType = ValueDesc.of(description[2]);
    ComplexMetricSerde serde = new ComplexMetricSerde()
    {
      @Override
      public ValueDesc getType()
      {
        return ValueDesc.ofArray(valueType);
      }

      @Override
      public ObjectStrategy getObjectStrategy()
      {
        return ObjectStrategy.RAW;
      }
    };
    MetricColumnSerializer keys = TagColumnSerializer.create(metric + ".k", ValueDesc.STRING, compression, bitmap);
    MetricColumnSerializer values = ComplexColumnSerializer.create(metric + ".v", serde, null, null, compression);
    Preconditions.checkArgument(valueType.isPrimitiveNumeric() || valueType.isBoolean());
    return new MapColumnSerializer(keys, values, valueType);
  }

  private final ValueType valueType;

  private final MetricColumnSerializer keys;
  private final MetricColumnSerializer values;

  public MapColumnSerializer(MetricColumnSerializer keys, MetricColumnSerializer values, ValueDesc valueType)
  {
    this.keys = keys;
    this.values = values;
    this.valueType = valueType.type();
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    keys.open(ioPeon);
    values.open(ioPeon);
  }

  private final List<String> kscratch = Lists.newArrayList();
  private final BytesOutputStream vscratch = new BytesOutputStream();

  @Override
  @SuppressWarnings("unchecked")
  public void serialize(int rowNum, Object aggs) throws IOException
  {
    kscratch.clear();
    vscratch.position(Short.BYTES);
    if (aggs instanceof Map) {
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) aggs).entrySet()) {
        Object v = valueType.castIfPossible(entry.getValue());
        if (v == null) {
          continue;
        }
        kscratch.add(entry.getKey());
        vscratch.write(v, valueType);
      }
      vscratch.writeShort(0, kscratch.size());
    }
    keys.serialize(rowNum, kscratch);
    values.serialize(rowNum, vscratch.toByteArray());
  }

  @Override
  public void close() throws IOException
  {
    keys.close();
    values.close();
  }

  @Override
  public ColumnDescriptor.Builder buildDescriptor(IOPeon ioPeon, ColumnDescriptor.Builder builder) throws IOException
  {
    ColumnDescriptor key = keys.buildDescriptor(ioPeon, new ColumnDescriptor.Builder()).build();
    ColumnDescriptor value = values.buildDescriptor(ioPeon, new ColumnDescriptor.Builder()).build();
    builder.setValueType(ValueDesc.ofMap(ValueDesc.STRING, ValueDesc.of(valueType)));
    builder.addSerde(new MapColumnPartSerde(key, value));
    return builder;
  }
}
