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

import com.google.common.collect.Lists;
import io.druid.common.utils.IOUtils;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.IOPeon;
import io.druid.segment.serde.StructColumnPartSerde;
import io.druid.segment.serde.StructMetricSerde;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StructColumnSerializer implements MetricColumnSerializer
{
  public static StructColumnSerializer create(String metric, ValueDesc type, Factory factory) throws IOException
  {
    String prefix = metric + ".";
    List<String> fieldNames = Lists.newArrayList();
    List<MetricColumnSerializer> serializers = Lists.newArrayList();
    StructMetricSerde struct = new StructMetricSerde(TypeUtils.splitDescriptiveType(type.typeName()));
    for (Pair<String, ValueDesc> pair : struct) {
      fieldNames.add(pair.lhs);
      serializers.add(factory.create(prefix + pair.lhs, pair.rhs));
    }
    return new StructColumnSerializer(fieldNames, serializers);
  }

  private final String[] fieldNames;
  private final MetricColumnSerializer[] serializers;

  public StructColumnSerializer(List<String> fieldNames, List<MetricColumnSerializer> serializers)
  {
    this.fieldNames = fieldNames.toArray(new String[0]);
    this.serializers = serializers.toArray(new MetricColumnSerializer[0]);
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    for (MetricColumnSerializer serializer : serializers) {
      serializer.open(ioPeon);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void serialize(int rowNum, Object aggs) throws IOException
  {
    if (aggs == null) {
      for (MetricColumnSerializer serializer : serializers) {
        serializer.serialize(rowNum, null);
      }
    } else if (aggs instanceof Map) {
      Map<String, Object> document = (Map<String, Object>) aggs;
      for (int i = 0; i < fieldNames.length; i++) {
        serializers[i].serialize(rowNum, get(document, fieldNames[i]));
      }
    } else if (aggs instanceof List) {
      List<Object> document = (List<Object>) aggs;
      for (int i = 0; i < serializers.length; i++) {
        serializers[i].serialize(rowNum, document.get(i));
      }
    } else {
      for (int i = 0; i < serializers.length; i++) {
        serializers[i].serialize(rowNum, Array.get(aggs, i));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Object get(Map<String, Object> document, String field)
  {
    int ix = field.indexOf('.');
    for (; ix > 0; ix = field.indexOf('.', ix + 1)) {
      Object value = document.get(field.substring(0, ix));
      if (value instanceof Map) {
        return get((Map<String, Object>) value, field.substring(ix + 1));
      }
    }
    return document.get(field);
  }

  @Override
  public void close() throws IOException
  {
    for (int i = 0; i < serializers.length; i++) {
      try {
        serializers[i].close();
      }
      catch (IOException e) {
        for (; i < serializers.length; i++) {
          IOUtils.closeQuietly(serializers[i]);
        }
        throw e;
      }
    }
  }

  @Override
  public ColumnDescriptor.Builder buildDescriptor(IOPeon ioPeon, ColumnDescriptor.Builder builder) throws IOException
  {
    ValueDesc[] fieldType = new ValueDesc[serializers.length];
    List<ColumnDescriptor> descriptors = Lists.newArrayList();
    for (int i = 0; i < serializers.length; i++) {
      ColumnDescriptor descriptor = serializers[i].buildDescriptor(ioPeon, new ColumnDescriptor.Builder()).build();
      fieldType[i]= descriptor.getValueType();
      descriptors.add(descriptor);
    }
    builder.setValueType(ValueDesc.ofStruct(fieldNames, fieldType));
    builder.addSerde(new StructColumnPartSerde(Arrays.asList(fieldNames), descriptors));
    return builder;
  }
}
