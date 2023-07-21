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
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.IOPeon;
import io.druid.segment.serde.ArrayColumnPartSerde;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;

public class ArrayColumnSerializer implements MetricColumnSerializer
{
  public static ArrayColumnSerializer create(String metric, ValueDesc type, Factory factory) throws IOException
  {
    String prefix = metric + ".";
    String[] elements = TypeUtils.splitDescriptiveType(type);
    if (elements == null) {
      throw new IOException();
    }
    return new ArrayColumnSerializer(prefix, ValueDesc.of(elements[1]), factory);
  }

  private final String prefix;
  private final ValueDesc elementType;
  private final Factory factory;
  private final List<MetricColumnSerializer> serializers;

  private int ix;
  private IOPeon ioPeon;

  public ArrayColumnSerializer(String prefix, ValueDesc elementType, Factory factory)
  {
    this.prefix = prefix;
    this.elementType = elementType;
    this.factory = factory;
    this.serializers = Lists.newArrayList();
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    this.ioPeon = ioPeon;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void serialize(int rowNum, Object aggs) throws IOException
  {
    if (aggs == null) {
      for (MetricColumnSerializer serializer : serializers) {
        serializer.serialize(rowNum, null);
      }
    } else if (aggs instanceof List) {
      List<Object> document = (List<Object>) aggs;
      int size = document.size();
      List<MetricColumnSerializer> serializers = serializers(size, rowNum);
      for (int i = 0; i < serializers.size(); i++) {
        serializers.get(i).serialize(rowNum, i < size ? document.get(i) : null);
      }
    } else {
      int size = Array.getLength(aggs);
      List<MetricColumnSerializer> serializers = serializers(size, rowNum);
      for (int i = 0; i < serializers.size(); i++) {
        serializers.get(i).serialize(rowNum, i < size ? Array.get(aggs, i) : null);
      }
    }
  }

  private List<MetricColumnSerializer> serializers(int elements, int rowNum) throws IOException
  {
    while (serializers.size() < elements) {
      MetricColumnSerializer serializer = factory.create(prefix + serializers.size(), elementType);
      serializer.open(ioPeon);
      for (int i = 0; i < rowNum; i++) {
        serializer.serialize(i, null);
      }
      serializers.add(serializer);
    }
    return serializers;
  }

  @Override
  public void close() throws IOException
  {
    for (int i = 0; i < serializers.size(); i++) {
      try {
        serializers.get(i).close();
      }
      catch (IOException e) {
        for (; i < serializers.size(); i++) {
          IOUtils.closeQuietly(serializers.get(i));
        }
        throw e;
      }
    }
  }

  @Override
  public ColumnDescriptor.Builder buildDescriptor(IOPeon ioPeon, ColumnDescriptor.Builder builder) throws IOException
  {
    if (serializers.isEmpty()) {
      return builder;
    }
    List<ColumnDescriptor> descriptors = Lists.newArrayList();
    for (MetricColumnSerializer serializer : serializers) {
      descriptors.add(serializer.buildDescriptor(ioPeon, new ColumnDescriptor.Builder()).build());
    }
    builder.setValueType(ValueDesc.ofArray(descriptors.get(0).getValueType()));
    builder.addSerde(new ArrayColumnPartSerde(descriptors));
    return builder;
  }
}
