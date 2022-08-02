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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.IOPeon;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonTypeName("list")
public class ListIndexingSpec implements SecondaryIndexingSpec.WithDescriptor
{
  private final List<SecondaryIndexingSpec> indexingSpecs;

  public ListIndexingSpec(@JsonProperty("indexingSpecs") List<SecondaryIndexingSpec> indexingSpecs)
  {
    this.indexingSpecs = indexingSpecs;
  }

  @Override
  public MetricColumnSerializer serializer(String columnName, ValueDesc type)
  {
    final List<MetricColumnSerializer> serializers = GuavaUtils.transform(
        indexingSpecs, spec -> spec.serializer(columnName, type)
    );
    return new MetricColumnSerializer()
    {
      @Override
      public void open(IOPeon ioPeon) throws IOException
      {
        for (MetricColumnSerializer serializer : serializers) {
          serializer.open(ioPeon);
        }
      }

      @Override
      public void serialize(int rowNum, Object aggs) throws IOException
      {
        for (MetricColumnSerializer serializer : serializers) {
          serializer.serialize(rowNum, aggs);
        }
      }

      @Override
      public ColumnDescriptor.Builder buildDescriptor(ValueDesc desc, ColumnDescriptor.Builder builder)
          throws IOException
      {
        for (MetricColumnSerializer serializer : serializers) {
          builder = serializer.buildDescriptor(desc, builder);
        }
        return builder;
      }

      @Override
      public void close() throws IOException
      {
        for (MetricColumnSerializer serializer : serializers) {
          serializer.close();
        }
      }
    };
  }

  @Override
  public Map<String, String> descriptor(String column)
  {
    Map<String, String> descriptor = Maps.newHashMap();
    for (SecondaryIndexingSpec indexingSpec : indexingSpecs) {
      if (indexingSpec instanceof SecondaryIndexingSpec.WithDescriptor) {
        for (Map.Entry<String, String> entry : ((WithDescriptor) indexingSpec).descriptor(column).entrySet()) {
          descriptor.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + "|" + entry.getValue());
        }
      }
    }
    return descriptor;
  }
}
