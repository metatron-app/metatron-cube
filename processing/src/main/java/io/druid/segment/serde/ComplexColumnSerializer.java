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

package io.druid.segment.serde;

import com.google.common.collect.ImmutableMap;
import io.druid.data.ValueDesc;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.CompressedComplexColumnSerializer;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.IOPeon;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class ComplexColumnSerializer implements GenericColumnSerializer
{
  public static ComplexColumnSerializer create(
      String filenameBase,
      ComplexMetricSerde serde,
      SecondaryIndexingSpec indexingSpec,
      CompressionStrategy compression
  )
  {
    return new ComplexColumnSerializer(filenameBase, serde, indexingSpec, compression);
  }

  private final String columnName;
  private final ComplexMetricSerde serde;
  private final CompressionStrategy compression;
  private final MetricColumnSerializer seconday;

  private String minValue;
  private String maxValue;
  private int numNulls;

  private ColumnPartWriter writer;

  private ComplexColumnSerializer(
      String columnName,
      ComplexMetricSerde serde,
      SecondaryIndexingSpec indexingSpec,
      CompressionStrategy compression
  )
  {
    this.columnName = columnName;
    this.serde = serde;
    this.compression = compression;
    this.seconday = indexingSpec == null ? MetricColumnSerializer.DUMMY :
                    indexingSpec.serializer(columnName, ValueDesc.of(serde.getTypeName()));
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    String filenameBase = String.format("%s.complex_column", columnName);
    writer = CompressedComplexColumnSerializer.create(ioPeon, filenameBase, compression, serde);
    writer.open();
    seconday.open(ioPeon);
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void serialize(int rowNum, Object obj) throws IOException
  {
    writer.add(obj);
    seconday.serialize(rowNum, obj);

    if (obj == null) {
      numNulls++;
    } else if (serde == StringMetricSerde.INSTANCE) {
      String value = (String) obj;
      minValue = minValue == null || minValue.compareTo(value) > 0 ? value : minValue;
      maxValue = maxValue == null || maxValue.compareTo(value) < 0 ? value : maxValue;
    }
  }

  @Override
  public Builder buildDescriptor(ValueDesc desc, Builder builder) throws IOException
  {
    if (desc.isString()) {
      builder.setValueType(ValueDesc.STRING);
      builder.addSerde(new StringColumnPartSerde(this));
    } else {
      builder.setValueType(desc);
      builder.addSerde(new ComplexColumnPartSerde(desc.typeName(), this));
    }
    return seconday.buildDescriptor(desc, builder);
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
    seconday.close();
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    if (numNulls > 0 && (minValue == null || maxValue == null)) {
      return null;
    }
    if (minValue == null || maxValue == null) {
      return ImmutableMap.<String, Object>of(
          "numNulls", numNulls
      );
    }
    return ImmutableMap.<String, Object>of(
        "min", minValue,
        "max", maxValue,
        "numNulls", numNulls
    );
  }
}
