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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnStats;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.CompressedComplexColumnSerializer;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.SizePrefixedCompressedObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde.CompressionSupport;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class ComplexColumnSerializer implements GenericColumnSerializer
{
  public static ComplexColumnSerializer create(
      String metric,
      ValueDesc type,
      Iterable<Object> values,
      SecondaryIndexingSpec indexingSpec,
      CompressionStrategy compression
  )
  {
    ComplexMetricSerde serde = Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type), "Unknown type[%s]", type);
    return create(metric, serde, values, indexingSpec, compression);
  }

  public static ComplexColumnSerializer create(
      String metric,
      ComplexMetricSerde serde,
      Iterable<Object> values,
      SecondaryIndexingSpec indexingSpec,
      CompressionStrategy compression
  )
  {
    CompressionStrategy strategy = compression == null ? CompressionStrategy.NONE : compression;
    return new ComplexColumnSerializer(metric, serde, values, indexingSpec, strategy);
  }

  private final String columnName;
  private final ComplexMetricSerde serde;
  private final CompressionStrategy compression;
  private final MetricColumnSerializer secondary;

  private String minValue;
  private String maxValue;
  private int numNulls;

  private ColumnPartWriter writer;

  private ComplexColumnSerializer(
      String columnName,
      ComplexMetricSerde serde,
      Iterable<Object> values,
      SecondaryIndexingSpec indexingSpec,
      CompressionStrategy compression
  )
  {
    this.columnName = columnName;
    this.serde = serde;
    this.compression = compression;
    this.secondary = indexingSpec == null ? MetricColumnSerializer.DUMMY :
                     indexingSpec.serializer(columnName, serde.getType(), values);
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    writer = create(ioPeon, String.format("%s.complex_column", columnName));
    writer.open();
    secondary.open(ioPeon);
  }

  @SuppressWarnings("unchecked")
  private ColumnPartWriter create(IOPeon ioPeon, String filenameBase) throws IOException
  {
    if (compression == CompressionStrategy.NONE || !(serde instanceof CompressionSupport)) {
      return GenericIndexedWriter.v2(ioPeon, filenameBase, serde.getObjectStrategy());
    }
    return new CompressedComplexColumnSerializer(
        GenericIndexedWriter.v2(ioPeon, filenameBase, new SizePrefixedCompressedObjectStrategy(compression)),
        compression,
        (CompressionSupport) serde
    );
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void serialize(int rowNum, Object obj) throws IOException
  {
    writer.add(obj);
    secondary.serialize(rowNum, obj);

    if (obj == null) {
      numNulls++;
    } else if (serde == StringMetricSerde.INSTANCE) {
      String value = (String) obj;
      minValue = minValue == null || minValue.compareTo(value) > 0 ? value : minValue;
      maxValue = maxValue == null || maxValue.compareTo(value) < 0 ? value : maxValue;
    }
  }

  @Override
  public Builder buildDescriptor(IOPeon ioPeon, Builder builder) throws IOException
  {
    ValueDesc type = serde.getType();
    if (type.isString()) {
      builder.setValueType(ValueDesc.STRING);
      builder.addSerde(new StringColumnPartSerde(this));
    } else {
      builder.setValueType(type);
      builder.addSerde(new ComplexColumnPartSerde(type.typeName(), this));
    }
    return secondary.buildDescriptor(ioPeon, builder);
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
    secondary.close();
  }

  @Override
  public long getSerializedSize()
  {
    return writer.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    return writer.writeToChannel(channel);
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    if (numNulls > 0 && (minValue == null || maxValue == null)) {
      return null;
    }
    if (minValue == null || maxValue == null) {
      return ImmutableMap.<String, Object>of(
          ColumnStats.NUM_NULLS, numNulls
      );
    }
    return ImmutableMap.<String, Object>of(
        ColumnStats.MIN, minValue,
        ColumnStats.MAX, maxValue,
        ColumnStats.NUM_NULLS, numNulls
    );
  }
}
