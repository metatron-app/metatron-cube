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

import io.druid.collections.ResourceHolder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategies;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class CompressedComplexColumnPartSupplier implements ColumnPartProvider<ComplexColumn>
{
  private static final Logger LOG = new Logger(CompressedComplexColumnPartSupplier.class);

  private final CompressionStrategy compressionType;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> indexed;
  private final int[] mapping;
  private final ShortBuffer offsets;
  private final ComplexMetricSerde.CompressionSupport serde;

  public CompressedComplexColumnPartSupplier(
      CompressionStrategy compressionType,
      ByteBuffer offsets,
      int[] mapping,
      GenericIndexed<ResourceHolder<ByteBuffer>> indexed,
      ComplexMetricSerde.CompressionSupport serde
  )
  {
    this.compressionType = compressionType;
    this.indexed = indexed;
    this.mapping = mapping;
    this.offsets = offsets.slice().asShortBuffer();
    this.serde = serde;
  }

  @Override
  public int numRows()
  {
    return mapping.length == 0 ? 0 : mapping[mapping.length - 1];
  }

  @Override
  public long getSerializedSize()
  {
    return indexed.getSerializedSize() + (1L + mapping.length) * Integer.BYTES + indexed.size() * Short.BYTES;
  }

  @Override
  public CompressionStrategy compressionType()
  {
    return compressionType;
  }

  @Override
  public Class<? extends ComplexColumn> provides()
  {
    return ComplexColumn.Compressed.class;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ComplexColumn get()
  {
    ValueDesc type = ValueDesc.of(serde.getTypeName());
    ObjectStrategy strategy = ObjectStrategies.singleThreaded(serde.getObjectStrategy());
    return new ComplexColumn.Compressed(type, strategy, mapping, offsets, indexed.asSingleThreaded(), compressionType);
  }
}
