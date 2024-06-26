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
import io.druid.java.util.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.SizePrefixedCompressedObjectStrategy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 */
public class StringMetricSerde extends ComplexMetricSerde.CompressionSupport
{
  public static final StringMetricSerde INSTANCE = new StringMetricSerde();

  @Override
  public ValueDesc getType()
  {
    return ValueDesc.STRING;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return rawValue -> Objects.toString(rawValue, null);
  }

  @Override
  public ColumnBuilder deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final byte versionFromBuffer = buffer.get();
    if (versionFromBuffer == GenericIndexed.version) {
      GenericIndexed<String> indexed = GenericIndexed.readIndex(buffer, ObjectStrategy.STRING_STRATEGY);
      builder.setType(ValueDesc.STRING)
             .setHasMultipleValues(false)
             .setGenericColumn(new StringColumnPartSupplier(indexed, CompressionStrategy.NONE));
    } else if (versionFromBuffer == ColumnPartSerde.WITH_COMPRESSION_ID) {
      CompressionStrategy compression = CompressedObjectStrategy.forId(buffer.get());
      ByteBuffer compressMeta = ByteBufferSerializer.prepareForRead(buffer);
      int[] mapping = new int[compressMeta.getInt()];
      for (int i = 0; i < mapping.length; i++) {
        mapping[i] = compressMeta.getInt();
      }
      SizePrefixedCompressedObjectStrategy strategy = new SizePrefixedCompressedObjectStrategy(compression);
      GenericIndexed<ResourceHolder<ByteBuffer>> compressed = GenericIndexed.read(buffer, strategy);
      builder.setType(ValueDesc.STRING)
             .setHasMultipleValues(false)
             .setGenericColumn(
                 new CompressedGenericColumnPartSupplier(compression, compressMeta, mapping, compressed, this)
             );
    } else {
      throw new IAE("Unknown version[%s]", versionFromBuffer);
    }
    return builder;
  }

  @Override
  public ObjectStrategy<String> getObjectStrategy()
  {
    return ObjectStrategy.STRING_STRATEGY;
  }
}
