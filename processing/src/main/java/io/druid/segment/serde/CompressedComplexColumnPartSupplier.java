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

import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.AbstractGenericColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Objects;

public class CompressedComplexColumnPartSupplier implements ColumnPartProvider<GenericColumn>
{
  private final CompressionStrategy compressionType;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> indexed;
  private final int[] mapping;
  private final ShortBuffer offsets;
  private final ObjectStrategy strategy;
  private final ValueDesc type;

  public CompressedComplexColumnPartSupplier(
      CompressionStrategy compressionType,
      ByteBuffer offsets,
      int[] mapping,
      GenericIndexed<ResourceHolder<ByteBuffer>> indexed,
      ComplexMetricSerde serde
  )
  {
    this.compressionType = compressionType;
    this.indexed = indexed;
    this.mapping = mapping;
    this.offsets = offsets.slice().asShortBuffer();
    this.strategy = serde.getObjectStrategy();
    this.type = ValueDesc.of(serde.getTypeName());
  }

  @Override
  public int numRows()
  {
    return mapping[mapping.length - 1];
  }

  @Override
  public long getSerializedSize()
  {
    return indexed.getSerializedSize() + (1 + mapping.length) * Ints.BYTES + indexed.size() * Short.BYTES;
  }

  @Override
  public GenericColumn get()
  {
    return new AbstractGenericColumn()
    {
      private int cacheId = -1;
      private ResourceHolder<ByteBuffer> cached;

      private final GenericIndexed<ResourceHolder<ByteBuffer>> compressed = indexed.asSingleThreaded();

      @Override
      public int getNumRows()
      {
        return numRows();
      }

      @Override
      public ValueDesc getType()
      {
        return type;
      }

      @Override
      public CompressionStrategy compressionType()
      {
        return compressionType;
      }

      @Override
      public String getString(int rowNum)
      {
        return Objects.toString(getValue(rowNum), null);
      }

      @Override
      public Object getValue(int rowNum)
      {
        int index = Arrays.binarySearch(mapping, rowNum);
        final int startOffset = rowNum == 0 || index >= 0 ? 0 : offsets.get(rowNum - 1) & 0xFFFF;
        final int endOffset = offsets.get(rowNum) & 0xFFFF;
        if (startOffset == endOffset) {
          return null;
        }
        if (index < 0) {
          index = -index - 1;
        } else {
          index = index + 1;
        }
        if (index != cacheId) {
          if (cached != null) {
            cached.close();
          }
          cached = compressed.get(index);
          cacheId = index;
        }
        final ByteBuffer buffer = cached.get();
        buffer.position(startOffset);
        buffer.limit(endOffset);

        return strategy.fromByteBuffer(buffer, buffer.remaining());
      }

      @Override
      public void close() throws IOException
      {
        if (cached != null) {
          cached.close();
        }
      }
    };
  }
}
