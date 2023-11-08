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

package io.druid.segment.column;

import io.druid.collections.ResourceHolder;
import io.druid.common.guava.BufferRef;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.CompressedPools;
import io.druid.segment.Tools;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

public interface ColumnAccess extends Closeable
{
  ValueDesc getType();

  Object getValue(int rowNum);

  interface WithRawAccess extends ColumnAccess
  {
    byte[] getAsRaw(int index);

    BufferRef getAsRef(int index);

    <R> R apply(int index, Tools.Function<R> function);
  }

  abstract class Compressed implements ColumnAccess
  {
    protected static final Logger LOG = new Logger(Compressed.class);

    private final ObjectStrategy strategy;
    private final int[] mapping;
    private final ShortBuffer offsets;
    private final GenericIndexed<ResourceHolder<ByteBuffer>> compressed;

    private int cacheId = -1;
    private ResourceHolder<ByteBuffer> cached;

    protected Compressed(
        ObjectStrategy strategy,
        int[] mapping,
        ShortBuffer offsets,
        GenericIndexed<ResourceHolder<ByteBuffer>> indexed
    )
    {
      this.strategy = strategy;
      this.mapping = mapping;
      this.offsets = offsets;
      this.compressed = indexed.dedicated();
    }

    public int numRows()
    {
      return mapping.length == 0 ? 0 : mapping[mapping.length - 1];
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
      if (endOffset == CompressedPools.BUFFER_EXCEEDED) {
        buffer.limit(buffer.capacity());
        buffer.position(0);
      } else {
        try {
          buffer.limit(endOffset);
          buffer.position(startOffset);
        }
        catch (IllegalArgumentException e) {
          LOG.warn("-----> %d = %d ( %d ~ %d )", rowNum, index, startOffset, endOffset);
          throw e;
        }
      }
      // this moves position of buffer
      return strategy.fromByteBuffer(buffer);
    }

    @Override
    public void close() throws IOException
    {
      if (cached != null) {
        cached.close();
      }
    }
  }
}
