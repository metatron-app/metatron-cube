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

package io.druid.segment.data;

import com.google.common.collect.Ordering;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

public class CompressedIntBufferObjectStrategy extends FixedSizeCompressedObjectStrategy<IntBuffer>
{
  public static final Ordering<Comparable> ORDERING = Ordering.natural().nullsFirst();

  public static CompressedIntBufferObjectStrategy getBufferForOrder(final ByteOrder order, final CompressionStrategy compression, final int sizePer)
  {
    return new CompressedIntBufferObjectStrategy(order, compression, sizePer);
  }

  private CompressedIntBufferObjectStrategy(final ByteOrder order, final CompressionStrategy compression, final int sizePer)
  {
    super(
        order,
        new BufferConverter<IntBuffer>()
        {
          @Override
          public IntBuffer convert(ByteBuffer buf)
          {
            return buf.asIntBuffer();
          }

          @Override
          public int sizeOf(int count)
          {
            return count * Integer.BYTES;
          }

          @Override
          public IntBuffer combine(ByteBuffer into, IntBuffer from)
          {
            return into.asIntBuffer().put(from);
          }
        },
        compression,
        sizePer
    );
  }
}
