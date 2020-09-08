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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

/**
*/
public class CompressedDoubleBufferObjectStrategy extends FixedSizeCompressedObjectStrategy<DoubleBuffer>
{
  public static CompressedDoubleBufferObjectStrategy getBufferForOrder(final ByteOrder order, final CompressionStrategy compression, final int sizePer)
  {
    return new CompressedDoubleBufferObjectStrategy(order, compression, sizePer);
  }

  private CompressedDoubleBufferObjectStrategy(
      final ByteOrder order,
      final CompressionStrategy compression,
      final int sizePer
  )
  {
    super(
        order,
        new BufferConverter<DoubleBuffer>()
        {
          @Override
          public DoubleBuffer convert(ByteBuffer buf)
          {
            return buf.asDoubleBuffer();
          }

          @Override
          public int sizeOf(int count)
          {
            return count * Double.BYTES;
          }

          @Override
          public DoubleBuffer combine(ByteBuffer into, DoubleBuffer from)
          {
            return into.asDoubleBuffer().put(from);
          }
        },
        compression,
        sizePer
    );
  }
}
