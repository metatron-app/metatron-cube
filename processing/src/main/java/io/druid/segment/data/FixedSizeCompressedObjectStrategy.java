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

import io.druid.collections.ResourceHolder;
import io.druid.segment.CompressedPools;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class FixedSizeCompressedObjectStrategy<T extends Buffer> extends CompressedObjectStrategy<T>
{
  private final int sizePer;

  protected FixedSizeCompressedObjectStrategy(
      ByteOrder order,
      BufferConverter<T> converter,
      CompressionStrategy compression,
      int sizePer
  )
  {
    super(order, converter, compression);
    this.sizePer = sizePer;
  }

  @Override
  protected ByteBuffer bufferFor(T val)
  {
    return ByteBuffer.allocate(converter.sizeOf(sizePer)).order(order);
  }

  @Override
  protected ResourceHolder<ByteBuffer> decompress(ByteBuffer in, int numBytes)
  {
    ResourceHolder<ByteBuffer> holder = CompressedPools.getByteBuf(order);
    decompressor.decompress(in, numBytes, holder.get(), converter.sizeOf(sizePer));
    return holder;
  }
}
