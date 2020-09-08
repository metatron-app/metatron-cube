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

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SizePrefixedCompressedObjectStrategy extends CompressedObjectStrategy<ByteBuffer>
{
  public SizePrefixedCompressedObjectStrategy(CompressionStrategy compression)
  {
    super(ByteOrder.BIG_ENDIAN, BufferConverter.IDENTITY, compression);
  }

  @Override
  public final byte[] toBytes(ResourceHolder<ByteBuffer> val)
  {
    // todo optimize this
    final ByteBuffer byteBuffer = val.get();
    if (byteBuffer.hasArray()) {
      final byte[] array = byteBuffer.array();
      final int offset = byteBuffer.arrayOffset();
      final int length = byteBuffer.remaining();
      return Bytes.concat(Ints.toByteArray(length), compressor.compress(array, offset, length));
    } else {
      final ByteBuffer buffer = val.get();
      final byte[] data = new byte[buffer.remaining()];
      buffer.get(data);
      return Bytes.concat(Ints.toByteArray(data.length), compressor.compress(data));
    }
  }

  @Override
  protected final ResourceHolder<ByteBuffer> decompress(ByteBuffer in, int numBytes)
  {
    final int size = in.getInt();
    final ResourceHolder<ByteBuffer> holder;
    if (size < CompressedPools.BUFFER_SIZE - CompressedPools.RESERVE) {
      holder = CompressedPools.getByteBuf(order);
    } else {
      holder = StupidResourceHolder.create(ByteBuffer.allocate(size));
    }
    decompressor.decompress(in, numBytes - Integer.BYTES, holder.get(), size);
    return holder;
  }
}
