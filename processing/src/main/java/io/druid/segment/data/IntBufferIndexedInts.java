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

import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;

/**
 */
public class IntBufferIndexedInts extends IndexedInts.Abstract implements Comparable<IntBufferIndexedInts>
{
  public static ObjectStrategy<IntBufferIndexedInts> objectStrategy =
      new IntBufferIndexedIntsObjectStrategy();

  public static IntBufferIndexedInts fromArray(int[] array)
  {
    final ByteBuffer buffer = ByteBuffer.allocate(array.length * Ints.BYTES);
    buffer.asIntBuffer().put(array);

    return new IntBufferIndexedInts(buffer.asReadOnlyBuffer());
  }

  private final ByteBuffer buffer;

  public IntBufferIndexedInts(ByteBuffer buffer)
  {
    this.buffer = buffer;
  }

  @Override
  public int size()
  {
    return buffer.remaining() / 4;
  }

  @Override
  public int get(int index)
  {
    return buffer.getInt(buffer.position() + (index * 4));
  }

  public ByteBuffer getBuffer()
  {
    return buffer.asReadOnlyBuffer();
  }

  @Override
  public int compareTo(IntBufferIndexedInts o)
  {
    return buffer.compareTo(o.getBuffer());
  }

  private static class IntBufferIndexedIntsObjectStrategy implements ObjectStrategy<IntBufferIndexedInts>
  {
    @Override
    public Class<? extends IntBufferIndexedInts> getClazz()
    {
      return IntBufferIndexedInts.class;
    }

    @Override
    public IntBufferIndexedInts fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return new IntBufferIndexedInts(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(IntBufferIndexedInts val)
    {
      ByteBuffer buffer = val.getBuffer();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);

      return bytes;
    }
  }
}
