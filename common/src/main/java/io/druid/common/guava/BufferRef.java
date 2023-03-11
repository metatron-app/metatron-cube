/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.common.guava;

import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;

public class BufferRef implements BinaryRef
{
  public static BufferRef of(ByteBuffer buffer, int offset, int length)
  {
    return new BufferRef(buffer, offset, length);
  }

  private final ByteBuffer buffer;
  private final int offset;
  private final int length;

  private BufferRef(ByteBuffer buffer, int offset, int length)
  {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int length()
  {
    return length;
  }

  @Override
  public byte get(int index)
  {
    return buffer.get(offset + index);
  }

  @Override
  public ByteBuffer toBuffer()
  {
    return (ByteBuffer) buffer.asReadOnlyBuffer()
                              .limit(offset + length)
                              .position(offset);
  }

  @Override
  public byte[] toBytes()
  {
    final byte[] bytes = new byte[length];
    toBuffer().get(bytes);
    return bytes;
  }

  @Override
  public boolean equals(Object obj)
  {
    final BufferRef o = (BufferRef) obj;
    if (length != o.length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (buffer.get(offset + i) != o.buffer.get(o.offset + i)) {
        return false;
      }
    }
    return true;
  }

  public int compareTo(final byte[] value)
  {
    final int len1 = length;
    final int len2 = value.length;
    final int limit = Math.min(len1, len2);
    for (int i = 0; i < limit; i++) {
      final int cmp = Integer.compare(buffer.get(offset + i) & 0xff, value[i] & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Ints.compare(len1, len2);
  }

  private static final int ADDRESS_BITS_PER_WORD = 6;
  private static final int BIT_INDEX_MASK = 0b111111;

  // see java.util.BitSet#get
  public boolean getBool(final int index)
  {
    final int x = 8 * offset + index;
    final long word = getIxWord(x);
    return word != 0 && (word & 1L << (x & BIT_INDEX_MASK)) != 0;
  }

  private long getIxWord(final int index)
  {
    final int limit = length + offset;
    if (index >= limit * Byte.SIZE) {
      return 0L;
    }
    final int ix = index >> ADDRESS_BITS_PER_WORD;
    final int x = ix * Long.BYTES;
    final int remnant = limit - x;
    if (remnant >= Long.BYTES) {
      return buffer.getLong(x);
    }
    long v = 0;
    for (int i = 0; i < remnant; i++) {
      v |= (buffer.get(x + i) & 0xffL) << Byte.SIZE * i;
    }
    return v;
  }
}
