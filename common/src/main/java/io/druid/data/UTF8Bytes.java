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

package io.druid.data;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import io.druid.common.guava.Comparators;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public final class UTF8Bytes implements Comparable<UTF8Bytes>
{
  public static UTF8Bytes of(byte[] value)
  {
    return of(value, 0, value.length);
  }

  public static UTF8Bytes read(ByteBuffer buffer, int offset, int length)
  {
    byte[] bytes = new byte[length];
    ((ByteBuffer) buffer.limit(offset + length).position(offset)).get(bytes);
    return UTF8Bytes.of(bytes);
  }

  public static UTF8Bytes of(byte[] value, int offset, int length)
  {
    return new UTF8Bytes(value, offset, length);
  }

  public static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
  public static final Comparator<byte[]> COMPARATOR_NF = Comparators.NULL_FIRST(COMPARATOR);

  private final byte[] value;
  private final int offset;
  private final int length;

  private UTF8Bytes(byte[] value, int offset, int length)
  {
    this.value = value;
    this.offset = offset;
    this.length = length;
  }

  public byte[] value()
  {
    return value;
  }

  public int offset()
  {
    return offset;
  }

  public int length()
  {
    return length;
  }

  public byte[] asBytes()
  {
    return offset == 0 && length == value.length ? value : Arrays.copyOfRange(value, offset, offset + length);
  }

  public void writeTo(BytesOutputStream output)
  {
    output.write(value, offset, length);
  }

  @Override
  public int hashCode()
  {
    int result = 1;
    for (int i = 0; i < length; i++) {
      result = 31 * result + value[offset + i];
    }
    return result;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof UTF8Bytes)) {
      return false;
    }
    final UTF8Bytes other = (UTF8Bytes) o;
    return length == other.length && compareTo(other) == 0;
  }

  @Override
  public String toString()
  {
    return length == 0 ? null : StringUtils.fromUtf8(value, offset, length);
  }

  @Override
  public int compareTo(UTF8Bytes o)
  {
    final int minLength = Math.min(length, o.length);
    final int minWords = minLength / Longs.BYTES;

    final int wordCompare = minWords * Longs.BYTES;
    for (int i = 0; i < wordCompare; i += Longs.BYTES) {
      long lw = readLong(value, offset + i);
      long rw = readLong(o.value, o.offset + i);
      if (lw != rw) {
        return UnsignedLongs.compare(lw, rw);
      }
    }
    for (int i = wordCompare; i < minLength; i++) {
      int result = UnsignedBytes.compare(value[offset + i], o.value[o.offset + i]);
      if (result != 0) {
        return result;
      }
    }
    return Integer.compare(length, o.length);
  }

  private static long readLong(final byte[] b, final int x)
  {
    long lw = b[x] & 0xff;
    lw = (lw << 8) + (b[x + 1] & 0xff);
    lw = (lw << 8) + (b[x + 2] & 0xff);
    lw = (lw << 8) + (b[x + 3] & 0xff);
    lw = (lw << 8) + (b[x + 4] & 0xff);
    lw = (lw << 8) + (b[x + 5] & 0xff);
    lw = (lw << 8) + (b[x + 6] & 0xff);
    lw = (lw << 8) + (b[x + 7] & 0xff);
    return lw;
  }
}
