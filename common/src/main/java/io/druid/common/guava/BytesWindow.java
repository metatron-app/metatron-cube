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
import io.druid.common.utils.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BytesWindow implements Comparable<BytesWindow>, BinaryRef
{
  private byte[] buffer;
  private int from;
  private int length;

  public BytesWindow set(byte[] buffer)
  {
    return set(buffer, 0, buffer.length);
  }

  public BytesWindow set(byte[] buffer, int from, int length)
  {
    this.buffer = buffer;
    this.from = from;
    this.length = length;
    return this;
  }

  @Override
  public int length()
  {
    return length;
  }

  @Override
  public byte get(int index)
  {
    return buffer[from + index];
  }

  @Override
  public ByteBuffer toBuffer()
  {
    return ByteBuffer.wrap(buffer, from, length);
  }

  @Override
  public byte[] toBytes()
  {
    return Arrays.copyOfRange(buffer, from, from + length);
  }

  @Override
  public String toUTF8()
  {
    return StringUtils.toUTF8String(buffer, from, length);
  }

  @Override
  public boolean equals(Object obj)
  {
    final BytesWindow o = (BytesWindow) obj;
    if (length != o.length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (buffer[from + i] != o.buffer[o.from + i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(final BytesWindow o)
  {
    final int limit = from + Math.min(length, length);
    for (int i = from, j = o.from; i < limit; i++, j++) {
      final int cmp = Integer.compare(buffer[i] & 0xff, o.buffer[j] & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Ints.compare(length, length);
  }
}
