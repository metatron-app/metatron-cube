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
import io.druid.data.input.BytesOutputStream;

import java.nio.ByteBuffer;

public class BufferWindow implements Comparable<BufferWindow>, BinaryRef
{
  private ByteBuffer buffer;
  private int offset;
  private int length;

  public BufferWindow set(ByteBuffer buffer, int from, int length)
  {
    this.buffer = buffer;
    this.offset = from;
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
    for (int i = 0; i < length; i++) {
      bytes[i] = buffer.get(offset + i);
    }
    return bytes;
  }

  @Override
  public BytesOutputStream copyTo(BytesOutputStream output)
  {
    output.ensureCapacity(length);
    for (int i = 0; i < length; i++) {
      output.write(buffer.get(offset + i));
    }
    return output;
  }

  @Override
  public boolean equals(Object obj)
  {
    final BufferWindow o = (BufferWindow) obj;
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

  @Override
  public int compareTo(final BufferWindow o)
  {
    final int limit = offset + Math.min(length, length);
    for (int i = offset, j = o.offset; i < limit; i++, j++) {
      final int cmp = Integer.compare(buffer.get(i) & 0xff, o.buffer.get(j) & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Ints.compare(length, length);
  }
}
