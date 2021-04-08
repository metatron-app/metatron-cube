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

public class BufferRef implements Comparable<BufferRef>
{
  public static BufferRef of(ByteBuffer buffer, int from, int to)
  {
    return new BufferRef(buffer, from, to);
  }

  private final ByteBuffer buffer;
  private final int from;
  private final int to;

  private BufferRef(ByteBuffer buffer, int from, int to)
  {
    this.buffer = buffer;
    this.from = from;
    this.to = to;
  }

  private int remaining()
  {
    return to - from;
  }

  @Override
  public boolean equals(Object obj)
  {
    final BufferRef o = (BufferRef) obj;
    final int len1 = remaining();
    final int len2 = o.remaining();
    if (len1 != len2) {
      return false;
    }
    for (int i = to - 1, j = o.to - 1; i >= from; i--, j--) {
      if (buffer.get(i) != o.buffer.get(j)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(BufferRef o)
  {
    final int len1 = remaining();
    final int len2 = o.remaining();
    final int limit = from + Math.min(len1, len2);
    for (int i = from, j = o.from; i < limit; i++, j++) {
      final int cmp = Byte.compare(buffer.get(i), o.buffer.get(j));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Ints.compare(len1, len2);
  }
}