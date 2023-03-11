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
import io.druid.data.UTF8Bytes;
import io.druid.data.input.BytesOutputStream;

import java.nio.ByteBuffer;

public interface BinaryRef extends Comparable<BinaryRef>
{
  int length();

  byte get(int index);

  ByteBuffer toBuffer();

  byte[] toBytes();

  default String toUTF8()
  {
    final byte[] bytes = toBytes();
    return StringUtils.toUTF8String(bytes, 0, bytes.length);
  }

  default BytesOutputStream copyTo(BytesOutputStream output)
  {
    final byte[] bytes = toBytes();
    output.write(bytes);
    return output;
  }

  default boolean eq(UTF8Bytes value)
  {
    final int length = length();
    if (length != value.length()) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (get(i) != value.get(i)) {
        return false;
      }
    }
    return true;
  }

  default boolean startsWith(UTF8Bytes value)
  {
    final int length = length();
    if (length < value.length()) {
      return false;
    }
    final int limit = Math.min(length, value.length());
    for (int i = 0; i < limit; i++) {
      if (get(i) != value.get(i)) {
        return false;
      }
    }
    return true;
  }

  default boolean startsWith(UTF8Bytes value, int offset)
  {
    final int length = length() - offset;
    if (length < value.length()) {
      return false;
    }
    final int limit = Math.min(length, value.length());
    for (int i = 0; i < limit; i++) {
      if (get(offset + i) != value.get(i)) {
        return false;
      }
    }
    return true;
  }

  default boolean endsWith(UTF8Bytes value)
  {
    final int length = length();
    if (length < value.length()) {
      return false;
    }
    return startsWith(value, length - value.length());
  }

  default boolean contains(UTF8Bytes value)
  {
    return indexOf(value) >= 0;
  }

  default int indexOf(UTF8Bytes value)
  {
    return indexOf(value, 0);
  }

  // this returns indexOf + value.length
  default int indexOf(UTF8Bytes value, int offset)
  {
    if (offset < 0) {
      return -1;
    }
    final int length = length() - offset;
    final int limit = length - value.length();
    if (limit < 0) {
      return -1;
    }
x:
    for (int i = offset; i <= offset + limit; i++) {
      if (get(i) == value.get(0)) {
        final int shift = i++;
        for (int x = 1; x < value.length(); x++, i++) {
          if (get(shift + x) != value.get(x)) {
            continue x;
          }
        }
        return i;
      }
    }
    return -1;
  }

  @Override
  default int compareTo(BinaryRef o)
  {
    int length1 = length();
    int length2 = o.length();
    final int limit = Math.min(length1, length2);
    for (int i = 0, j = 0; i < limit; i++, j++) {
      final int cmp = Integer.compare(get(i) & 0xff, o.get(j) & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Ints.compare(length1, length2);
  }
}
