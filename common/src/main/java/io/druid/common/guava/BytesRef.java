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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BytesRef
{
  public final byte[] bytes;
  public final int offset;
  public final int length;

  @JsonCreator
  public BytesRef(byte[] bytes)
  {
    this(bytes, 0, bytes.length);
  }

  public BytesRef(byte[] bytes, int offset, int length)
  {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  @JsonValue
  public byte[] asArray()
  {
    return Arrays.copyOfRange(bytes, offset, offset + length);
  }

  public byte get(int index)
  {
    return bytes[offset + index];
  }

  public ByteBuffer toBuffer()
  {
    return ByteBuffer.wrap(bytes, offset, length);
  }

  public String toUTF8()
  {
    return new String(bytes, offset, length);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BytesRef bytesRef = (BytesRef) o;
    if (length != bytesRef.length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (bytes[offset + i] != bytesRef.bytes[bytesRef.offset + i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = 1;
    for (int i = 0; i < length; i++) {
      result = 31 * result + bytes[offset + i];
    }
    return result;
  }
}
