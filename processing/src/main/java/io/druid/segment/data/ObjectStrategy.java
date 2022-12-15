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

import io.druid.common.guava.BinaryRef;

import java.nio.ByteBuffer;

public interface ObjectStrategy<T>
{
  Class<? extends T> getClazz();

  default T fromByteBuffer(ByteBuffer buffer)
  {
    return fromByteBuffer(buffer, buffer.remaining());
  }

  default T fromByteBuffer(BinaryRef ref)
  {
    return fromByteBuffer(ref.toBuffer(), ref.length());
  }

  /**
   * Convert values from their underlying byte representation.
   *
   * Implementations of this method must not change the given buffer mark, or limit, but may modify its position.
   * Use buffer.asReadOnlyBuffer() or buffer.duplicate() if mark or limit need to be set.
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return an object created from the given byte buffer representation
   */
  T fromByteBuffer(ByteBuffer buffer, int numBytes);

  byte[] toBytes(T val);

  interface CompareSupport<T> extends ObjectStrategy<T>, java.util.Comparator<T>
  {
  }

  // marker
  interface RawComparable<T> extends CompareSupport<T>
  {
  }

  interface SingleThreadSupport<T> extends ObjectStrategy<T>
  {
    ObjectStrategy<T> singleThreaded();
  }

  interface Recycling<T> extends ObjectStrategy<T>
  {
    T fromByteBuffer(ByteBuffer buffer, int numBytes, T object);
  }

  ObjectStrategy<String> STRING_STRATEGY = new ObjectStrategies.StringObjectStrategy();

  ObjectStrategy<Object> DUMMY = new ObjectStrategy<Object>()
  {
    @Override
    public Class getClazz()
    {
      return Object.class;
    }

    @Override
    public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      throw new UnsupportedOperationException("fromByteBuffer");
    }

    @Override
    public byte[] toBytes(Object val)
    {
      throw new UnsupportedOperationException("toBytes");
    }
  };
}
