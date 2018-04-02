/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;

import java.nio.ByteBuffer;

/**
 */
public class ColumnPartProviders
{
  @SuppressWarnings("unchecked")
  public static <T> ColumnPartProvider<T> ofInstance(final VSizeIndexedInts instance)
  {
    return ofInstance((T) instance, instance.getSerializedSize(), instance.size());
  }

  @SuppressWarnings("unchecked")
  public static <T> ColumnPartProvider<T> ofInstance(final VSizeIndexed instance)
  {
    return ofInstance((T) instance, instance.getSerializedSize(), instance.size());
  }

  public static <T> ColumnPartProvider<T> ofInstance(final T instance, final long length, final int count)
  {
    return new ColumnPartProvider<T>()
    {
      @Override
      public int size()
      {
        return count;
      }

      @Override
      public long getSerializedSize()
      {
        return length;
      }

      @Override
      public T get()
      {
        return instance;
      }
    };
  }

  public static <T> ColumnPartProvider<T> ofType(
      final int numRows,
      final ByteBuffer buffer,
      final ObjectStrategy<T> strategy
  )
  {
    final int length = buffer.remaining();

    return new ColumnPartProvider<T>()
    {
      @Override
      public int size()
      {
        return numRows;
      }

      @Override
      public long getSerializedSize()
      {
        return length;
      }

      @Override
      public T get()
      {
        return strategy.fromByteBuffer(buffer.asReadOnlyBuffer(), length);
      }
    };
  }

  public static ColumnPartProvider<BitSlicedBitmap> ofBitSlicedBitmap(
      final int numRows,
      final ByteBuffer buffer,
      final ObjectStrategy<BitSlicedBitmap> strategy
  )
  {
    final int length = buffer.remaining();

    return new ColumnPartProvider<BitSlicedBitmap>()
    {
      @Override
      public int size()
      {
        return numRows;
      }

      @Override
      public long getSerializedSize()
      {
        return length;
      }

      @Override
      public BitSlicedBitmap get()
      {
        return strategy.fromByteBuffer(buffer.asReadOnlyBuffer(), length);
      }
    };
  }
}
