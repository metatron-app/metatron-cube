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

import io.druid.segment.bitmap.IntIterable;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.IntScanner;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntSupplier;

/**
 * Get an int an index (array or list lookup abstraction without boxing).
 */
public interface IndexedInts extends IntIterable, Closeable
{
  IndexedInts EMPTY = new IndexedInts()
  {
    @Override
    public int size() { return 0;}

    @Override
    public int get(int index) { return -1;}
  };

  abstract class SingleValued implements IndexedInts
  {
    @Override
    public final int size() { return 1;}
  }

  public static IndexedInts from(IntSupplier supplier)
  {
    return new IndexedInts.SingleValued()
    {
      @Override
      public int get(int index) { return index == 0 ? supplier.getAsInt() : -1;}
    };
  }

  static IndexedInts from(int v)
  {
    return new SingleValued()
    {
      @Override
      public int get(int index) { return index == 0 ? v : -1;}
    };
  }

  static IndexedInts from(final int[] array)
  {
    return new IndexedInts()
    {
      @Override
      public int size()
      {
        return array.length;
      }

      @Override
      public int get(int index)
      {
        return array[index];
      }
    };
  }

  static IndexedInts from(int[] array, int length)
  {
    return length == 0 ? EMPTY : from(array.length == length ? array : Arrays.copyOf(array, length));
  }

  int size();

  int get(int index);

  default int get(int index, int[] convey)
  {
    int limit = Math.min(size() - index, convey.length);
    for (int i = 0; i < limit; i++) {
      convey[i] = get(index + i);
    }
    return limit;
  }

  @Override
  default IntIterator iterator()
  {
    return new IntIterators.Abstract()
    {
      private final int limit = size();
      private int index;

      @Override
      public int next()
      {
        return get(index++);
      }

      @Override
      public boolean hasNext()
      {
        return index < limit;
      }
    };
  }

  default void scan(final IntIterator iterator, final IntScanner scanner)
  {
    if (iterator == null) {
      final int size = size();
      for (int index = 0; index < size; index++) {
        scanner.apply(index, this::get);
      }
    } else {
      while (iterator.hasNext()) {
        scanner.apply(iterator.next(), this::get);
      }
    }
  }

  default int[] toArray()
  {
    final int[] array = new int[size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = get(i);
    }
    return array;
  }

  default void close() throws IOException {}

  interface Shared
  {
    IndexedInts asSingleThreaded(int cache);
  }

  static IndexedInts prepare(IndexedInts source, int cache)
  {
    if (source instanceof Shared) {
      return ((Shared) source).asSingleThreaded(cache);
    }
    return source;
  }
}
