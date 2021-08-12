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

import io.druid.segment.column.IntScanner;
import it.unimi.dsi.fastutil.ints.IntIterable;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * Get a int an index (array or list lookup abstraction without boxing).
 */
public interface IndexedInts extends IntIterable, Closeable
{
  static IndexedInts from(int[] array)
  {
    return new ArrayBasedIndexedInts(array);
  }

  static IndexedInts from(int v)
  {
    return new IndexedInts() {

      @Override
      public int size()
      {
        return 1;
      }

      @Override
      public int get(int index)
      {
        return v;
      }
    };
  }

  int size();

  int get(int index);

  default it.unimi.dsi.fastutil.ints.IntIterator iterator()
  {
    return new it.unimi.dsi.fastutil.ints.IntIterator()
    {
      private final int limit = size();
      private int index;

      @Override
      public int nextInt()
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

  default void close() throws IOException {}


  abstract interface SingleValued extends IndexedInts
  {
    @Override
    public default int size()
    {
      return 1;
    }

    int get();

    @Override
    public default int get(int index)
    {
      return get();
    }
  }

  // marker.. use iterator instead of size + get
  interface PreferIterator extends IndexedInts {}
}
