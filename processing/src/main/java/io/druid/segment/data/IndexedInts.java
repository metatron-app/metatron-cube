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

import com.google.common.collect.Iterators;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Get a int an index (array or list lookup abstraction without boxing).
 */
public interface IndexedInts extends Iterable<Integer>, Closeable
{
  int size();
  int get(int index);
  void fill(int index, int[] toFill);

  abstract class SingleValued implements IndexedInts
  {
    @Override
    public int size()
    {
      return 1;
    }

    protected abstract int get();

    @Override
    public int get(int index)
    {
      return get();
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return Iterators.singletonIterator(get());
    }

    @Override
    public void fill(int index, int[] toFill)
    {
      throw new UnsupportedOperationException("fill not supported");
    }

    @Override
    public void close() throws IOException
    {
    }
  }
}
