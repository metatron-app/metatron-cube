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

package io.druid.common.guava;

import io.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

public class Iterators
{
  private static final int BATCH_DEFAULT = 1024;

  public static <T> CloseableIterator<T> batch(Class<T> clazz, Iterator<T> delegate)
  {
    return new Batch<>(clazz, BATCH_DEFAULT, delegate);
  }

  public static <T> CloseableIterator<T> batch(Class<T> clazz, int size, Iterator<T> delegate)
  {
    return new Batch<>(clazz, size, delegate);
  }

  private static class Batch<T> implements CloseableIterator<T>
  {
    private final Iterator<T> delegate;

    private final T[] batch;
    private Iterator<T> iterator = com.google.common.collect.Iterators.emptyIterator();

    @SuppressWarnings("unchecked")
    public Batch(Class<T> clazz, int size, Iterator<T> delegate)
    {
      this.delegate = delegate;
      this.batch = (T[]) Array.newInstance(clazz, size);
    }

    @Override
    public void close() throws IOException
    {
      if (delegate instanceof Closeable) {
        ((Closeable) delegate).close();
      }
    }

    @Override
    public boolean hasNext()
    {
      return iterator.hasNext() || delegate.hasNext();
    }

    @Override
    public T next()
    {
      if (!iterator.hasNext()) {
        int i = 0;
        for (; i < batch.length && delegate.hasNext(); i++) {
          batch[i] = delegate.next();
        }
        int limit = i;
        iterator = new Iterator<T>()
        {
          private int x;

          @Override
          public boolean hasNext() {return x < limit;}

          @Override
          public T next() {return batch[x++];}
        };
      }
      return iterator.next();
    }
  }

  public static <T> Iterator<T> concat(List<Iterator<T>> iterators)
  {
    return com.google.common.collect.Iterators.concat(iterators.iterator());
  }
}
