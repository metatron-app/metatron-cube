/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.parsers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable
{
  CloseableIterator EMPTY = new CloseableIterator()
  {
    @Override
    public void close() throws IOException
    {
    }

    @Override
    public boolean hasNext()
    {
      return false;
    }

    @Override
    public Object next()
    {
      throw new NoSuchElementException("no");
    }
  };

  @SuppressWarnings("unchecked")
  static <T> CloseableIterator<T> empty()
  {
    return EMPTY;
  }

  static <T> CloseableIterator<T> wrap(Iterator<T> iterator)
  {
    if (iterator instanceof CloseableIterator) {
      return (CloseableIterator<T>) iterator;
    }
    return new CloseableIterator<T>()
    {
      @Override
      public void close() throws IOException {}

      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public T next()
      {
        return iterator.next();
      }
    };
  }
}
