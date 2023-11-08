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

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.druid.java.util.common.guava.CloseQuietly;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public interface CloseableIterable<T> extends Iterable<T>, Closeable
{
  static <T> CloseableIterable<T> wrap(Iterable<T> iterable)
  {
    if (iterable instanceof CloseableIterable) {
      return (CloseableIterable<T>) iterable;
    }
    return new CloseableIterable<T>()
    {
      @Override
      public Iterator<T> iterator() {return iterable.iterator();}

      @Override
      public void close() throws IOException {}
    };
  }

  static <T> CloseableIterable<T> wrap(List<Iterable<T>> iterables)
  {
    return new CloseableIterable<T>()
    {
      @Override
      public Iterator<T> iterator()
      {
        return Iterators.concat(Iterables.transform(iterables, Iterable::iterator).iterator());
      }

      @Override
      public void close() throws IOException
      {
        for (Iterable<T> iterable : iterables) {
          CloseQuietly.close(iterable);
        }
      }
    };
  }

  public static <T> CloseableIterable<T> concat(Iterable<? extends Iterable<? extends T>> inputs)
  {
    return new CloseableIterable<T>()
    {
      @Override
      public Iterator<T> iterator()
      {
        return Iterables.concat(inputs).iterator();
      }

      @Override
      public void close() throws IOException
      {
        inputs.forEach(CloseQuietly::close);
      }
    };
  }
}
