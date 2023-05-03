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
import io.druid.common.guava.BufferRef;
import io.druid.segment.Tools;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface Indexed<T> extends Iterable<T>
{
  int size();

  T get(int index);

  default boolean isEmpty() {return size() == 0;}

  /**
   * Returns the index of "value" in this Indexed object, or a negative number if the value is not present.
   * The negative number is not guaranteed to be any particular number. Subclasses may tighten this contract
   * (GenericIndexed does this).
   *
   * @param value value to search for
   *
   * @return index of value, or a negative number
   */
  default int indexOf(T value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  interface Closeable<T> extends Indexed<T>, java.io.Closeable { }

  interface BufferBacked<T> extends Closeable<T>
  {
    BufferRef getAsRef(int index);

    default byte[] getAsRaw(int index)
    {
      return getAsRef(index).toBytes();
    }
  }

  interface Scannable<T> extends BufferBacked<T>, io.druid.common.Scannable.BufferBacked<T>
  {
    void scan(int index, Tools.Scanner scanner);

    <R> R apply(int index, Tools.Function<R> function);
  }

  interface Searchable<T> extends Scannable<T>
  {
    default int indexOf(T value)
    {
      return indexOf(value, 0, size(), true);
    }

    int indexOf(T value, int start, int end, boolean binary);

    default IntStream indexOf(List<T> values)
    {
      return indexOf(values.stream(), true).filter(x -> x >= 0);
    }

    default IntStream indexOfRaw(List<BinaryRef> values)
    {
      return indexOfRaw(values.stream(), true).filter(x -> x >= 0);
    }

    default IntStream indexOf(Stream<T> stream, boolean binary)
    {
      return search(stream, (v, s) -> indexOf(v, s, size(), binary));
    }

    default IntStream indexOfRaw(Stream<BinaryRef> stream, boolean binary)
    {
      return search(stream, (v, s) -> indexOf(v, s, size(), binary));
    }

    int indexOf(BinaryRef bytes, int start, int end, boolean binary);

    // for searching stream of sorted value
    public static <F> IntStream search(Stream<F> stream, ToIntBiFunction<F, Integer> function)
    {
      return search(stream, function, 0);
    }

    public static <F> IntStream search(Stream<F> stream, ToIntBiFunction<F, Integer> function, int start)
    {
      return stream.mapToInt(new ToIntFunction<F>()
      {
        private int p = start;

        @Override
        public int applyAsInt(F input)
        {
          return p = function.applyAsInt(input, p < 0 ? -p - 1 : p);
        }
      });
    }
  }
}
