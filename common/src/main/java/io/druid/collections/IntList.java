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

package io.druid.collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

/**
 */
public class IntList implements Iterable<Integer>, IntConsumer
{
  public static <T> IntList collect(T[] sources, Predicate<T> predicate)
  {
    IntList ixs = new IntList();
    for (int i = 0; i < sources.length; i++) {
      if (predicate.apply(sources[i])) {
        ixs.add(i);
      }
    }
    return ixs;
  }

  public static IntList sizeOf(int size)
  {
    return new IntList(size);
  }

  @JsonCreator
  public static IntList of(int... values)
  {
    return new IntList(values);
  }

  private int[] array;
  private int size;

  public IntList()
  {
    this(10);
  }

  private IntList(int init)
  {
    this.array = new int[init];
  }

  private IntList(int... array)
  {
    this(array, array.length);
  }

  private IntList(int[] array, int size)
  {
    this.array = array;
    this.size = size;
    Preconditions.checkArgument(size <= array.length);
  }

  public int size()
  {
    return size;
  }

  public void add(int value)
  {
    reserve(1);
    array[size++] = value;
  }

  public void addAll(int... values)
  {
    reserve(values.length);
    System.arraycopy(values, 0, array, size, values.length);
    size += values.length;
  }

  public void addAll(int[] values, int offset, int length)
  {
    reserve(length);
    System.arraycopy(values, offset, array, size, length);
    size += length;
  }

  public void addAll(IntList intList)
  {
    reserve(intList.size);
    System.arraycopy(intList.array, 0, array, size, intList.size);
    size += intList.size;
  }

  public void addAll(IntStream stream)
  {
    stream.forEach(this);
  }

  @Override
  public void accept(int value)
  {
    add(value);
  }

  private void reserve(int reserve)
  {
    if (size + reserve > array.length) {
      array = Arrays.copyOf(array, Math.max(array.length << 1, array.length + reserve));
    }
  }

  public void set(int index, int value)
  {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    array[index] = value;
  }

  public int get(int index)
  {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return array[index];
  }

  public int indexOf(int value)
  {
    final int index = Ints.indexOf(array, value);
    return index >= size ? -1 : index;
  }

  public int compact(int current)
  {
    final int remanining = size - current;
    if (current >= remanining) {
      System.arraycopy(array, current, array, 0, remanining);
      size = remanining;
      current = 0;
    }
    return current;
  }

  public IntList sort()
  {
    Arrays.sort(array, 0, size);
    return this;
  }

  public <T> Iterable<T> transform(final IntFunction<T> function)
  {
    return () -> new Iterator<T>()
    {
      private int x;
      private final int limit = size;

      @Override
      public boolean hasNext()
      {
        return x < limit;
      }

      @Override
      public T next()
      {
        return function.apply(array[x++]);
      }
    };
  }

  @JsonValue
  public int[] array()
  {
    return Arrays.copyOfRange(array, 0, size);
  }

  public int[] unwrap()
  {
    return array;
  }

  public void shuffle()
  {
    shuffle(new Random());
  }

  public void shuffle(Random r)
  {
    if (size > 1) {
      for (int i = size; i > 1; i--) {
        final int from = i - 1;
        final int to = r.nextInt(i);
        final int x = array[from];
        array[from] = array[to];
        array[to] = x;
      }
    }
  }

  public IntList concat(IntList other)
  {
    final int length = size + other.size;
    if (array.length >= length) {
      System.arraycopy(other.array, 0, array, size, other.size);
      this.size = length;
    } else {
      final int[] concat = Arrays.copyOf(array, length);
      System.arraycopy(other.array, 0, concat, size, other.size);
      this.array = concat;
      this.size = length;
    }
    return this;
  }

  public IntStream stream()
  {
    return size == 0 ? IntStream.empty() : IntStream.of(array());
  }

  public IntList sortOn(IntComparator cp)
  {
    if (size > 1) {
      it.unimi.dsi.fastutil.Arrays.quickSort(0, size, (x1, x2) -> cp.compare(array[x1], array[x2]), this::swap);
    }
    return this;
  }

  public IntList sortOn(int[] v, boolean ascending)
  {
    if (size > 1) {
      IntComparator comparator = ascending
                                 ? (x1, x2) -> Integer.compare(v[array[x1]], v[array[x2]])
                                 : (x1, x2) -> Integer.compare(v[array[x2]], v[array[x1]]);
      it.unimi.dsi.fastutil.Arrays.quickSort(0, size, comparator, this::swap);
    }
    return this;
  }

  private void swap(int a, int b)
  {
    int t = array[a];
    array[a] = array[b];
    array[b] = t;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return Ints.asList(array()).iterator();
  }

  public IntIterator intIterator()
  {
    if (size == 0) {
      return IntIterators.EMPTY_ITERATOR;
    } else if (size == 1) {
      return IntIterators.singleton(array[0]);
    }
    return IntIterators.wrap(array, 0, size);
  }

  public boolean isEmpty()
  {
    return size == 0;
  }

  public void clear()
  {
    size = 0;
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
    IntList other = (IntList) o;
    if (size != other.size) {
      return false;
    }
    for (int i = 0; i < size ;i++) {
      if (array[i] != other.array[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(array());
  }
}
