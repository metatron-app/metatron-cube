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

  @JsonCreator
  public static IntList of(int... values)
  {
    return new IntList(values);
  }

  private int[] baseArray;
  private int size;

  public IntList()
  {
    this(10);
  }

  public IntList(int init)
  {
    this.baseArray = new int[init];
  }

  public IntList(int... baseArray)
  {
    this(baseArray, baseArray.length);
  }

  public IntList(int[] baseArray, int size)
  {
    this.baseArray = baseArray;
    this.size = size;
    Preconditions.checkArgument(size <= baseArray.length);
  }

  public int size()
  {
    return size;
  }

  public void add(int value)
  {
    reserve(1);
    baseArray[size++] = value;
  }

  public void addAll(int... values)
  {
    reserve(values.length);
    System.arraycopy(values, 0, baseArray, size, values.length);
    size += values.length;
  }

  public void addAll(IntList intList)
  {
    reserve(intList.size);
    System.arraycopy(intList.baseArray, 0, baseArray, size, intList.size);
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
    if (size + reserve > baseArray.length) {
      baseArray = Arrays.copyOf(baseArray, Math.max(baseArray.length << 1, baseArray.length + reserve));
    }
  }

  public void set(int index, int value)
  {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    baseArray[index] = value;
  }

  public int get(int index)
  {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return baseArray[index];
  }

  public int indexOf(int value)
  {
    final int index = Ints.indexOf(baseArray, value);
    return index >= size ? -1 : index;
  }

  public int compact(int current)
  {
    final int remanining = size - current;
    if (current >= remanining) {
      System.arraycopy(baseArray, current, baseArray, 0, remanining);
      size = remanining;
      current = 0;
    }
    return current;
  }

  public IntList sort()
  {
    Arrays.sort(baseArray, 0, size);
    return this;
  }

  public int forward(int value)
  {
    add(value);
    return value;
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
        return function.apply(baseArray[x++]);
      }
    };
  }

  @JsonValue
  public int[] array()
  {
    return Arrays.copyOfRange(baseArray, 0, size);
  }

  public void shuffle()
  {
    shuffle(new Random());
  }

  public void shuffle(Random r)
  {
    if (size <= 1) {
      return;
    }
    for (int i = size; i > 1; i--) {
      final int from = i - 1;
      final int to = r.nextInt(i);
      final int x = baseArray[from];
      baseArray[from] = baseArray[to];
      baseArray[to] = x;
    }
  }

  public IntList concat(IntList other)
  {
    final int length = size + other.size;
    if (baseArray.length >= length) {
      System.arraycopy(other.baseArray, 0, baseArray, size, other.size);
      this.size = length;
    } else {
      final int[] concat = Arrays.copyOf(baseArray, length);
      System.arraycopy(other.baseArray, 0, concat, size, other.size);
      this.baseArray = concat;
      this.size = length;
    }
    return this;
  }

  public IntStream stream()
  {
    return size == 0 ? IntStream.empty() : IntStream.of(array());
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
      return IntIterators.singleton(baseArray[0]);
    }
    return IntIterators.wrap(baseArray, 0, size);
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
      if (baseArray[i] != other.baseArray[i]) {
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
