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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 */
public class IntList implements Iterable<Integer>
{
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

  public void addAll(IntList comprisedRows)
  {
    reserve(comprisedRows.size);
    System.arraycopy(comprisedRows.baseArray, 0, baseArray, size, comprisedRows.size);
    size += comprisedRows.size;
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

  public int[] compact()
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

  @Override
  public Iterator<Integer> iterator()
  {
    return Ints.asList(compact()).iterator();
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
  public String toString()
  {
    return Arrays.toString(compact());
  }
}
