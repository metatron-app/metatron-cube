/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections;

import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 */
public class IntList
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

  @Override
  public String toString()
  {
    return Arrays.toString(compact());
  }
}
