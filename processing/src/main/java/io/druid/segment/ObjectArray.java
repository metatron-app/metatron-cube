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

package io.druid.segment;

import io.druid.common.guava.GuavaUtils;

import java.util.Arrays;
import java.util.Comparator;

/**
 */
public class ObjectArray<T> implements Comparable<ObjectArray<T>>
{
  protected final T[] array;

  public ObjectArray(T[] array)
  {
    this.array = array;
  }

  public int length()
  {
    return array.length;
  }

  public T[] array()
  {
    return array;
  }

  public T get(int index)
  {
    return array[index];
  }

  public ObjectArray<T> pack(T[] values)
  {
    System.arraycopy(values, 0, array, 0, values.length);
    return this;
  }

  public String concat(String delimiter, String postfix)
  {
    StringBuilder b = new StringBuilder();
    for (Object element : array) {
      if (b.length() > 0) {
        b.append(delimiter);
      }
      b.append(element);
    }
    return b.append(delimiter).append(postfix).toString();
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof ObjectArray && Arrays.equals(array, ((ObjectArray) o).array);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(array);
  }

  @Override
  public String toString()
  {
    return Arrays.toString(array);
  }

  private static final Comparator comparator = GuavaUtils.nullFirstNatural();

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(ObjectArray<T> o)
  {
    for (int i = 0; i < array.length; i++) {
      final int compare = comparator.compare(array[i], o.array[i]);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }
}
