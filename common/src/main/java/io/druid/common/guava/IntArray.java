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

package io.druid.common.guava;

import java.util.Arrays;

public class IntArray implements Comparable<IntArray>
{
  private final int[] array;

  public IntArray(int[] array)
  {
    this.array = array;
  }

  public int[] array()
  {
    return array;
  }

  @Override
  public int hashCode()
  {
    return array.length == 1 ? array[0] : Arrays.hashCode(array);
  }

  @Override
  public boolean equals(Object obj)
  {
    final int[] other = ((IntArray) obj).array;
    return array.length == 1 ? array[0] == other[0] : Arrays.equals(array, other);
  }

  @Override
  public int compareTo(IntArray o)
  {
    for (int i = 0; i < array.length; i++) {
      final int compare = Integer.compare(array[i], o.array[i]);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }
}
