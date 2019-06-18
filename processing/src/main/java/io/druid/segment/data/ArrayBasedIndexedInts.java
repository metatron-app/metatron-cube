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

import java.io.IOException;
import java.util.Iterator;

/**
*/
public class ArrayBasedIndexedInts implements IndexedInts
{
  private final int[] expansion;

  public ArrayBasedIndexedInts(int[] expansion) {this.expansion = expansion;}

  @Override
  public int size()
  {
    return expansion.length;
  }

  @Override
  public int get(int index)
  {
    return expansion[index];
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new IndexedIntsIterator(this);
  }

  @Override
  public void fill(int index, int[] toFill)
  {
    throw new UnsupportedOperationException("fill not supported");
  }

  @Override
  public void close() throws IOException
  {

  }

  public static int[] toIntArray(IndexedInts indexed)
  {
    if (indexed instanceof ArrayBasedIndexedInts) {
      return ((ArrayBasedIndexedInts) indexed).expansion;
    }
    int[] array = new int[indexed.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = indexed.get(i);
    }
    return array;
  }
}
