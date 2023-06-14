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

package io.druid.utils;

/**
 * An implementation of the core algorithm of HeapSort. (copied from Hadoop)
 */
public class HeapSort
{
  public static void sort(final long[] k, final int p, final int r)
  {
    final int N = r - p;
    // build heap w/ reverse comparator, then write in-place from end
    final int t = Integer.highestOneBit(N);
    for (int i = t; i > 1; i >>>= 1) {
      for (int j = i >>> 1; j < i; ++j) {
        downHeap(k, p - 1, j, N + 1);
      }
    }
    for (int i = r - 1; i > p; --i) {
      swap(k, p, i);
      downHeap(k, p - 1, 1, i - p + 1);
    }
  }

  private static void downHeap(final long[] k, final int b, int i, final int N)
  {
    for (int idx = i << 1; idx < N; idx = i << 1) {
      if (idx + 1 < N && k[b + idx] < k[b + idx + 1]) {
        if (k[b + i] < k[b + idx + 1]) {
          swap(k, b + i, b + idx + 1);
        } else {
          return;
        }
        i = idx + 1;
      } else if (k[b + i] < k[b + idx]) {
        swap(k, b + i, b + idx);
        i = idx;
      } else {
        return;
      }
    }
  }

  private static void swap(final long[] k, final int a, final int b)
  {
    long ka = k[a];
    k[a] = k[b];
    k[b] = ka;
  }

  public static void sort(final long[] k, final int[] v, final int p, final int r)
  {
    final int N = r - p;
    // build heap w/ reverse comparator, then write in-place from end
    final int t = Integer.highestOneBit(N);
    for (int i = t; i > 1; i >>>= 1) {
      for (int j = i >>> 1; j < i; ++j) {
        downHeap(k, v, p - 1, j, N + 1);
      }
    }
    for (int i = r - 1; i > p; --i) {
      swap(k, v, p, i);
      downHeap(k, v, p - 1, 1, i - p + 1);
    }
  }

  private static void downHeap(final long[] k, final int[] v, final int b, int i, final int N)
  {
    for (int idx = i << 1; idx < N; idx = i << 1) {
      if (idx + 1 < N && k[b + idx] < k[b + idx + 1]) {
        if (k[b + i] < k[b + idx + 1]) {
          swap(k, v, b + i, b + idx + 1);
        } else {
          return;
        }
        i = idx + 1;
      } else if (k[b + i] < k[b + idx]) {
        swap(k, v, b + i, b + idx);
        i = idx;
      } else {
        return;
      }
    }
  }

  private static void swap(final long[] k, final int[] v, final int a, final int b)
  {
    long ka = k[a];
    k[a] = k[b];
    k[b] = ka;
    int va = v[a];
    v[a] = v[b];
    v[b] = va;
  }
}
