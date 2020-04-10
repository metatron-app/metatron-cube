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

package io.druid.query.groupby.orderby;

import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * A utility class that sorts a list of comparable items in the given order, and keeps only the
 * top N sorted items.
 */
public class TopNSorter<T>
{
  public static <T> Iterator<T> topN(Comparator<T> ordering, Sequence<T> items, int n)
  {
    return new TopNSorter<>(ordering).toTopN(items, n);
  }

  public static <T> Iterator<T> topN(Comparator<T> ordering, Iterable<T> items, int n)
  {
    return new TopNSorter<>(ordering).toTopN(items, n);
  }

  private Comparator<T> ordering;

  /**
   * Constructs a sorter that will sort items with given ordering.
   * @param ordering the order that this sorter instance will use for sorting
   */
  public TopNSorter(Comparator<T> ordering)
  {
    this.ordering = ordering;
  }

  /**
   * Sorts a list of rows and retain the top n items
   * @param items the collections of items to be sorted
   * @param n the number of items to be retained
   * @return Top n items that are sorted in the order specified when this instance is constructed.
   */
  public Iterator<T> toTopN(Iterable<T> items, int n)
  {
    if(n <= 0) {
      return Collections.emptyIterator();
    }

    MinMaxPriorityQueue<T> queue = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(n).create(items);

    return new OrderedPriorityQueueItems<T>(queue);
  }

  public Iterator<T> toTopN(Sequence<T> items, int n)
  {
    if(n <= 0) {
      return Collections.emptyIterator();
    }

    final MinMaxPriorityQueue<T> queue = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(n).create();
    items.accumulate(
        queue, new Accumulator<MinMaxPriorityQueue<T>, T>()
        {
          @Override
          public MinMaxPriorityQueue<T> accumulate(MinMaxPriorityQueue<T> accumulated, T in)
          {
            accumulated.offer(in);
            return accumulated;
          }
        }
    );

    return new OrderedPriorityQueueItems<T>(queue);
  }
}
