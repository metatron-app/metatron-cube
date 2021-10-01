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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.druid.java.util.common.guava.FunctionalIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * An OrderedMergeIterator is an iterator that merges together multiple sorted iterators.  It is written assuming
 * that the input Iterators are provided in order.  That is, it places an extra restriction in the input iterators.
 *
 * Normally a merge operation could operate with the actual input iterators in any order as long as the actual values
 * in the iterators are sorted.  This requires that not only the individual values be sorted, but that the iterators
 * be provided in the order of the first element of each iterator.
 *
 * If this doesn't make sense, check out OrderedMergeIteratorTest.testScrewsUpOnOutOfOrderBeginningOfList()
 *
 * It places this extra restriction on the input data in order to implement an optimization that allows it to
 * remain as lazy as possible in the face of a common case where the iterators are just appended one after the other.
 */
public class OrderedMergeIterator<T> implements Iterator<T>
{
  private final PriorityQueue<PeekingIterator<T>> pQueue;

  private final PeekingIterator<PeekingIterator<T>> iterOfIterators;
  private final Comparator<T> comparator;

  public OrderedMergeIterator(
      final Comparator<T> comparator,
      Iterator<Iterator<T>> iterators
  )
  {
    this.comparator = comparator;
    pQueue = new PriorityQueue<PeekingIterator<T>>(
        16,
        new Comparator<PeekingIterator<T>>()
        {
          @Override
          public int compare(PeekingIterator<T> lhs, PeekingIterator<T> rhs)
          {
            return comparator.compare(lhs.peek(), rhs.peek());
          }
        }
    );

    iterOfIterators = Iterators.peekingIterator(
        FunctionalIterator.create(iterators)
                          .filter(
                              new Predicate<Iterator<T>>()
                              {
                                @Override
                                public boolean apply(Iterator<T> input)
                                {
                                  return input.hasNext();
                                }
                              }
                          )
                          .transform(
                              new Function<Iterator<T>, PeekingIterator<T>>()
                              {
                                @Override
                                public PeekingIterator<T> apply(Iterator<T> input)
                                {
                                  return Iterators.peekingIterator(input);
                                }
                              }
                          )
    );
  }

  @Override
  public boolean hasNext()
  {
    return !pQueue.isEmpty() || iterOfIterators.hasNext();
  }

  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final PeekingIterator<T> it;
    if (!iterOfIterators.hasNext()) {
      it = pQueue.remove();
    } else if (pQueue.isEmpty()) {
      it = iterOfIterators.next();
    } else {
      T pQueueValue = pQueue.peek().peek();
      T iterItersValue = iterOfIterators.peek().peek();

      if (comparator.compare(pQueueValue, iterItersValue) <= 0) {
        it = pQueue.remove();
      } else {
        it = iterOfIterators.next();
      }
    }

    T retVal = it.next();

    if (it.hasNext()) {
      pQueue.add(it);
    }

    return retVal;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
