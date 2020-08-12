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

package io.druid.segment.bitmap;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.collections.IntList;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.roaringbitmap.IntIterator;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class IntIterators
{
  public static abstract class Abstract implements IntIterator
  {
    @Override
    public IntIterator clone()
    {
      throw new UnsupportedOperationException();
    }
  }

  public static IntIterator EMPTY = new Abstract()
  {
    @Override
    public boolean hasNext()
    {
      return false;
    }

    @Override
    public int next()
    {
      throw new NoSuchElementException();
    }
  };

  public static class Range implements IntIterator
  {
    private final int from;
    private final int to;
    private int index;

    public Range(int from, int to)
    {
      this.from = from;
      this.to = to;
      this.index = from;
    }

    @Override
    public boolean hasNext()
    {
      return index <= to;    // inclusive
    }

    @Override
    public int next()
    {
      return index <= to ? index++ : -1;
    }

    @Override
    public IntIterator clone()
    {
      return new Range(index, to);
    }

    public void union(BitSet bitSet)
    {
      bitSet.set(from, to + 1);   // exclusive
    }
  }

  public static class FromArray extends Abstract
  {
    private int index;
    private final int[] array;

    public FromArray(int[] array)
    {
      this(array, 0);
    }

    public FromArray(int[] array, int index)
    {
      this.array = array;
      this.index = index;
    }

    @Override
    public boolean hasNext()
    {
      return index < array.length;
    }

    @Override
    public int next()
    {
      return index < array.length ? array[index++] : -1;
    }

    @Override
    public IntIterator clone()
    {
      return new FromArray(array, index);
    }
  }

  public static class UpTo extends Abstract
  {
    private final int limit;

    private int index;

    public UpTo(int limit) {this.limit = limit;}

    @Override
    public boolean hasNext()
    {
      return index < limit;
    }
    @Override
    public int next()
    {
      if (index >= limit) {
        throw new NoSuchElementException();
      }
      return index++;
    }

  }

  public static int[] toArray(final IntIterator iterator)
  {
    final IntList list = new IntList();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list.array();
  }

  public static class Peekable extends Abstract
  {
    private final IntIterator iterator;
    private boolean hasPeeked;
    private int peekedElement;

    public Peekable(IntIterator iterator) {this.iterator = iterator;}

    public int peek()
    {
      if (!hasPeeked) {
        peekedElement = iterator.next();
        hasPeeked = true;
      }
      return peekedElement;
    }

    @Override
    public boolean hasNext()
    {
      return hasPeeked || iterator.hasNext();
    }

    @Override
    public int next()
    {
      if (!hasPeeked) {
        return iterator.next();
      }
      hasPeeked = false;
      return peekedElement;
    }
  }

  // values should be sorted
  public static class Sorted extends IntIterators.Abstract
  {
    final PriorityQueue<Peekable> pQueue;

    public Sorted(List<IntIterator> iterators)
    {
      pQueue = new ObjectHeapPriorityQueue<>(
          iterators.size(),
          new Comparator<Peekable>()
          {
            @Override
            public int compare(Peekable lhs, Peekable rhs)
            {
              return Ints.compare(lhs.peek(), rhs.peek());
            }
          }
      );
      for (IntIterator iterator : iterators) {
        Peekable peekable = new Peekable(iterator);
        if (peekable.hasNext()) {
          pQueue.enqueue(peekable);
        }
      }
    }

    @Override
    public boolean hasNext()
    {
      return !pQueue.isEmpty();
    }

    @Override
    public int next()
    {
      final Peekable peekable = pQueue.first();
      final int value = peekable.next();
      if (peekable.hasNext()) {
        pQueue.changed();
      } else {
        pQueue.dequeue();
      }
      return value;
    }
  }

  // values should be >= 0 and sorted (for bitmap)
  public static class OR extends NextFirst
  {
    private final Peekable iterator;

    public OR(List<IntIterator> iterators)
    {
      this.iterator = new Peekable(new Sorted(iterators));
      this.next = findNext(-1);
    }

    @Override
    public int findNext(int current)
    {
      for (; iterator.hasNext() && current == iterator.peek(); iterator.next()) {
      }
      return iterator.hasNext() ? iterator.next() : -1;
    }
  }

  // values should be >= 0 and sorted (for bitmap)
  public static class AND extends NextFirst
  {
    private static final int EOF = Integer.MIN_VALUE;

    private final Peekable[] iterators;

    public AND(List<IntIterator> iterators)
    {
      boolean hasMore = !iterators.isEmpty();
      List<Peekable> peekables = Lists.newArrayList();
      for (IntIterator iterator : iterators) {
        Peekable peekable = new Peekable(iterator);
        hasMore &= peekable.hasNext();
        peekables.add(peekable);
      }
      this.iterators = peekables.toArray(new Peekable[0]);
      this.next = hasMore ? findNext(-1) : EOF;
    }

    @Override
    protected int findNext(int current)
    {
      int start = 0;
      for (int next = _findNext(start); start != next; next = _findNext(start = -next - 1)) {
        if (next == EOF) {
          return EOF;
        }
      }
      int ret = iterators[start].peek();
      iterators[start].next();
      return ret;
    }

    private int _findNext(int start)
    {
      if (!iterators[start].hasNext()) {
        return EOF;
      }
      final int current = iterators[start].peek();
      final int end = start + iterators.length;

      for (int i = start + 1; i < end; i++) {
        final int index = i % iterators.length;
        final Peekable peekable = iterators[index];
        if (!peekable.hasNext()) {
          return EOF;
        }
        while (peekable.peek() < current) {
          peekable.next();
          if (!peekable.hasNext()) {
            return EOF;    // end
          }
        }
        if (peekable.peek() > current) {
          return -index - 1;
        }
      }
      return start;
    }
  }

  // values should be >= 0 and sorted (for bitmap)
  public static class NOT extends NextFirst
  {
    private final IntIterator iterator;
    private final int limit;
    private int avoid;

    public NOT(IntIterator iterator, int limit)
    {
      this.iterator = iterator;
      this.limit = limit;
      this.avoid = iterator.hasNext() ? iterator.next() : limit;
      this.next = findNext(-1);
    }

    @Override
    protected int findNext(int current)
    {
      int next = current + 1;
      while (next == avoid && next < limit) {
        avoid = iterator.hasNext() ? iterator.next() : limit;
        next++;
      }
      return next < limit ? next : -1;
    }
  }

  public static class Delegated implements IntIterator
  {
    final IntIterator delegate;

    public Delegated(IntIterator delegate) {this.delegate = delegate;}

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext();
    }

    @Override
    public int next()
    {
      return delegate.next();
    }

    @Override
    public IntIterator clone()
    {
      return new Delegated(delegate.clone());
    }
  }

  protected abstract static class NextFirst extends Abstract
  {
    int next;

    @Override
    public boolean hasNext()
    {
      return next >= 0;
    }

    @Override
    public int next()
    {
      if (next < 0) {
        return -1;
      }
      final int ret = next;
      next = findNext(ret);
      return ret;
    }

    protected abstract int findNext(int current);
  }

  public static class Mapped extends Delegated
  {
    private final int[] conversion;

    public Mapped(IntIterator delegate, int[] conversion)
    {
      super(delegate);
      this.conversion = conversion;
    }

    @Override
    public int next()
    {
      return conversion[delegate.next()];
    }

    @Override
    public IntIterator clone()
    {
      return new Mapped(delegate.clone(), conversion);
    }
  }
}
