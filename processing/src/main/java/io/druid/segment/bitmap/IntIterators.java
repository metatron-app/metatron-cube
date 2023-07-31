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
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.collections.IntList;
import io.druid.segment.Cursor;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

// values should be >= 0 and sorted (all for handling bitmap iterator)
public final class IntIterators
{
  public static <T> Iterator<T> transfrom(IntIterator iterator, IntFunction<T> function)
  {
    return new Iterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public T next()
      {
        return function.apply(iterator.next());
      }
    };
  }

  public static int count(IntIterator iterator)
  {
    int c = 0;
    for (;iterator.hasNext(); iterator.next()) {
      c++;
    }
    return c;
  }

  public static abstract class Abstract implements IntIterator
  {
    @Override
    public IntIterator clone()
    {
      throw new UnsupportedOperationException();
    }
  }

  public static final IntIterator EMPTY = new Abstract()
  {
    @Override
    public boolean hasNext()
    {
      return false;
    }

    @Override
    public int next()
    {
      return -1;
    }
  };

  public static IntIterator upto(int limit)
  {
    return limit == 0 ? EMPTY : new UpTo(limit);
  }

  public static IntIterator from(IntList ids)
  {
    return from(ids.unwrap(), ids.size());
  }

  public static IntIterator from(int[] array)
  {
    return from(array, array.length);
  }

  public static IntIterator from(int[] array, int limit)
  {
    return from(array, 0, limit);
  }

  public static IntIterator from(int[] array, int start, int limit)
  {
    return new IntIterators.FromArray(array, start, limit);
  }

  public static IntIterator to(int end)
  {
    return fromTo(0, end);
  }

  public static IntIterator fromTo(int start, int end)
  {
    return start >= end ? EMPTY : new Range(start, end);
  }

  public static IntIterator advanceTo(IntIterator iterator, IntPredicate predicate)
  {
    if (!iterator.hasNext()) {
      return iterator;
    }
    final Peekable peekable = new Peekable(iterator);
    for (; predicate.test(peekable.peek()); peekable.next()) ;
    return peekable;
  }

  public static IntIterator sort(IntIterator... iterators)
  {
    return sort(Arrays.asList(iterators));
  }

  public static IntIterator sort(List<IntIterator> iterators)
  {
    return iterators.size() == 0
           ? EMPTY
           : iterators.size() == 1 ? iterators.get(0) : new IntIterators.Sorted(iterators);
  }

  public static IntIterator and(IntIterator... iterators)
  {
    return and(Arrays.asList(iterators));
  }

  public static IntIterator and(List<IntIterator> iterators)
  {
    return iterators.size() == 0 ? EMPTY : iterators.size() == 1 ? iterators.get(0) : new IntIterators.AND(iterators);
  }

  public static IntIterator or(IntIterator... iterators)
  {
    return or(Arrays.asList(iterators));
  }

  public static IntIterator or(List<IntIterator> iterators)
  {
    return iterators.size() == 0 ? EMPTY : iterators.size() == 1 ? iterators.get(0) : new IntIterators.OR(iterators);
  }

  public static IntIterator not(IntIterator iterator, int limit)
  {
    return new IntIterators.NOT(iterator, limit);
  }

  public static IntIterator diff(IntIterator iterator, IntIterator except)
  {
    return new IntIterators.DIFF(iterator, except);
  }

  public static IntIterator map(IntIterator iterator, int[] mapping)
  {
    return new IntIterators.Mapped(iterator, mapping);
  }

  public static IntIterator map(IntIterator iterator, Int2IntFunction mapping)
  {
    return new IntIterators.Delegated(iterator)
    {
      @Override
      public int next() {return mapping.applyAsInt(delegate.next());}
    };
  }

  public static <T> Iterator<T> apply(IntIterator iterator, IntFunction<T> function)
  {
    return new Iterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public T next()
      {
        return function.apply(iterator.next());
      }
    };
  }

  public static void forEach(IntIterator iterator, IntConsumer consumer)
  {
    while (iterator.hasNext()) {
      consumer.accept(iterator.next());
    }
  }

  // inclusive ~ exclusive
  public static final class Range implements IntIterator
  {
    private final int to;
    private int index;

    public Range(int from, int to)
    {
      this.to = to;
      this.index = from;
    }

    @Override
    public boolean hasNext()
    {
      return index < to;
    }

    @Override
    public int next()
    {
      return index < to ? index++ : -1;
    }

    @Override
    public IntIterator clone()
    {
      return new Range(index, to);
    }
  }

  public static final class FromArray extends Abstract
  {
    private int index;
    private final int[] array;
    private final int limit;

    public FromArray(int[] array, int limit)
    {
      this(array, 0, limit);
    }

    public FromArray(int[] array, int index, int limit)
    {
      this.array = array;
      this.limit = limit;
      this.index = index;
    }

    @Override
    public boolean hasNext()
    {
      return index < limit;
    }

    @Override
    public int next()
    {
      return index < limit ? array[index++] : -1;
    }

    @Override
    public IntIterator clone()
    {
      return new FromArray(array, index, limit);
    }
  }

  private static final class UpTo extends Abstract
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
      return index >= limit ? -1 : index++;
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

  public static PeekableIntIterator peek(IntIterator iterator)
  {
    return new Peekable(iterator);
  }

  private static final class Peekable extends Abstract implements PeekableIntIterator
  {
    private final IntIterator iterator;
    private boolean hasPeeked;
    private int peekedElement;

    public Peekable(IntIterator iterator) {this.iterator = iterator;}

    @Override
    public int peek()
    {
      if (!hasPeeked) {
        if (!iterator.hasNext()) {
          return -1;
        }
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
        return iterator.hasNext() ? iterator.next() : -1;
      }
      hasPeeked = false;
      return peekedElement;
    }
  }

  private static final class Sorted extends IntIterators.Abstract
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
              return Integer.compare(lhs.peek(), rhs.peek());
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

  private static final class OR extends AdvanceFirst
  {
    private final Peekable iterator;

    public OR(List<IntIterator> iterators)
    {
      this.iterator = new Peekable(sort(iterators));
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

  private static final class AND extends AdvanceFirst
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

  private static final class NOT extends AdvanceFirst
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

  private static final class DIFF extends AdvanceFirst
  {
    private final IntIterator iterator;
    private final Peekable other;

    public DIFF(IntIterator iterator, IntIterator other)
    {
      this.iterator = iterator;
      this.other = new Peekable(other);
      this.next = findNext(-1);
    }

    @Override
    public int findNext(int current)
    {
      while (iterator.hasNext()) {
        final int index = iterator.next();
        while (index > other.peek() && other.hasNext()) {
          other.next();
        }
        if (index != other.peek()) {
          return index;
        }
      }
      return -1;
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

  protected abstract static class AdvanceFirst extends Abstract
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

  private static final class Mapped extends Delegated
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

  public static IntIterator wrap(Cursor cursor)
  {
    if (cursor.isDone()) {
      return EMPTY;
    }
    return new IntIterator()
    {
      @Override
      public boolean hasNext()
      {
        return !cursor.isDone();
      }

      @Override
      public int next()
      {
        final int offset = cursor.offset();
        cursor.advance();
        return offset;
      }

      @Override
      public IntIterator clone()
      {
        throw new UnsupportedOperationException("clone");
      }
    };
  }

  public static boolean elementsEqual(IntIterator iterator1, IntIterator iterator2)
  {
    while (iterator1.hasNext()) {
      if (!iterator2.hasNext()) {
        return false;
      }
      if (iterator1.next() != iterator2.next()) {
        return false;
      }
    }
    return !iterator2.hasNext();
  }

  // iterator return >= 0
  public static IntIterator except(IntIterator iterator, ImmutableBitmap skip, int size)
  {
    if (skip == null || skip.isEmpty()) {
      return iterator;
    }
    if (iterator == null) {
      return not(skip.iterator(), size);
    }
    return new Abstract()
    {
      private int ix = advance();

      @Override
      public boolean hasNext()
      {
        return ix >= 0;
      }

      @Override
      public int next()
      {
        int next = ix;
        ix = advance();
        return next;
      }

      private int advance()
      {
        while (iterator.hasNext()) {
          int ix = iterator.next();
          if (!skip.get(ix)) {
            return ix;
          }
        }
        return -1;
      }
    };
  }

  public static IntIterator except(IntIterator iterator, IntPredicate predicate)
  {
    return new IntIterators.Abstract()
    {
      private final Peekable peekable = new Peekable(iterator);

      @Override
      public boolean hasNext()
      {
        return peekable.hasNext() && predicate.test(peekable.peek());
      }

      @Override
      public int next()
      {
        return peekable.next();
      }
    };
  }

  public static IntIterator include(IntIterator iterator, ImmutableBitmap include)
  {
    if (include == null) {
      return iterator;
    }
    if (include.isEmpty()) {
      return IntIterators.EMPTY;
    }
    if (iterator == null) {
      return include.iterator();
    }
    return new Abstract()
    {
      private int ix = advance();

      @Override
      public boolean hasNext()
      {
        return ix >= 0;
      }

      @Override
      public int next()
      {
        int next = ix;
        ix = advance();
        return next;
      }

      private int advance()
      {
        while (iterator.hasNext()) {
          int ix = iterator.next();
          if (include.get(ix)) {
            return ix;
          }
        }
        return -1;
      }
    };
  }

  public static IntStream stream(IntIterator iterator)
  {
    PrimitiveIterator.OfInt ints = new PrimitiveIterator.OfInt()
    {
      @Override
      public int nextInt()
      {
        return iterator.next();
      }

      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }
    };
    return StreamSupport.intStream(
        Spliterators.spliteratorUnknownSize(ints, Spliterator.NONNULL | Spliterator.IMMUTABLE), false
    );
  }

  public static IntStream filteredStream(IntIterator iterator, ImmutableBitmap skip, int size)
  {
    IntIterator filtered = IntIterators.except(iterator, skip, size);
    return filtered == null ? IntStream.range(0, size) : stream(iterator);
  }

  public static IntIterator from(IntIterable iterable, int size)
  {
    return Optional.ofNullable(iterable.iterator()).orElseGet(() -> fromTo(0, size));
  }
}
