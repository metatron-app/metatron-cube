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

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.roaringbitmap.IntIterator;

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

  public static class Sorted extends IntIterators.Abstract
  {
    private final PriorityQueue<Peekable> pQueue;

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
