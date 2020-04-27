/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeaps;

import java.io.IOException;

/**
 */
public class MergeSequence<T> extends YieldingSequenceBase<T>
{
  private final Ordering<T> ordering;
  private final Sequence<Sequence<T>> baseSequences;

  public MergeSequence(
      Ordering<T> ordering,
      Sequence<Sequence<T>> baseSequences
  )
  {
    this.ordering = ordering;
    this.baseSequences = baseSequences;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    Hack<Yielder<T>> pQueue = new Hack<>(
        32,
        ordering.onResultOf(
            new Function<Yielder<T>, T>()
            {
              @Override
              public T apply(Yielder<T> input)
              {
                return input.get();
              }
            }
        )
    );

    pQueue = baseSequences.accumulate(
        pQueue,
        new Accumulator<Hack<Yielder<T>>, Sequence<T>>()
        {
          @Override
          public Hack<Yielder<T>> accumulate(Hack<Yielder<T>> queue, Sequence<T> in)
          {
            final Yielder<T> yielder = Yielders.each(in);

            if (yielder.isDone()) {
              Yielders.close(yielder);
            } else {
              queue.enqueue(yielder);
            }

            return queue;
          }
        }
    );

    return makeYielder(pQueue, initValue, accumulator);
  }

  private <OutType> Yielder<OutType> makeYielder(
      final Hack<Yielder<T>> pQueue,
      final OutType initVal,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    OutType retVal = initVal;
    while (!accumulator.yielded() && !pQueue.isEmpty()) {
      Yielder<T> yielder = pQueue.first();
      retVal = accumulator.accumulate(retVal, yielder.get());
      yielder = yielder.next(null);
      if (yielder.isDone()) {
        Yielders.close(yielder);
        pQueue.dequeue();
      } else {
        pQueue.changedTo(yielder);
      }
    }

    if (pQueue.isEmpty() && !accumulator.yielded()) {
      return Yielders.done(retVal, null);
    }

    final OutType yieldVal = retVal;
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yieldVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.reset();
        return makeYielder(pQueue, initValue, accumulator);
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        while (!pQueue.isEmpty()) {
          pQueue.dequeue().close();
        }
      }
    };
  }

  private static class Hack<K> extends ObjectHeapPriorityQueue<K>
  {
    public Hack(int capacity, Ordering<K> comparator)
    {
      super(capacity, comparator);
    }

    public void changedTo(K changed)
    {
      heap[0] = changed;
      ObjectHeaps.downHeap(heap, size, 0, c);
    }
  }
}
