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

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeaps;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 */
public abstract class BaseMergeSequence<T> extends YieldingSequenceBase<T>
{
  protected final Hack<Yielder<T>> createQueue(Comparator<T> ordering, List<Yielder<T>> yielders)
  {
    final Hack<Yielder<T>> queue = createQueue(ordering, yielders.size());
    for (Yielder<T> yielder : yielders) {
      queue.enqueue(yielder);
    }
    return queue;
  }

  protected final Hack<Yielder<T>> createQueue(Comparator<T> ordering, int capacity)
  {
    return new Hack<>(capacity, (left, right) -> ordering.compare(left.get(), right.get()));
  }

  protected final <OutType> Yielder<OutType> makeYielder(
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
          Yielders.close(pQueue.dequeue());
        }
      }
    };
  }

  private static final class Hack<K> extends ObjectHeapPriorityQueue<K>
  {
    public Hack(int capacity, Comparator<K> comparator)
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
