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

import io.druid.common.Yielders;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class CombiningSequence<T, V> implements Sequence<V>
{
  public static <T, V> Sequence<V> create(Sequence<T> sequence, Comparator<T> ordering, BinaryFn.Identical<T> mergeFn)
  {
    if (mergeFn instanceof CombineFn) {
      return create(sequence, ordering, (CombineFn<T, V>) mergeFn);
    }
    return create(sequence, ordering, (v1, v2) -> mergeFn.apply(v1, v2));
  }

  public static <T, V> Sequence<V> create(
      final Sequence<T> sequence,
      final Comparator<T> ordering,
      final CombineFn<T, V> mergeFn
  )
  {
    if (ordering == null) {
      return Sequences.lazy(sequence.columns(), () -> {
        final T accumulated = Sequences.accumulate(sequence, mergeFn);
        return accumulated == null ? Sequences.empty() : Sequences.of(mergeFn.done(accumulated));
      });
    }
    return new CombiningSequence<T, V>(sequence, ordering, mergeFn);
  }

  private final Sequence<T> baseSequence;
  private final Comparator<T> ordering;
  private final CombineFn<T, V> mergeFn;

  public CombiningSequence(Sequence<T> baseSequence, Comparator<T> ordering, CombineFn<T, V> mergeFn)
  {
    this.baseSequence = baseSequence;
    this.ordering = ordering;
    this.mergeFn = mergeFn;
  }

  @Override
  public List<String> columns()
  {
    return baseSequence.columns();
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, V> accumulator)
  {
    final AtomicReference<OutType> retVal = new AtomicReference<OutType>(initValue);
    final CombiningAccumulator<OutType> combiningAccumulator = new CombiningAccumulator<OutType>(retVal, accumulator);
    T lastValue = baseSequence.accumulate(null, combiningAccumulator);
    return combiningAccumulator.accumulatedSomething() ? accumulator.accumulate(retVal.get(), mergeFn.done(lastValue)) : initValue;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, V> accumulator)
  {
    final CombiningYieldingAccumulator<OutType, T, V> combiningAccumulator = new CombiningYieldingAccumulator<OutType, T, V>(
        ordering, mergeFn, initValue, accumulator
    );
    Yielder<T> baseYielder = baseSequence.toYielder(null, combiningAccumulator);

    return makeYielder(baseYielder, combiningAccumulator, false);
  }

  public <OutType> Yielder<OutType> makeYielder(
      final Yielder<T> yielder,
      final CombiningYieldingAccumulator<OutType, T, V> combiningAccumulator,
      final boolean finalValue
  )
  {
    final Yielder<T> finalYielder;
    final OutType retVal;
    final boolean finalFinalValue;

    if (yielder.isDone()) {
      if (finalValue || combiningAccumulator.lastMerged == null) {
        return Yielders.done(combiningAccumulator.lastReturn, yielder);
      }
      retVal = combiningAccumulator.accumulateRemainings();
      if (!combiningAccumulator.yielded()) {
        return Yielders.done(retVal, yielder);
      }
      finalYielder = Yielders.done(null, yielder);
      finalFinalValue = true;
    } else {
      retVal = combiningAccumulator.lastReturn;
      finalYielder = null;
      finalFinalValue = false;
    }

    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return retVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        combiningAccumulator.reset();
        return makeYielder(
            finalYielder == null ? yielder.next(yielder.get()) : finalYielder,
            combiningAccumulator,
            finalFinalValue
        );
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        yielder.close();
      }
    };
  }

  private static class CombiningYieldingAccumulator<OutType, T, V> extends YieldingAccumulator<T, T>
  {
    private final Comparator<T> ordering;
    private final CombineFn<T, V> mergeFn;
    private final YieldingAccumulator<OutType, V> accumulator;

    private T lastMerged;
    private OutType lastReturn;

    public CombiningYieldingAccumulator(
        Comparator<T> ordering,
        CombineFn<T, V> mergeFn,
        OutType initValue,
        YieldingAccumulator<OutType, V> accumulator
    )
    {
      this.ordering = ordering;
      this.mergeFn = mergeFn;
      this.lastReturn = initValue;
      this.accumulator = accumulator;
    }

    @Override
    public void reset()
    {
      accumulator.reset();
    }

    @Override
    public boolean yielded()
    {
      return accumulator.yielded();
    }

    @Override
    public void yield()
    {
      accumulator.yield();
    }

    @Override
    public T accumulate(T prev, T current)
    {
      if (prev == null || ordering.compare(prev, current) == 0) {
        return lastMerged = mergeFn.apply(lastMerged, current);
      }
      lastReturn = accumulator.accumulate(lastReturn, mergeFn.done(lastMerged));
      lastMerged = mergeFn.apply(null, current);
      return current;
    }

    public OutType accumulateRemainings()
    {
      return lastReturn = accumulator.accumulate(lastReturn, mergeFn.done(lastMerged));
    }
  }

  private class CombiningAccumulator<OutType> implements Accumulator<T, T>
  {
    private final AtomicReference<OutType> retVal;
    private final Accumulator<OutType, V> accumulator;

    private volatile boolean accumulatedSomething = false;

    public CombiningAccumulator(AtomicReference<OutType> retVal, Accumulator<OutType, V> accumulator)
    {
      this.retVal = retVal;
      this.accumulator = accumulator;
    }

    public boolean accumulatedSomething()
    {
      return accumulatedSomething;
    }

    @Override
    public T accumulate(T prevValue, T t)
    {
      if (!accumulatedSomething) {
        accumulatedSomething = true;
      }

      if (prevValue == null || ordering.compare(prevValue, t) == 0) {
        return mergeFn.apply(prevValue, t);
      }

      retVal.set(accumulator.accumulate(retVal.get(), mergeFn.done(prevValue)));
      return t;
    }
  }
}
