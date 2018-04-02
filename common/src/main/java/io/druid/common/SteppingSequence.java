/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
 *
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

package io.druid.common;

import com.google.common.base.Preconditions;
import com.metamx.common.guava.DelegatingYieldingAccumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;

import java.io.IOException;

/**
 * copied from com.metamx.common.guava.LimitedSequence
 *
 * Limits the number of inputs from this sequence.  For example, if there are actually 100 things in the sequence
 * but the limit is set to 10, the Sequence will act as if it only had 10 things.
 */
public abstract class SteppingSequence<T> extends YieldingSequenceBase<T>
{
  private final Sequence<T> baseSequence;

  public SteppingSequence(
      Sequence<T> baseSequence
  )
  {
    Preconditions.checkNotNull(baseSequence);

    this.baseSequence = baseSequence;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final SteppingYieldingAccumulator<OutType> limitedAccumulator = new SteppingYieldingAccumulator<>(
        accumulator
    );
    final Yielder<OutType> subYielder = baseSequence.toYielder(initValue, limitedAccumulator);
    return new SteppingYielder<>(subYielder, limitedAccumulator);
  }

  private class SteppingYielder<OutType> implements Yielder<OutType>
  {
    private final Yielder<OutType> subYielder;
    private final SteppingYieldingAccumulator<OutType> limitedAccumulator;

    public SteppingYielder(
        Yielder<OutType> subYielder,
        SteppingYieldingAccumulator<OutType> limitedAccumulator
    )
    {
      this.subYielder = subYielder;
      this.limitedAccumulator = limitedAccumulator;
    }

    @Override
    public OutType get()
    {
      return subYielder.get();
    }

    @Override
    public Yielder<OutType> next(OutType initValue)
    {
      if (!withinThreshold()) {
        return Yielders.done(initValue, subYielder);
      }

      Yielder<OutType> next = subYielder.next(initValue);
      if (!withinThreshold() && (!limitedAccumulator.yielded() || limitedAccumulator.isInterruptYield())) {
        next = Yielders.done(next.get(), next);
      }
      return new SteppingYielder<>(next, limitedAccumulator);
    }

    @Override
    public boolean isDone()
    {
      return subYielder.isDone() || (
          !withinThreshold() && (!limitedAccumulator.yielded() || limitedAccumulator.isInterruptYield())
      );
    }

    @Override
    public void close() throws IOException
    {
      subYielder.close();
    }
  }

  private class SteppingYieldingAccumulator<OutType> extends DelegatingYieldingAccumulator<OutType, T>
  {
    private boolean interruptYield = false;

    public SteppingYieldingAccumulator(YieldingAccumulator<OutType, T> accumulator)
    {
      super(accumulator);
    }

    @Override
    public OutType accumulate(OutType accumulated, T in)
    {
      accumulating(in);

      if (!withinThreshold()) {
        // yield to interrupt the sequence
        interruptYield = true;
      }

      // if delegate yields as well we need to distinguish between the two yields
      final OutType retVal = super.accumulate(accumulated, in);
      if (yielded() && interruptYield) {
        interruptYield = false;
      }
      if (interruptYield) {
        yield();
      }

      return retVal;
    }

    public boolean isInterruptYield()
    {
      return interruptYield;
    }
  }

  protected abstract void accumulating(T in);

  protected abstract boolean withinThreshold();

  public static class Limit<T> extends SteppingSequence<T>
  {
    protected final int limit;
    protected int count;

    public Limit(Sequence<T> baseSequence, int limit)
    {
      super(baseSequence);
      Preconditions.checkArgument(limit >= 0, "limit is negative");
      this.limit = limit;
    }

    @Override
    protected void accumulating(T in)
    {
      count++;
    }

    protected boolean withinThreshold()
    {
      return count < limit;
    }
  }
}
