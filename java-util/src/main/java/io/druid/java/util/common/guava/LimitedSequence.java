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

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Limits the number of inputs from this sequence.  For example, if there are actually 100 things in the sequence
 * but the limit is set to 10, the Sequence will act as if it only had 10 things.
 */
public class LimitedSequence<T> extends YieldingSequenceBase<T>
{
  private final Sequence<T> baseSequence;
  private final int limit;

  public LimitedSequence(
      Sequence<T> baseSequence,
      int limit
  )
  {
    Preconditions.checkNotNull(baseSequence);
    Preconditions.checkArgument(limit >= 0, "limit is negative");

    this.baseSequence = baseSequence;
    this.limit = limit;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final LimitedYieldingAccumulator<OutType, T> limited = new LimitedYieldingAccumulator<>(accumulator, limit);
    final Yielder<OutType> subYielder = baseSequence.toYielder(initValue, limited);
    return new LimitedYielder<>(subYielder, limited);
  }

  private class LimitedYielder<OutType> implements Yielder<OutType>
  {
    private final Yielder<OutType> subYielder;
    private final LimitedYieldingAccumulator<OutType, T> limitedAccumulator;

    public LimitedYielder(
        Yielder<OutType> subYielder,
        LimitedYieldingAccumulator<OutType, T> limitedAccumulator
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
      if (limitedAccumulator.finished()) {
        return Yielders.done(initValue, subYielder);
      }

      Yielder<OutType> next = subYielder.next(initValue);
      if (limitedAccumulator.finished() && (!limitedAccumulator.yielded() || limitedAccumulator.isInterruptYield())) {
        next = Yielders.done(next.get(), next);
      }
      return new LimitedYielder<>(next, limitedAccumulator);
    }

    @Override
    public boolean isDone()
    {
      return subYielder.isDone() ||
             limitedAccumulator.finished() && (!limitedAccumulator.yielded() || limitedAccumulator.isInterruptYield());
    }

    @Override
    public void close() throws IOException
    {
      subYielder.close();
    }
  }

  private static class LimitedYieldingAccumulator<OutType, T> extends DelegatingYieldingAccumulator<OutType, T>
  {
    private final int limit;
    int count;
    boolean interruptYield;

    public LimitedYieldingAccumulator(YieldingAccumulator<OutType, T> accumulator, int limit)
    {
      super(accumulator);
      this.limit = limit;
    }

    @Override
    public OutType accumulate(OutType accumulated, T in)
    {
      ++count;

      if (finished()) {
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

    private boolean finished()
    {
      return count >= limit;
    }
  }
}
