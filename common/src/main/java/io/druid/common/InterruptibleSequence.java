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

import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;

import java.util.concurrent.Future;

/**
 */
public class InterruptibleSequence<T> implements Sequence<T>
{
  private final Future<?> future;
  private final Sequence<T> delegate;

  public InterruptibleSequence(Future<?> future, Sequence<T> delegate) {
    this.future = future;
    this.delegate = delegate;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    return delegate.accumulate(initValue, accumulator);   // don't interrupt
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, T> accumulator
  )
  {
    final Yielder<OutType> yielder = delegate.toYielder(initValue, accumulator);
    if (future.isCancelled()) {
      return Yielders.done(null, yielder);
    }
    return yielder;
  }
}
