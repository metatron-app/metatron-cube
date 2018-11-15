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

import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 */
public class InterruptibleSequence<T> extends YieldingSequenceBase<T>
{
  private static final Logger LOG = new Logger(InterruptibleSequence.class);

  private final Future<?> future;
  private final Sequence<T> delegate;

  public InterruptibleSequence(Future<?> future, Sequence<T> delegate)
  {
    this.future = future;
    this.delegate = delegate;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    return wrapYielder(delegate.toYielder(initValue, accumulator));
  }

  private <OutType> Yielder<OutType> wrapYielder(final Yielder<OutType> yielder)
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yielder.get();
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        return future.isDone() ? Yielders.<OutType>done(initValue, yielder) : wrapYielder(yielder.next(initValue));
      }

      @Override
      public boolean isDone()
      {
        return yielder.isDone();
      }

      @Override
      public void close() throws IOException
      {
        yielder.close();
      }
    };
  }
}
