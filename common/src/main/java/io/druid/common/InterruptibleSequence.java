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

package io.druid.common;

import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.YieldingSequenceBase;
import io.druid.java.util.common.logger.Logger;

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
        return yielder == null || yielder.isDone();
      }

      @Override
      public void close() throws IOException
      {
        if (yielder != null) {
          yielder.close();
        }
      }
    };
  }
}
