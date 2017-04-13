/*
 * Copyright 2011,2012 Metamarkets Group Inc.
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

package io.druid.common.guava;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 */
public class FutureSequence<T> implements Sequence<T>
{
  public static <V> Function<Future<Sequence<V>>, Sequence<V>> toSequence()
  {
    return new Function<Future<Sequence<V>>, Sequence<V>>()
    {
      @Override
      public Sequence<V> apply(Future<Sequence<V>> input)
      {
        return new FutureSequence<V>(input);
      }
    };
  }

  private final Future<Sequence<T>> provider;

  public FutureSequence(Future<Sequence<T>> provider)
  {
    this.provider = provider;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    try {
      return provider.get().accumulate(initValue, accumulator);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    try {
      return provider.get().toYielder(initValue, accumulator);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
