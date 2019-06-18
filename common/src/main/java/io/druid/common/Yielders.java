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

import com.google.common.io.Closeables;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class Yielders
{
  // walk around of NPE from next()
  public static <T> Yielder<T> done(final T finalVal, final Closeable closeable)
  {
    return new Yielder<T>()
    {
      @Override
      public T get()
      {
        return finalVal;
      }

      @Override
      public Yielder<T> next(T initValue)
      {
        return this;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public void close() throws IOException
      {
        Closeables.close(closeable, false);
      }
    };
  }

  public static <T> Yielder<T> each(final Sequence<T> sequence)
  {
    return sequence.toYielder(null, new Yielding<T>());
  }

  public static class Yielding<T> extends YieldingAccumulator<T, T>
  {
    @Override
    public T accumulate(T accumulated, T in)
    {
      yield();
      return in;
    }
  }
}
