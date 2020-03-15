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

import io.druid.java.util.common.guava.Yielder;

import java.io.IOException;
import java.util.concurrent.Executor;

public class ExecuteWhenDoneYielder<T> implements Yielder<T>
{
  private final Yielder<T> baseYielder;
  private final Runnable runnable;
  private final Executor executor;

  public ExecuteWhenDoneYielder(Yielder<T> baseYielder, Runnable runnable, Executor executor)
  {
    this.baseYielder = baseYielder;
    this.runnable = runnable;
    this.executor = executor;
  }

  @Override
  public T get()
  {
    return baseYielder.get();
  }

  @Override
  public Yielder<T> next(T initValue)
  {
    return new ExecuteWhenDoneYielder<>(baseYielder.next(initValue), runnable, executor);
  }

  @Override
  public boolean isDone()
  {
    return baseYielder == null || baseYielder.isDone();
  }

  @Override
  public void close() throws IOException
  {
    try {
      executor.execute(runnable); // should be called either it's done or not (called from limit sequence, etc.)
    }
    finally {
      if (baseYielder != null) {
        baseYielder.close();
      }
    }
  }
}
