/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import io.druid.java.util.common.guava.CloseQuietly;

import java.io.IOException;
import java.util.List;

/**
 * copied from java-util to fix NPE
 */
public class ConcatSequence<T> implements Sequence<T>
{
  private final List<String> columns;
  private final Sequence<Sequence<T>> baseSequences;

  public ConcatSequence(List<String> columns, Sequence<Sequence<T>> baseSequences)
  {
    this.columns = columns;
    this.baseSequences = baseSequences;
  }

  @Override
  public List<String> columns()
  {
    return columns;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> accumulator)
  {
    return baseSequences.accumulate(
        initValue, new Accumulator<OutType, Sequence<T>>()
        {
          @Override
          public OutType accumulate(OutType accumulated, Sequence<T> in)
          {
            return in.accumulate(accumulated, accumulator);
          }
        }
    );
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    Yielder<Sequence<T>> yielderYielder = Yielders.each(baseSequences);

    try {
      return makeYielder(yielderYielder, initValue, accumulator);
    }
    catch (RuntimeException e) {
      // We caught a RuntimeException instead of returning a really, real, live, real boy, errr, iterator
      // So we better try to close our stuff, 'cause the exception is what is making it out of here.
      CloseQuietly.close(yielderYielder);
      throw e;
    }
  }

  private <OutType> Yielder<OutType> makeYielder(
      Yielder<Sequence<T>> yielderYielder,
      OutType initValue,
      YieldingAccumulator<OutType, T> accumulator
  )
  {
    while (!yielderYielder.isDone()) {
      Yielder<OutType> yielder = yielderYielder.get().toYielder(initValue, accumulator);
      if (accumulator.yielded()) {
        return wrapYielder(yielder, yielderYielder, accumulator);
      }
      initValue = Yielders.getAndClose(yielder);
      yielderYielder = yielderYielder.next(null);
    }

    return Yielders.done(initValue, yielderYielder);
  }

  private <OutType> Yielder<OutType> wrapYielder(
      final Yielder<OutType> yielder,
      final Yielder<Sequence<T>> yielderYielder,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    if (!accumulator.yielded()) {
      return makeYielder(yielderYielder.next(null), Yielders.getAndClose(yielder), accumulator);
    }
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
        accumulator.reset();
        if (yielder.isDone()) {
          return makeYielder(yielderYielder.next(null), Yielders.getAndClose(yielder), accumulator);
        } else {
          return wrapYielder(yielder.next(initValue), yielderYielder, accumulator);
        }
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
        yielderYielder.close();
      }
    };
  }
}