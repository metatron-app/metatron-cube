package io.druid.java.util.common.concurrent;

/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;
import java.util.function.Function;

public class ListenableFutures
{
  /**
   * Guava 26 removed the two-arg {@code Futures.addCallback(future, callback)} overload; an explicit
   * Executor is now required. These helpers preserve the old call sites: the two-arg form runs the
   * callback on a direct (same-thread) executor, matching Guava's historical default.
   */
  public static <V> void addCallback(ListenableFuture<V> future, FutureCallback<? super V> callback)
  {
    Futures.addCallback(future, callback, MoreExecutors.directExecutor());
  }

  public static <V> void addCallback(
      ListenableFuture<V> future,
      FutureCallback<? super V> callback,
      Executor executor
  )
  {
    Futures.addCallback(future, callback, executor);
  }

  /**
   * Guava 26 removed the two-arg {@code Futures.transform(future, function)} overload; an explicit
   * Executor is now required. The two-arg form runs the transform on a direct (same-thread) executor,
   * matching Guava's historical default.
   */
  public static <I, O> ListenableFuture<O> transform(
      ListenableFuture<I> input,
      com.google.common.base.Function<? super I, ? extends O> function
  )
  {
    return Futures.transform(input, function, MoreExecutors.directExecutor());
  }

  public static <I, O> ListenableFuture<O> transform(
      ListenableFuture<I> input,
      com.google.common.base.Function<? super I, ? extends O> function,
      Executor executor
  )
  {
    return Futures.transform(input, function, executor);
  }

  /**
   * Guava 19 changes the Futures.transform signature so that the async form is different. This is here as a
   * compatability layer until such a time as druid only supports Guava 19 or later, in which case
   * Futures.transformAsync should be used
   *
   * This is NOT copied from guava.
   *
   */
  public static <I, O> ListenableFuture<O> transformAsync(
          final ListenableFuture<I> inFuture,
          final Function<I, ListenableFuture<O>> transform
  )
  {
    final SettableFuture<O> finalFuture = SettableFuture.create();
    Futures.addCallback(inFuture, new FutureCallback<I>()
    {
      @Override
      public void onSuccess(@Nullable I result)
      {
        final ListenableFuture<O> transformFuture = transform.apply(result);
        Futures.addCallback(transformFuture, new FutureCallback<O>()
        {
          @Override
          public void onSuccess(@Nullable O result)
          {
            finalFuture.set(result);
          }

          @Override
          public void onFailure(Throwable t)
          {
            finalFuture.setException(t);
          }
        }, MoreExecutors.directExecutor());
      }

      @Override
      public void onFailure(Throwable t)
      {
        finalFuture.setException(t);
      }
    }, MoreExecutors.directExecutor());
    return finalFuture;
  }
}
