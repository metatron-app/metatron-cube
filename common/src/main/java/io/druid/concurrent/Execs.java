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

package io.druid.concurrent;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.logger.Logger;
import io.druid.common.Tagged;
import io.druid.common.guava.GuavaUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 */
public class Execs
{
  private static Logger log = new Logger(Execs.class);

  public static ExecutorService singleThreaded(@NotNull String nameFormat)
  {
    return singleThreaded(nameFormat, null);
  }

  public static ExecutorService singleThreaded(@NotNull String nameFormat, @Nullable Integer priority)
  {
    return Executors.newSingleThreadExecutor(makeThreadFactory(nameFormat, priority));
  }

  public static ExecutorService multiThreaded(int threads, @NotNull String nameFormat)
  {
    return multiThreaded(threads, nameFormat, null);
  }

  public static ExecutorService multiThreaded(int threads, @NotNull String nameFormat, @Nullable Integer priority)
  {
    return Executors.newFixedThreadPool(threads, makeThreadFactory(nameFormat, priority));
  }

  public static ScheduledExecutorService scheduledSingleThreaded(@NotNull String nameFormat)
  {
    return scheduledSingleThreaded(nameFormat, null);
  }

  public static ScheduledExecutorService scheduledSingleThreaded(@NotNull String nameFormat, @Nullable Integer priority)
  {
    return Executors.newSingleThreadScheduledExecutor(makeThreadFactory(nameFormat, priority));
  }

  public static ThreadFactory makeThreadFactory(@NotNull String nameFormat)
  {
    return makeThreadFactory(nameFormat, null);
  }

  public static ThreadFactory makeThreadFactory(@NotNull String nameFormat, @Nullable Integer priority)
  {
    final ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(nameFormat);
    if (priority != null) {
      builder.setPriority(priority);
    }
    return builder.build();
  }

  /**
   * @param nameFormat nameformat for threadFactory
   * @param capacity   maximum capacity after which the executorService will block on accepting new tasks
   *
   * @return ExecutorService which blocks accepting new tasks when the capacity reached
   */
  public static ExecutorService newBlockingSingleThreaded(final String nameFormat, final int capacity)
  {
    return newBlockingSingleThreaded(nameFormat, capacity, null);
  }

  public static ExecutorService newBlockingSingleThreaded(
      final String nameFormat,
      final int capacity,
      final Integer priority
  )
  {
    final BlockingQueue<Runnable> queue;
    if (capacity > 0) {
      queue = new ArrayBlockingQueue<>(capacity);
    } else {
      queue = new SynchronousQueue<>();
    }
    return new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, queue, makeThreadFactory(nameFormat, priority),
        new RejectedExecutionHandler()
        {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
          {
            try {
              executor.getQueue().put(r);
            }
            catch (InterruptedException e) {
              throw new RejectedExecutionException("Got Interrupted while adding to the Queue");
            }
          }
        }
    );
  }

  public static <T> Function<Future<T>, T> getUnchecked()
  {
    return new Function<Future<T>, T>()
    {
      @Override
      public T apply(Future<T> input)
      {
        return Futures.getUnchecked(input);
      }
    };
  }

  public static class Semaphore implements Closeable
  {
    private final java.util.concurrent.Semaphore semaphore;
    private final String name = Integer.toHexString(System.identityHashCode(this));

    private volatile boolean destroyed;

    public Semaphore(int parallelism)
    {
      log.debug("init parallelism = %d", parallelism);
      this.semaphore = new java.util.concurrent.Semaphore(parallelism);
    }

    public boolean acquire(WaitingFuture future)
    {
      log.debug("> acquiring %s", name);
      try {
        semaphore.acquire();
      }
      catch (Exception e) {
        return future.setException(e);
      }
      log.debug("< acquired %s", name);
      return !future.isCancelled();
    }

    @Override
    public void close()
    {
      log.debug("> close %s", name);
      semaphore.release();
    }

    public void destroy()
    {
      log.debug("> destroy %s", name);
      destroyed = true;
      semaphore.release(semaphore.getQueueLength());
    }

    public boolean isDestroyed()
    {
      return destroyed;
    }

    public int availablePermits()
    {
      return semaphore.availablePermits();
    }
  }

  public static <T> Iterable<Future<T>> execute(
      final ExecutorService executor,
      final Iterable<Callable<T>> works
  )
  {
    return Iterables.transform(
        works, new Function<Callable<T>, Future<T>>()
        {
          @Override
          public Future<T> apply(Callable<T> callable)
          {
            return executor.submit(callable);
          }
        }
    );
  }

  public static <V> List<ListenableFuture<V>> execute(
      final ExecutorService executor,
      final Iterable<Callable<V>> works,
      final Semaphore semaphore,
      final int priority
  )
  {
    final int parallelism = semaphore.availablePermits();
    log.debug("Executing with parallelism : %d", parallelism);
    // must be materialized first
    final List<WaitingFuture<V>> futures = Lists.newArrayList(Iterables.transform(works, WaitingFuture.<V>toWaiter()));
    final Queue<WaitingFuture<V>> queue = new LinkedBlockingQueue<WaitingFuture<V>>(futures);
    for (int i = 0; i < parallelism; i++) {
      executor.submit(
          new PrioritizedRunnable()
          {
            @Override
            public int getPriority()
            {
              return priority;
            }

            @Override
            public void run()
            {
              for (WaitingFuture<V> work = queue.poll(); work != null; work = queue.poll()) {
                if (!semaphore.acquire(work) || !work.execute()) {
                  log.debug("Something wrong.. aborting");  // can be normal process
                  break;
                }
              }
            }
          }
      );
    }
    return GuavaUtils.cast(futures);
  }

  private static class WaitingFuture<V> extends AbstractFuture<V>
  {
    private final Callable<V> callable;

    private WaitingFuture(Callable<V> callable) {this.callable = callable;}

    public boolean execute()
    {
      log.debug("--- executing %s", callable);
      try {
        return set(callable.call());
      }
      catch (Exception e) {
        return setException(e);
      }
    }

    @Override
    public boolean setException(Throwable throwable)
    {
      super.setException(throwable);
      Throwables.propagate(throwable);
      return false;
    }

    private static <V> Function<Callable<V>, WaitingFuture<V>> toWaiter()
    {
      return new Function<Callable<V>, WaitingFuture<V>>()
      {
        @Override
        public WaitingFuture<V> apply(Callable<V> input)
        {
          return new WaitingFuture<V>(input);
        }
      };
    }
  }

  public static <V> TaggedFuture<V> tag(ListenableFuture<V> future, String tag)
  {
    return new TaggedFuture<V>(future, tag);
  }

  public static class TaggedFuture<V> extends ForwardingListenableFuture<V>
      implements ListenableFuture<V>, Tagged, Closeable
  {
    private final ListenableFuture<V> delegate;
    private final String tag;

    private TaggedFuture(ListenableFuture<V> delegate, String tag)
    {
      this.delegate = delegate;
      this.tag = tag;
    }

    @Override
    protected ListenableFuture<V> delegate()
    {
      return delegate;
    }

    @Override
    public String getTag()
    {
      return tag;
    }

    @Override
    public void close() throws IOException
    {
      if (delegate instanceof Closeable) {
        ((Closeable) delegate).close();
      }
    }

    @Override
    public String toString()
    {
      return tag + ":" + super.toString();
    }
  }

  public static class SettableFuture<V> extends AbstractFuture<V> implements Closeable
  {
    @Override
    public boolean set(@Nullable V value)
    {
      return super.set(value);
    }

    @Override
    public boolean setException(Throwable throwable)
    {
      return super.setException(throwable);
    }

    @Override
    public void close() throws IOException
    {
      set(null);
    }
  }

  public static boolean cancelQuietly(Future<?> future)
  {
    try {
      return future.isDone() || future.cancel(true);
    }
    catch (Exception e) {
      // ignore
    }
    return future.isDone();
  }
}
