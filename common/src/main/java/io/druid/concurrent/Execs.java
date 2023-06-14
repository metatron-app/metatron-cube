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

package io.druid.concurrent;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.common.Tagged;
import io.druid.common.guava.DirectExecutorService;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Yielder;
import io.druid.java.util.common.logger.Logger;
import io.druid.utils.Runnables;
import io.druid.utils.StopWatch;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class Execs
{
  private static final Logger log = new Logger(Execs.class);

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
              throw new RejectedExecutionException("Got Interrupted while adding to the Queue", e);
            }
          }
        }
    );
  }

  // same threded executor which differs in that work is started when future.get() is called
  public static class SubmitSingleThreaded extends AbstractExecutorService
  {
    private boolean shutdown;

    @Override
    public void shutdown()
    {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow()
    {
      return ImmutableList.of();
    }

    @Override
    public boolean isShutdown()
    {
      return shutdown;
    }

    @Override
    public boolean isTerminated()
    {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
      return shutdown;
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task)
    {
      return new Future<T>()
      {
        private boolean canceled;
        private boolean done;
        private T result;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
          if (done) {
            return false;
          }
          return canceled = done = true;
        }

        @Override
        public boolean isCancelled()
        {
          return canceled;
        }

        @Override
        public boolean isDone()
        {
          return done;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException
        {
          if (canceled) {
            throw new CancellationException();
          }
          if (done) {
            return result;
          }
          try {
            return result = task.call();
          }
          catch (Exception e) {
            throw new ExecutionException(e);
          }
          finally {
            done = true;
          }
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
          throw new UnsupportedOperationException("get with timeout");
        }
      };
    }

    @Override
    public void execute(Runnable command)
    {
      command.run();
    }
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

  public static <V> List<ListenableFuture<V>> execute(
      ExecutorService executor, Iterable<Callable<V>> works, Semaphore semaphore, int priority
  )
  {
    return new Executor(executor, semaphore, priority, null).execute(works);
  }

  public static class Executor implements Closeable
  {
    private final ExecutorService executor;
    private final Semaphore semaphore;
    private final int priority;
    private final StopWatch watch;

    public Executor(ExecutorService executor, Semaphore semaphore, int priority, Long timeout)
    {
      this.executor = executor;
      this.semaphore = semaphore;
      this.priority = priority;
      this.watch = timeout == null ? null : new StopWatch(timeout);
    }

    @Override
    public void close() throws IOException
    {
      semaphore.destroy();
    }

    public <V> Yielder<V> yield(Yielder<V> yielder)
    {
      return yield(Arrays.asList(yielder)).get(0);
    }

    public <V> List<Yielder<V>> yield(Iterable<Yielder<V>> yielders)
    {
      return collect(execute(Iterables.transform(yielders, y -> semaphore.wrap(() -> y.next(null)))));
    }

    public <V> List<ListenableFuture<V>> execute(Iterable<Callable<V>> works)
    {
      final int parallelism = semaphore.availablePermits();
      Preconditions.checkArgument(parallelism > 0, "Invalid parallelism %d", parallelism);
      log.debug("Executing with parallelism : %d", parallelism);
      // must be materialized first
      final List<WaitingFuture<V>> futures = GuavaUtils.transform(works, WaitingFuture::new);
      final BlockingQueue<WaitingFuture<V>> queue = new LinkedBlockingQueue<WaitingFuture<V>>(futures);
      try {
        for (int i = 0; i < parallelism; i++) {
          executor.submit(PrioritizedRunnable.wrap(priority, () -> {
            for (WaitingFuture<V> work = queue.poll(); work != null; work = queue.poll()) {
              if (!semaphore.acquire(work, watch) || !work.execute()) {
                log.debug("Something wrong.. aborting");  // can be normal process
                break;
              }
            }
          }));
        }
      }
      catch (RejectedExecutionException e) {
        semaphore.destroy();
        cancelQuietly(Futures.allAsList(futures));
        throw e;
      }
      return GuavaUtils.cast(futures);
    }

    public <V> List<V> collect(List<ListenableFuture<V>> futures)
    {
      throw new UnsupportedOperationException("collect");
    }
  }

  public static class ExecutorQueue<T> implements Closeable
  {
    private final Semaphore semaphore;
    private final Runnable closer;
    private final List<Callable<T>> callables = Lists.newArrayList();

    public ExecutorQueue(int parallelism)
    {
      this(new Semaphore(parallelism));
    }

    public ExecutorQueue(Semaphore semaphore)
    {
      this.semaphore = semaphore;
      closer = () -> semaphore.close();
    }

    public void add(Callable<T> callable)
    {
      callables.add(Runnables.after(callable, closer));
    }

    public List<ListenableFuture<T>> execute(ExecutorService executor, int priority)
    {
      return Execs.execute(executor, callables, semaphore, priority);
    }

    @Override
    public void close() throws IOException
    {
      semaphore.destroy();
    }
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

    public <V> PrioritizedCallable<V> wrap(Callable<V> callable)
    {
      return () -> {
        try {
          return callable.call();
        }
        finally {
          semaphore.release();
        }
      };
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

    public boolean acquire(WaitingFuture future, StopWatch watch)
    {
      log.debug("> acquiring %s", name);
      try {
        if (watch != null) {
          watch.acquire(semaphore);
        } else {
          semaphore.acquire();
        }
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
      catch (Throwable e) {
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

  public static ListeningExecutorService newDirectExecutorService()
  {
    return new DirectExecutorService();
  }

  public static boolean isDirectExecutor(ExecutorService executor)
  {
    return executor instanceof DirectExecutorService;
  }

  public static <T> Future<T> excuteDirect(Callable<T> callable)
  {
    FutureTask<T> future = new FutureTask<>(callable);
    future.run();
    return future;
  }

  public static ThreadFactory simpleDaemonFactory(final String name)
  {
    return new ThreadFactory()
    {
      private final ThreadFactory factory = Executors.defaultThreadFactory();

      @Override
      public Thread newThread(Runnable r)
      {
        Thread thread = factory.newThread(r);
        thread.setDaemon(true);
        thread.setName(name);
        return thread;
      }
    };
  }

  public static <T> T waitOn(Future<T> future, long remain)
      throws TimeoutException, ExecutionException, InterruptedException
  {
    if (remain <= 0) {
      throw new TimeoutException();
    }
    return future.get(remain, TimeUnit.MILLISECONDS);
  }
}
