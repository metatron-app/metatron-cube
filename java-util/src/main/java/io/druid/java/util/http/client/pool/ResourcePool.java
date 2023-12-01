/*
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

package io.druid.java.util.http.client.pool;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import io.druid.java.util.common.logger.Logger;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.jboss.netty.handler.timeout.TimeoutException;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class ResourcePool<K, V> implements Closeable
{
  private static final Logger LOG = new Logger(ResourcePool.class);

  private static final int FALLBACK_MSEC = 40;
  private static final int MINIMUM_GET_TIMEOUT_MSEC = 1000;
  private static final int MAXIMUM_GET_TIMEOUT_MSEC = 30000;

  private final long getTimeoutMillis;
  private final LoadingCache<K, ImmediateCreationResourceHolder<K, V>> pool;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ResourcePool(
      final ResourceFactory<K, V> factory,
      final ResourcePoolConfig config
  )
  {
    getTimeoutMillis = Math.min(MAXIMUM_GET_TIMEOUT_MSEC, Math.max(MINIMUM_GET_TIMEOUT_MSEC, config.getGetTimeoutMillis()));
    final ResourceFactory<K, V> wrapped = new Wrap<>(factory);
    this.pool = CacheBuilder.newBuilder().build(
        new CacheLoader<K, ImmediateCreationResourceHolder<K, V>>()
        {
          @Override
          public ImmediateCreationResourceHolder<K, V> load(K input)
          {
            return new ImmediateCreationResourceHolder<K, V>(
                input,
                config.getMaxPerKey(),
                config.getUnusedConnectionTimeoutMillis(),
                wrapped
            );
          }
        }
    );
  }

  public ResourceContainer<V> take(final K key) throws InterruptedException
  {
    if (closed.get()) {
      LOG.error(String.format("take(%s) called even though I'm closed.", key));
      return null;
    }

    final ImmediateCreationResourceHolder<K, V> holder;
    try {
      holder = pool.get(key);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
    final V value = holder.get(getTimeoutMillis);

    return new ResourceContainer<V>()
    {
      private final AtomicBoolean returned = new AtomicBoolean(false);

      @Override
      public V get()
      {
        Preconditions.checkState(!returned.get(), "Resource for key[%s] has been returned, cannot get().", key);
        return value;
      }

      @Override
      public void returnResource()
      {
        if (returned.getAndSet(true)) {
          LOG.warn("Resource at key[%s] was returned multiple times?", key);
        } else {
          holder.giveBack(value);
        }
      }

      @Override
      protected void finalize() throws Throwable
      {
        if (!returned.get()) {
          LOG.warn(
              "Resource[%s] at key[%s] was not returned before Container was finalized, potential resource leak.",
              value,
              key
          );
          returnResource();
        }
        super.finalize();
      }
    };
  }

  @Override
  public void close()
  {
    closed.set(true);
    final Map<K, ImmediateCreationResourceHolder<K, V>> mapView = pool.asMap();
    for (K k : ImmutableSet.copyOf(mapView.keySet())) {
      mapView.remove(k).close();
    }
  }

  private static class ImmediateCreationResourceHolder<K, V>
  {
    private static final int INITIAL_CONNECTION = 10;
    private static final int INITIAL_CONNECTION_TIMEOUT = 2000;

    private final K key;
    private final int maxSize;
    private final ResourceFactory<K, V> factory;
    private final ArrayDeque<ResourceHolder<V>> resourceHolderList;
    private final FailQueue failQueue;
    private final AtomicInteger deficit;
    private final long unusedResourceTimeoutMillis;

    private boolean closed = false;

    private ImmediateCreationResourceHolder(
        K key,
        int maxSize,
        long unusedResourceTimeoutMillis,
        ResourceFactory<K, V> factory
    )
    {
      this.key = key;
      this.maxSize = maxSize;
      this.factory = factory;
      this.unusedResourceTimeoutMillis = unusedResourceTimeoutMillis;
      this.resourceHolderList = new ArrayDeque<>();
      this.failQueue = new FailQueue();

      long remain = INITIAL_CONNECTION_TIMEOUT;
      for (int i = Math.min(INITIAL_CONNECTION, maxSize); i >= 0 && remain > 0; i--) {
        final long current = System.currentTimeMillis();
        final V channel = factory.generate(key);
        final ResourceHolder<V> holder = ResourceHolder.of(channel);
        if (holder == null) {
          break;
        }
        resourceHolderList.add(holder);
        if (!factory.isGood(channel, remain)) {
          break;
        }
        remain -= System.currentTimeMillis() - current;
      }
      deficit = new AtomicInteger(maxSize - resourceHolderList.size());
    }

    V get(final long timeout) throws InterruptedException
    {
      final long deadline = timeout + System.currentTimeMillis();

      while (true) {
        final long remain = deadline - System.currentTimeMillis();
        if (remain <= 0) {
          throw new TimeoutException(String.format("Timeout getting connection for '%s' (%d msec)", key, timeout));
        }
        ResourceHolder<V> holder = null;
        synchronized (this) {
          if (!closed) {
            if (resourceHolderList.isEmpty() && deficit.get() == 0) {
              wait(remain);
            } else {
              int failed = failQueue.dequeue(Math.max(timeout >> 2, MINIMUM_GET_TIMEOUT_MSEC));
              if (failed > 0) {
                // suppress excessive (vain) trial of connection
                long backoff = Math.min(remain, (long) Math.pow(FALLBACK_MSEC, 1 + 0.15 * failed));
                LOG.info("Retrying [%s] with backoff time %,d msec", Thread.currentThread().getName(), backoff);
                wait(backoff);
              }
            }
          }
          if (closed) {
            LOG.info("get() called even though I'm closed. key[%s]", key);
            return null;
          }
          if (!resourceHolderList.isEmpty()) {
            holder = resourceHolderList.removeFirst();
          } else if (deficit.get() > 0) {
            deficit.decrementAndGet();
          } else {
            continue;
          }
        }
        if (holder == null) {
          holder = ResourceHolder.of(factory.generate(key));
        }
        final long current = System.currentTimeMillis();
        if (holder != null && !holder.isTimedOut(current, unusedResourceTimeoutMillis)) {
          if (factory.isGood(holder.resource, deadline - current)) {
            return holder.resource;
          }
        }
        deficitAvailable();
        if (holder != null) {
          factory.close(holder.resource);
        }
        failQueue.enqueue();
      }
    }

    void giveBack(V object)
    {
      Preconditions.checkNotNull(object, "object");

      if (!factory.isValid(object)) {
        LOG.debug("Destroying invalid object[%s] at key[%s]", object, key);
        deficitAvailable();
        factory.close(object);
        return;
      }
      if (!enqueue(object)) {
        factory.close(object);
      }
    }

    private synchronized boolean enqueue(V object)
    {
      if (closed) {
        LOG.debug("giveBack called after being closed. key[%s]", key);
        return false;
      }
      if (resourceHolderList.size() >= maxSize) {
        if (resourceHolderList.stream().anyMatch(a -> object.equals(a.resource))) {
          LOG.warn(
              new Exception("Exception for stacktrace"),
              "Returning object[%s] at key[%s] that has already been returned!? Skipping",
              object,
              key
          );
        } else {
          LOG.warn(
              new Exception("Exception for stacktrace"),
              "Returning object[%s] at key[%s] even though we already have all that we can hold[%s]!? Skipping",
              object,
              key,
              resourceHolderList
          );
        }
        return false;
      }
      resourceHolderList.addLast(ResourceHolder.of(object));
      notifyAll();
      return true;
    }

    private synchronized void deficitAvailable()
    {
      deficit.incrementAndGet();
      notifyAll();
    }

    private synchronized void close()
    {
      closed = true;
      resourceHolderList.forEach(v -> factory.close(v.resource));
      resourceHolderList.clear();
      notifyAll();
    }
  }

  private static class FailQueue
  {
    private final LongList queue = new LongArrayList();
    private long nextFlush = System.currentTimeMillis() + MAXIMUM_GET_TIMEOUT_MSEC;

    synchronized void enqueue()
    {
      queue.add(System.currentTimeMillis());
    }

    synchronized int dequeue(long timeout)
    {
      long timestamp = System.currentTimeMillis();
      int size = queue.size();
      if (size == 0) {
        return 0;
      }
      if (timestamp > nextFlush) {
        nextFlush = timestamp + MAXIMUM_GET_TIMEOUT_MSEC;
        int i = 0;
        for (; i < size && timestamp - queue.getLong(i) >= MAXIMUM_GET_TIMEOUT_MSEC; i++) {
        }
        queue.removeElements(0, i);
        size = queue.size();
      }
      int i = size - 1;
      for (; i >= 0 && timestamp - queue.getLong(i) < timeout; i--) {
      }
      return size - 1 - i;
    }
  }

  private static class ResourceHolder<V>
  {
    public static <V> ResourceHolder<V> of(V resource)
    {
      return resource == null ? null : new ResourceHolder<>(resource);
    }

    private final long lastAccessedTime = System.currentTimeMillis();
    private final V resource;

    private ResourceHolder(V resource)
    {
      this.resource = resource;
    }

    private boolean isTimedOut(long current, long threshold)
    {
      return threshold > 0 && current - lastAccessedTime >= threshold;
    }
  }

  private static class Wrap<K, V> implements ResourceFactory<K, V>
  {
    private final ResourceFactory<K, V> delegate;

    private Wrap(ResourceFactory<K, V> delegate) {this.delegate = delegate;}

    @Override
    public V generate(K key)
    {
      try {
        return delegate.generate(key);
      }
      catch (Exception e) {
        return null;
      }
    }

    @Override
    public boolean isGood(V resource, long timeout)
    {
      try {
        return resource != null && delegate.isGood(resource, timeout);
      }
      catch (Exception e) {
        return false;
      }
    }

    @Override
    public boolean isValid(V resource)
    {
      try {
        return resource != null && delegate.isValid(resource);
      }
      catch (Exception e) {
        return false;
      }
    }

    @Override
    public void close(V resource)
    {
      if (resource != null) {
        delegate.close(resource);
      }
    }
  }
}
