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

  private static final long FALLBACK_MSEC = 40;
  private static final long MINIMUM_GET_TIMEOUT_MSEC = 1000;

  private final long getTimeoutMillis;
  private final LoadingCache<K, ImmediateCreationResourceHolder<K, V>> pool;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ResourcePool(
      final ResourceFactory<K, V> factory,
      final ResourcePoolConfig config
  )
  {
    this.getTimeoutMillis = Math.max(MINIMUM_GET_TIMEOUT_MSEC, config.getGetTimeoutMillis());
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

    private final K key;
    private final int maxSize;
    private final ResourceFactory<K, V> factory;
    private final ArrayDeque<ResourceHolder<V>> resourceHolderList;
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

      for (int i = Math.min(INITIAL_CONNECTION, maxSize); i > 0; i--) {
        final ResourceHolder<V> holder = ResourceHolder.of(factory.generate(key));
        if (holder != null) {
          resourceHolderList.add(holder);
        }
      }
      deficit = new AtomicInteger(maxSize - resourceHolderList.size());
    }

    V get(final long timeout) throws InterruptedException
    {
      final long deadline = timeout + System.currentTimeMillis();

      int created = 0;
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
            } else if (created > 0) {
              // suppress excessive (vain) trial of connection
              wait(Math.min(remain, (long) Math.pow(FALLBACK_MSEC, 1 + 0.2 * created)));
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
          created++;
        }
        final long current = System.currentTimeMillis();
        if (holder != null &&
            holder.isGood(current, unusedResourceTimeoutMillis) &&
            factory.isGood(holder.resource, deadline - current)) {
          return holder.resource;
        }
        deficitAvailable();
        if (holder != null) {
          factory.close(holder.resource);
        }
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

    public boolean isGood(long current, long threshold)
    {
      return current - lastAccessedTime < threshold;
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
