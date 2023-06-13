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

package io.druid.collections;

import com.google.common.base.Supplier;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class StupidPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  private final Queue<T> objects = new ConcurrentLinkedQueue<>();
  private final AtomicInteger counter = new AtomicInteger();

  //note that this is just the max entries in the cache, pool can still create as many buffers as needed.
  private final int objectsCacheMaxCount;

  private int createdObjects;

  public StupidPool(Supplier<T> generator)
  {
    this(generator, Integer.MAX_VALUE);
  }

  public StupidPool(Supplier<T> generator, int objectsCacheMaxCount)
  {
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;
  }

  public ResourceHolder<T> take()
  {
    return take("");
  }

  public ResourceHolder<T> take(String source)
  {
    T obj = objects.poll();
    if (obj != null) {
      counter.decrementAndGet();
    } else {
      obj = generate(source);
      if (++createdObjects > objectsCacheMaxCount) {
        log.warn("creating [%d] for max cache [%d].. leak?", createdObjects, objectsCacheMaxCount);
      }
    }
    return new ObjectResourceHolder(prepare(obj, source), source);
  }

  protected T generate(String source)
  {
    return generator.get();
  }

  protected T prepare(T object, String source)
  {
    return object;
  }

  public void clear()
  {
    objects.clear();
    counter.set(0);
  }

  protected class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final T object;
    private final String source;

    public ObjectResourceHolder(T object, String source)
    {
      this.object = object;
      this.source = source;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      if (closed.get()) {
        throw new ISE("Already Closed! [%s]", source);
      }

      return object;
    }

    @Override
    public void close()
    {
      if (!closed.compareAndSet(false, true)) {
        log.warn(new ISE("Already Closed! [%s]", source), "Already closed [%s]", source);
        return;
      }
      if (counter.get() < objectsCacheMaxCount) {
        if (objects.offer(object)) {
          counter.incrementAndGet();
        }
      } else {
        log.debug("cache num entries is exceeding max limit [%s]", objectsCacheMaxCount);
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      try {
        if (!closed.get()) {
          log.warn("Not closed! Object was[%s:%s]. Allowing gc to prevent leak.", object, source);
        }
      }
      finally {
        super.finalize();
      }
    }
  }
}
