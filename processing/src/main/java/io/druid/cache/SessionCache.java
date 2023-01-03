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

package io.druid.cache;

import com.google.common.base.Supplier;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.Cacheable;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.bitmap.RoaringBitmapFactory.LazyImmutableBitmap;
import io.druid.segment.filter.BitmapHolder;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionCache implements Cache
{
  private static final RoaringBitmapFactory FACTORY = new RoaringBitmapFactory(true);

  private final Cache delegate;
  private final Map<String, CacheHolder> cached;

  public SessionCache()
  {
    delegate = Cache.NULL;
    cached = new ConcurrentHashMap<>();
  }

  private SessionCache(Cache delegate, Map<String, CacheHolder> cached)
  {
    this.delegate = delegate == null ? Cache.NULL : delegate;
    this.cached = cached;
  }

  public SessionCache wrap(Cache delegate)
  {
    return delegate == null || delegate == NULL ? this : new SessionCache(delegate, cached);
  }

  public BitmapHolder cache(String namespace, Cacheable filter, BitmapHolder holder)
  {
    return cached.computeIfAbsent(namespace, k -> new CacheHolder()).put(filter, holder);
  }

  public BitmapHolder get(String namespace, Cacheable filter, Supplier<BitmapHolder> populator)
  {
    final CacheHolder cache = cached.computeIfAbsent(namespace, k -> new CacheHolder());
    if (delegate == NULL || Cacheable.isHeavy(filter)) {
      return cache.get(filter, populator);
    }
    BitmapHolder holder = cache.get(filter);
    if (holder != null) {
      return holder;
    }
    final byte[] objKey = filter.getCacheKey();
    if (objKey == null) {
      return cache.put(filter, populator);
    }
    final Cache.NamedKey key = new Cache.NamedKey(namespace.getBytes(), objKey);
    final byte[] bytes = delegate.get(key);
    if (bytes != null) {
      ByteBuffer wrapped = ByteBuffer.wrap(bytes);
      return cache.put(filter, BitmapHolder.of(wrapped.get() != 0, FACTORY.mapImmutableBitmap(wrapped)));
    }
    holder = cache.put(filter, populator);
    if (holder != null) {
      byte exact = holder.exact() ? (byte) 0x01 : 0x00;
      delegate.put(key, StringUtils.concat(new byte[]{exact}, holder.bitmap().toBytes()));
    }
    return holder;
  }

  @Override
  public byte[] get(NamedKey key)
  {
    return delegate.get(key);
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    delegate.put(key, value);
  }

  @Override
  public void close(String namespace)
  {
    if (namespace == null) {
      cached.clear();
    } else {
      delegate.close(namespace);
    }
  }

  @Override
  public CacheStats getStats()
  {
    return delegate.getStats();
  }

  @Override
  public boolean isLocal()
  {
    return delegate.isLocal();
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    delegate.doMonitor(emitter);
  }

  private static class CacheHolder extends ConcurrentHashMap<Object, BitmapHolder>
  {
    private BitmapHolder get(Cacheable filter, Supplier<BitmapHolder> populator)
    {
      BitmapHolder holder = get(filter);
      return holder != null ? holder : put(filter, populator);
    }

    private BitmapHolder put(Cacheable filter, Supplier<BitmapHolder> populator)
    {
      return put(filter, populator.get());
    }

    private BitmapHolder put(Cacheable filter, BitmapHolder holder)
    {
      BitmapHolder prepared = prepare(holder);
      if (prepared != null) {
        super.put(filter, prepared);
      }
      return holder;
    }

    private BitmapHolder prepare(BitmapHolder holder)
    {
      if (holder == null || holder.rhs instanceof LazyImmutableBitmap) {
        return holder;
      }
      // cannot sure clone thing
      boolean exact = holder.lhs;
      ImmutableBitmap bitmap = holder.rhs;
      return BitmapHolder.of(exact, RoaringBitmapFactory.copyToBitmap(bitmap.iterator()));
    }
  }
}
