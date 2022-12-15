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
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.Cacheable;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.segment.filter.BitmapHolder;
import org.jboss.netty.util.internal.ConcurrentIdentityHashMap;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionCache implements Cache
{
  private static final RoaringBitmapFactory FACTORY = new RoaringBitmapFactory(true);

  private final Cache delegate;
  private final Map<String, Map<Object, BitmapHolder>> cached;

  public SessionCache()
  {
    delegate = Cache.NULL;
    cached = new ConcurrentHashMap<>();
  }

  private SessionCache(Cache delegate, Map<String, Map<Object, BitmapHolder>> cached)
  {
    this.delegate = delegate == null ? Cache.NULL : delegate;
    this.cached = cached;
  }

  public SessionCache wrap(Cache delegate)
  {
    return delegate == null || delegate == NULL ? this : new SessionCache(delegate, cached);
  }

  public BitmapHolder get(String namespace, Cacheable filter, Supplier<BitmapHolder> populator)
  {
    final Map<Object, BitmapHolder> map = cached.computeIfAbsent(
        namespace, k -> new ConcurrentIdentityHashMap<Object, BitmapHolder>()
    );
    if (delegate == NULL) {
      return map.computeIfAbsent(filter, k -> prepare(populator.get()));
    }
    BitmapHolder holder = map.get(filter);
    if (holder != null) {
      return holder;
    }
    final byte[] objKey = filter.getCacheKey();
    if (objKey == null) {
      holder = populator.get();
      if (holder != null) {
        map.put(filter, prepare(holder));
      }
      return holder;
    }
    final Cache.NamedKey key = new Cache.NamedKey(namespace.getBytes(), objKey);
    final byte[] bytes = delegate.get(key);
    if (bytes != null) {
      ByteBuffer wrapped = ByteBuffer.wrap(bytes);
      return BitmapHolder.of(wrapped.get() != 0, FACTORY.mapImmutableBitmap(wrapped));
    }
    holder = populator.get();
    if (holder != null) {
      map.put(filter, prepare(holder));
      byte exact = holder.exact() ? (byte) 0x01 : 0x00;
      delegate.put(key, StringUtils.concat(new byte[]{exact}, holder.bitmap().toBytes()));
    }
    return holder;
  }

  private BitmapHolder prepare(BitmapHolder holder)
  {
    if (holder == null) {
      return null;
    }
    boolean exact = holder.lhs;
    ImmutableBitmap bitmap = holder.rhs;
    if (bitmap instanceof WrappedImmutableRoaringBitmap) {
      // cannot sure clone thing
      MutableBitmap mutable = FACTORY.makeEmptyMutableBitmap();
      IntIterator iterators = bitmap.iterator();
      while (iterators.hasNext()) {
        mutable.add(iterators.next());
      }
      return BitmapHolder.of(exact, FACTORY.makeImmutableBitmap(mutable));
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
}
