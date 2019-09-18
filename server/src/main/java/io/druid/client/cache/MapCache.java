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

package io.druid.client.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.cache.Cache;
import io.druid.cache.CacheStats;
import io.druid.common.guava.ByteArray;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 */
public class MapCache extends Cache.ZipSupport implements Function<ByteArray, byte[]>
{
  private static final Logger logger = new Logger(MapCache.class);

  public static Cache create(long sizeInBytes)
  {
    return new MapCache(new ByteCountingLRUMap(sizeInBytes));
  }

  private final Map<ByteArray, byte[]> baseMap;
  private final Map<ByteArray, byte[]> namespaceId;
  private final AtomicInteger ids = new AtomicInteger();

  private final Object clearLock = new Object();

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);

  MapCache(ByteCountingLRUMap baseMap)
  {
    this.baseMap = baseMap;
    this.namespaceId = Maps.newConcurrentMap();
    logger.info("Creating local cache with size " + baseMap.getSizeInBytes());
  }

  @Override
  public CacheStats getStats()
  {
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        baseMap.size(),
        ((ByteCountingLRUMap)baseMap).getNumBytes(),
        ((ByteCountingLRUMap)baseMap).getEvictionCount(),
        0,
        0
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    final ByteArray computed = concat(ensureNamespaceId(key.namespace), key.key);
    final byte[] compressed;
    synchronized (clearLock) {
      compressed = baseMap.get(computed);
    }
    if (compressed == null) {
      missCount.incrementAndGet();
    } else {
      hitCount.incrementAndGet();
    }
    return deserialize(compressed);
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    final ByteArray computed = concat(ensureNamespaceId(key.namespace), key.key);
    final byte[] compressed = serialize(value);
    synchronized (clearLock) {
      baseMap.put(computed, compressed);
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<NamedKey, byte[]> retVal = Maps.newHashMap();
    for (NamedKey key : keys) {
      final byte[] value = get(key);
      if (value != null) {
        retVal.put(key, value);
      }
    }
    return retVal;
  }

  @Override
  public void close(String namespace)
  {
    final byte[] idBytes = removeNamespaceId(namespace);
    if (idBytes == null) {
      return;
    }
    final int id = Ints.fromByteArray(idBytes);
    synchronized (clearLock) {
      List<ByteArray> toRemove = Lists.newLinkedList();
      for (ByteArray key : baseMap.keySet()) {
        if (Ints.fromByteArray(key.array()) == id) {
          toRemove.add(key);
        }
      }
      for (ByteArray key : toRemove) {
        baseMap.remove(key);
      }
    }
  }

  private byte[] removeNamespaceId(final String identifier)
  {
    return namespaceId.remove(new ByteArray(StringUtils.toUtf8(identifier)));
  }

  private byte[] ensureNamespaceId(final byte[] identifier)
  {
    return namespaceId.computeIfAbsent(new ByteArray(identifier), this);
  }

  private ByteArray concat(byte[] idBytes, byte[] key)
  {
    byte[] retVal = new byte[idBytes.length + key.length];
    System.arraycopy(idBytes, 0, retVal, 0, idBytes.length);
    System.arraycopy(key, 0, retVal, idBytes.length, key.length);
    return new ByteArray(retVal);
  }

  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    // No special monitoring
  }

  @Override
  public byte[] apply(ByteArray ByteArray)
  {
    return Ints.toByteArray(ids.getAndIncrement());
  }
}
