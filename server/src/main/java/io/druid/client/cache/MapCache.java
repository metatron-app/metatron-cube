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

package io.druid.client.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.cache.Cache;
import io.druid.cache.CacheStats;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class MapCache extends Cache.ZipSupport
{
  private static final Logger logger = new Logger(MapCache.class);

  public static Cache create(long sizeInBytes)
  {
    return new MapCache(new ByteCountingLRUMap(sizeInBytes));
  }

  private final Map<ByteBuffer, byte[]> baseMap;

  private final Map<String, byte[]> namespaceId;
  private final AtomicInteger ids = new AtomicInteger();

  private final Object clearLock = new Object();

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);

  MapCache(ByteCountingLRUMap baseMap)
  {
    this.baseMap = baseMap;
    this.namespaceId = Maps.newHashMap();
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
    final ByteBuffer computed = computeKey(getNamespaceId(key.namespace), key.key);
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
    final ByteBuffer computed = computeKey(getNamespaceId(key.namespace), key.key);
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
    byte[] idBytes;
    synchronized (namespaceId) {
      idBytes = getNamespaceId(namespace);
      if (idBytes == null) {
        return;
      }

      namespaceId.remove(namespace);
    }
    synchronized (clearLock) {
      Iterator<ByteBuffer> iter = baseMap.keySet().iterator();
      List<ByteBuffer> toRemove = Lists.newLinkedList();
      while (iter.hasNext()) {
        ByteBuffer next = iter.next();

        if (next.get(0) == idBytes[0]
            && next.get(1) == idBytes[1]
            && next.get(2) == idBytes[2]
            && next.get(3) == idBytes[3]) {
          toRemove.add(next);
        }
      }
      for (ByteBuffer key : toRemove) {
        baseMap.remove(key);
      }
    }
  }

  private byte[] getNamespaceId(final String identifier)
  {
    synchronized (namespaceId) {
      byte[] idBytes = namespaceId.get(identifier);
      if (idBytes != null) {
        return idBytes;
      }

      idBytes = Ints.toByteArray(ids.getAndIncrement());
      namespaceId.put(identifier, idBytes);
      return idBytes;
    }
  }

  private ByteBuffer computeKey(byte[] idBytes, byte[] key)
  {
    final ByteBuffer retVal = ByteBuffer.allocate(key.length + 4).put(idBytes).put(key);
    retVal.rewind();
    return retVal;
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
}
