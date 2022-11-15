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

import io.druid.common.guava.ByteArray;
import io.druid.java.util.common.UOE;
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionCache implements Cache
{
  private final Map<ByteArray, byte[]> cached = new ConcurrentHashMap<>();

  @Override
  public byte[] get(NamedKey key)
  {
    return cached.get(ByteArray.wrap(key.toByteArray()));
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    cached.put(ByteArray.wrap(key.toByteArray()), value);
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    throw new UOE("getBulk");
  }

  @Override
  public void close(String namespace)
  {
    cached.clear();
  }

  @Override
  public CacheStats getStats()
  {
    return null;
  }

  @Override
  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
  }
}
