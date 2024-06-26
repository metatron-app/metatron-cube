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

package io.druid.common;

import java.util.Arrays;

/**
 *
 */
public interface Cacheable
{
  int DEFAULT_KEY_LIMIT = 2048;

  default byte[] getCacheKey()
  {
    return getCacheKey(KeyBuilder.get(DEFAULT_KEY_LIMIT)).build();
  }

  KeyBuilder getCacheKey(KeyBuilder builder);

  interface PossiblyHeavy extends Cacheable
  {
    default boolean isHeavy() {return true;}
  }

  static boolean isHeavy(Cacheable cacheable)
  {
    return cacheable instanceof PossiblyHeavy && ((PossiblyHeavy) cacheable).isHeavy();
  }

  static Cacheable withIdentity(Cacheable cacheable)
  {
    return new Identity(cacheable);
  }

  static class Identity implements Cacheable
  {
    private final Cacheable cacheable;
    private final byte[] key;

    private Identity(Cacheable cacheable)
    {
      this.cacheable = cacheable;
      this.key = cacheable.getCacheKey();
    }

    @Override
    public byte[] getCacheKey()
    {
      return key;
    }

    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return cacheable.getCacheKey(builder);
    }

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(key);
    }

    @Override
    public boolean equals(Object other)
    {
      return other instanceof Identity && Arrays.equals(key, ((Identity) other).key);
    }
  }
}
