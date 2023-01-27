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

package io.druid.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Pair;
import io.druid.java.util.emitter.service.ServiceEmitter;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 */
public interface Cache
{
  byte[] get(NamedKey key);

  void put(NamedKey key, byte[] value);

  /**
   * Resulting map should not contain any null values (i.e. cache misses should not be included)
   *
   * @param keys
   *
   * @return
   */
  default Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    return GuavaUtils.asMap(Iterables.filter(Iterables.transform(keys, k -> Pair.of(k, get(k))), p -> p.rhs == null));
  }

  void close(String namespace);

  CacheStats getStats();

  boolean isLocal();

  /**
   * Custom metrics not covered by CacheStats may be emitted by this method.
   *
   * @param emitter The service emitter to emit on.
   */
  void doMonitor(ServiceEmitter emitter);

  class NamedKey
  {
    final public byte[] namespace;
    final public byte[] key;

    public NamedKey(byte[] namespace, byte[] key)
    {
      Preconditions.checkArgument(namespace != null, "namespace must not be null");
      Preconditions.checkArgument(key != null, "key must not be null");
      this.namespace = namespace;
      this.key = key;
    }

    public byte[] toByteArray()
    {
      return ByteBuffer.allocate(Integer.BYTES + namespace.length + key.length)
                       .putInt(namespace.length)
                       .put(namespace)
                       .put(key).array();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NamedKey namedKey = (NamedKey) o;

      if (!Arrays.equals(namespace, namedKey.namespace)) {
        return false;
      }
      if (!Arrays.equals(key, namedKey.key)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = Arrays.hashCode(namespace);
      result = 31 * result + Arrays.hashCode(key);
      return result;
    }
  }

  abstract class ZipSupport implements Cache
  {
    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
    private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();
    private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();

    protected final byte[] deserialize(byte[] bytes)
    {
      if (bytes == null) {
        return null;
      }
      final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
      final byte[] out = new byte[decompressedLen];
      LZ4_DECOMPRESSOR.decompress(bytes, Integer.BYTES, out, 0, out.length);
      return out;
    }

    protected final byte[] serialize(byte[] value)
    {
      final byte[] length = Ints.toByteArray(value.length);
      final byte[] out = new byte[Integer.BYTES + LZ4_COMPRESSOR.maxCompressedLength(value.length)];
      System.arraycopy(length, 0, out, 0, Integer.BYTES);
      final int compressedSize = LZ4_COMPRESSOR.compress(value, 0, value.length, out, Integer.BYTES);
      return Arrays.copyOf(out, compressedSize + Integer.BYTES);
    }
  }

  Cache NULL = new Cache()
  {
    @Override
    public byte[] get(NamedKey key) { return null;}

    @Override
    public void put(NamedKey key, byte[] value) {}

    @Override
    public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys) { return null;}

    @Override
    public void close(String namespace) {}

    @Override
    public CacheStats getStats() { return new CacheStats(0, 0, 0, 0, 0, 0, 0);}

    @Override
    public boolean isLocal() { return true;}

    @Override
    public void doMonitor(ServiceEmitter emitter) {}
  };
}
