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

package io.druid.query.aggregation.countmin;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import io.druid.common.guava.BytesRef;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.aggregation.Murmur3;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

public class CountMinSketch implements Comparable<CountMinSketch>, HashCollector
{
  private final int w;
  private final int d;
  private final int[][] multiset;

  public CountMinSketch(int width, int depth)
  {
    this.w = width;
    this.d = depth;
    this.multiset = new int[d][w];
  }

  private CountMinSketch(int width, int depth, int[][] ms)
  {
    this.w = width;
    this.d = depth;
    this.multiset = ms;
  }

  @Override
  public void collect(Object[] values, BytesRef key)
  {
    // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
    // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
    // implement a Bloom filter without any loss in the asymptotic false positive probability'
    // The paper also proves that the same technique (using just 2 pairwise independent hash functions)
    // can be used for Count-Min sketch.

    // Lets split up 64-bit hashcode into two 32-bit hashcodes and employ the technique mentioned
    // in the above paper
    collect(Murmur3.hash64(key.bytes, 0, key.length));
  }

  public void collect(long hash64)
  {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);
    for (int i = 1; i <= d; i++) {
      int hash = hash1 + (i * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (hash < 0) {
        hash = ~hash;
      }
      multiset[i - 1][hash % w] += 1;
    }
  }

  public int getEstimatedCount(byte[] key)
  {
    return getEstimatedCount(Murmur3.hash64(key));
  }

  public int getEstimatedCount(long hash64)
  {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);

    int min = Integer.MAX_VALUE;
    for (int i = 1; i <= d; i++) {
      int hash = hash1 + (i * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (hash < 0) {
        hash = ~hash;
      }
      min = Math.min(min, multiset[i - 1][hash % w]);
    }

    return min;
  }

  public CountMinSketch merge(CountMinSketch that)
  {
    Preconditions.checkArgument(w == that.w);
    Preconditions.checkArgument(d == that.d);

    for (int i = 0; i < d; i++) {
      for (int j = 0; j < w; j++) {
        this.multiset[i][j] += that.multiset[i][j];
      }
    }
    return this;
  }

  public byte[] toBytes()
  {
    BytesOutputStream output = new BytesOutputStream();
    output.writeInt(w);
    output.writeInt(d);
    for (int i = 0; i < d; i++) {
      for (int j = 0; j < w; j++) {
        output.writeUnsignedVarInt(multiset[i][j]);
      }
    }
    return output.toByteArray();
  }

  private static final LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4FastDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();

  @JsonValue
  public byte[] toCompressedBytes()
  {
    final byte[] bytes = toBytes();
    final byte[] compressed = COMPRESSOR.compress(bytes);
    return Bytes.concat(Ints.toByteArray(bytes.length), compressed);
  }

  public static CountMinSketch fromBytes(byte[] serialized)
  {
    final BytesInputStream input = new BytesInputStream(serialized);
    final int width = input.readInt();
    final int depth = input.readInt();
    final int[][] multiset = new int[depth][width];
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < width; j++) {
        multiset[i][j] = input.readUnsignedVarInt();
      }
    }
    return new CountMinSketch(width, depth, multiset);
  }

  public static CountMinSketch fromBytes(ByteBuffer serialized)
  {
    final int width = serialized.getInt();
    final int depth = serialized.getInt();
    final int[][] multiset = new int[depth][width];
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < width; j++) {
        multiset[i][j] = BytesInputStream.readUnsignedVarInt(serialized);
      }
    }
    return new CountMinSketch(width, depth, multiset);
  }

  public static CountMinSketch fromCompressedBytes(byte[] compressed)
  {
    final byte[] dest = new byte[Ints.fromByteArray(compressed)];
    DECOMPRESSOR.decompress(compressed, Ints.BYTES, dest, 0, dest.length);
    return fromBytes(dest);
  }

  @Override
  public int compareTo(CountMinSketch o)
  {
    return 0;
  }
}