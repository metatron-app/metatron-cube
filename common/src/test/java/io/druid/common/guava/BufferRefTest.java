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

package io.druid.common.guava;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Random;

public class BufferRefTest
{
  @Test
  public void testBitsetIx()
  {
    Random r = new Random();
    BitSet bs = new BitSet();
    for (int x = 0; x < 1024; x++) {
      for (int i = r.nextInt(32); i >= 0; i--) {
        bs.set(r.nextInt(1024));
      }
      ByteBuffer buffer = ByteBuffer.wrap(bs.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
      BufferRef ref = BufferRef.of(buffer, 0, buffer.remaining());
      for (int i = 0; i < 1024; i++) {
        Assert.assertEquals(i + ":" + bs, bs.get(i), ref.getBool(i));
      }
      bs.clear();
    }
  }

  @Test
  public void testBitsetIxWithOffset()
  {
    long h = 0b11111111_00000000_00110000_00100000;
    long l = 0b00110011_10010001_10011111_10010001;
    long[] x = new long[]{(h << 32) + l};
    BitSet bs = BitSet.valueOf(x);
    ByteBuffer buffer = ByteBuffer.wrap(bs.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
    BufferRef ref = BufferRef.of(buffer, 0, buffer.remaining());
    for (int i = 0; i < bs.length(); i++) {
      Assert.assertEquals(i + ":" + bs, bs.get(i), ref.getBool(i));
    }
    System.out.println(bs);
    System.out.println(toString(ref, bs.size()));

    ref = BufferRef.of(buffer, 1, buffer.remaining() - 1);
    System.out.println(toString(ref, bs.size()));
    for (int i = 0; i < bs.length(); i++) {
      Assert.assertEquals(i + ":" + bs, bs.get(i + 8), ref.getBool(i));
    }
  }

  private String toString(BufferRef ref, int max)
  {
    StringBuilder b = new StringBuilder("{");
    for (int i = 0; i < max; i++) {
      if (ref.getBool(i)) {
        if (b.length() > 1) {
          b.append(", ");
        }
        b.append(i);
      }
    }
    return b.append("}").toString();
  }
}
