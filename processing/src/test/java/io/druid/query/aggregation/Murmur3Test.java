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

package io.druid.query.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

public class Murmur3Test
{
  @Test
  public void testByteBuffer()
  {
    byte[] bytes = new byte[0x100000];
    ByteBuffer wrap = ByteBuffer.wrap(bytes);
    ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);

    Random r = new Random();
    for (int i = 0; i < 10; i++) {
      r.nextBytes(bytes);
      direct.position(0);
      direct.put(bytes);
      for (int offset = 0, length = 3; offset + length < bytes.length; length = r.nextInt(80) + 1) {
        final long expected = Murmur3.hash64(bytes, offset, length);
        Assert.assertEquals(expected, Murmur3.hash64(wrap, offset, length));
        Assert.assertEquals(expected, Murmur3.hash64(direct, offset, length));
        offset += length;
      }
    }
    for (int i = 0; i < 10; i++) {
      r.nextBytes(bytes);
      direct.position(0);
      direct.put(bytes);
      for (int offset = 0, length = 3; offset + length < bytes.length; length = r.nextInt(80) + 1) {
        final long[] expected = Murmur3.hash128(bytes, offset, length);
        Assert.assertArrayEquals(expected, Murmur3.hash128(wrap, offset, length));
        Assert.assertArrayEquals(expected, Murmur3.hash128(direct, offset, length));
        offset += length;
      }
    }
  }
}
