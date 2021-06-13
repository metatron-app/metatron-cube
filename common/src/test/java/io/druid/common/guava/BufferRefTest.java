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
        Assert.assertEquals(i + ":" + bs.toString(), bs.get(i), ref.get(i));
      }
      bs.clear();
    }
  }
}
