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

package io.druid.segment.loading;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;

public class RangeDiskCacheTest
{
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private File root;

  @Before
  public void setUp() throws Exception
  {
    root = new File(tmp.getRoot(), "range-disk");
  }

  private static ByteBuffer bytes(int len, int seed)
  {
    final byte[] b = new byte[len];
    for (int i = 0; i < len; i++) {
      b[i] = (byte) (seed + i);
    }
    return ByteBuffer.wrap(b);
  }

  private static void assertBytes(ByteBuffer buf, int len, int seed)
  {
    Assert.assertNotNull(buf);
    Assert.assertEquals(len, buf.remaining());
    for (int i = 0; i < len; i++) {
      Assert.assertEquals("byte " + i, (byte) (seed + i), buf.get(buf.position() + i));
    }
  }

  @Test
  public void missThenWriteThroughThenHit()
  {
    final RangeDiskCache cache = new RangeDiskCache(root, 1L << 30);
    final String seg = "ds/2026-01-01T00:00:00.000Z_2026-01-02T00:00:00.000Z/v1/0";

    Assert.assertNull("cold miss", cache.get(seg, 0, 4096, 100));

    cache.put(seg, 0, 4096, bytes(100, 7));
    assertBytes(cache.get(seg, 0, 4096, 100), 100, 7);   // warm hit returns the same bytes
  }

  @Test
  public void offsetsAreServedIndependently()
  {
    final RangeDiskCache cache = new RangeDiskCache(root, 1L << 30);
    final String seg = "ds/iv/v/0";
    cache.put(seg, 0, 0, bytes(50, 1));
    cache.put(seg, 0, 1_000_000, bytes(80, 200));   // a far offset -> sparse hole in between

    assertBytes(cache.get(seg, 0, 0, 50), 50, 1);
    assertBytes(cache.get(seg, 0, 1_000_000, 80), 80, 200);
    Assert.assertNull("unwritten offset misses", cache.get(seg, 0, 500, 10));
  }

  @Test
  public void differentLengthAtSameOffsetIsAMiss()
  {
    final RangeDiskCache cache = new RangeDiskCache(root, 1L << 30);
    final String seg = "ds/iv/v/0";
    cache.put(seg, 0, 0, bytes(100, 3));
    assertBytes(cache.get(seg, 0, 0, 100), 100, 3);
    Assert.assertNull("length mismatch -> miss", cache.get(seg, 0, 0, 64));
  }

  @Test
  public void putIsIdempotentAndAccounted()
  {
    final RangeDiskCache cache = new RangeDiskCache(root, 1L << 30);
    final String seg = "ds/iv/v/0";
    cache.put(seg, 0, 0, bytes(100, 5));
    final long after1 = cache.residentBytes();
    cache.put(seg, 0, 0, bytes(100, 5));   // same range again
    Assert.assertEquals("second put of the same range must not double-count", after1, cache.residentBytes());
    Assert.assertEquals(100, after1);
  }

  @Test
  public void evictsColdestWholeSegmentUnderBudget()
  {
    // budget below two segments' worth so the second put evicts the first (LRU)
    final RangeDiskCache cache = new RangeDiskCache(root, 150);
    final String a = "ds/iv/v/0";
    final String b = "ds/iv/v/1";

    cache.put(a, 0, 0, bytes(100, 1));
    assertBytes(cache.get(a, 0, 0, 100), 100, 1);

    cache.put(b, 0, 0, bytes(100, 2));   // resident would be 200 > 150 -> evict coldest (a)

    Assert.assertNull("coldest segment evicted", cache.get(a, 0, 0, 100));
    assertBytes(cache.get(b, 0, 0, 100), 100, 2);   // the just-written segment survives
    Assert.assertTrue("resident under budget", cache.residentBytes() <= 150);
  }

  @Test
  public void separateFileNumsDoNotCollide()
  {
    final RangeDiskCache cache = new RangeDiskCache(root, 1L << 30);
    final String seg = "ds/iv/v/0";
    cache.put(seg, 0, 0, bytes(40, 10));
    cache.put(seg, 1, 0, bytes(40, 99));   // same offset, different smoosh chunk file
    assertBytes(cache.get(seg, 0, 0, 40), 40, 10);
    assertBytes(cache.get(seg, 1, 0, 40), 40, 99);
  }
}
