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

package io.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HeapSegmentCacheTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private File persistSegment() throws Exception
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val"))
        .withRollup(false)
        .build();
    final File dir = new File(temp.newFolder(), "seg");
    try (IncrementalIndex incremental = IndexBuilder.create()
        .schema(schema)
        .add(row(1, "a", 10), row(2, "b", 20), row(3, "c", 30))   // identical shape -> identical footprint
        .buildIncrementalIndex()) {
      TestHelper.getTestIndexMergerV9().persist(incremental, dir, IndexSpec.DEFAULT);
    }
    return dir;
  }

  @Test
  public void testCapEvictionPinningAndQuery() throws Exception
  {
    final IndexIO io = TestHelper.getTestIndexIO();
    final File d1 = persistSegment();
    final File d2 = persistSegment();
    final File d3 = persistSegment();

    // cap holds exactly two segments (all three have identical footprints)
    final long one;
    try (HeapSegmentCache probe = new HeapSegmentCache(io, Long.MAX_VALUE)) {
      probe.acquire("s1", d1).close();
      one = probe.usedBytes();
    }
    final HeapSegmentCache cache = new HeapSegmentCache(io, 2 * one);

    // load s1 then s2 (both unpinned): cache holds 2
    cache.acquire("s1", d1).close();
    cache.acquire("s2", d2).close();
    Assert.assertEquals(2, cache.size());
    Assert.assertTrue(cache.isCached("s1") && cache.isCached("s2"));

    // s3 over cap -> evict LRU (s1) -> {s2, s3}
    cache.acquire("s3", d3).close();
    Assert.assertEquals(2, cache.size());
    Assert.assertFalse(cache.isCached("s1"));
    Assert.assertTrue(cache.isCached("s2") && cache.isCached("s3"));

    // PIN s2, then load s1: s2 is pinned so it must survive; the unpinned LRU (s3) is evicted instead
    try (HeapSegmentCache.Handle pinned = cache.acquire("s2", d2)) {
      cache.acquire("s1", d1).close();
      Assert.assertTrue("pinned s2 must not be evicted", cache.isCached("s2"));
      Assert.assertTrue(cache.isCached("s1"));
      Assert.assertFalse(cache.isCached("s3"));
      Assert.assertEquals(2, cache.size());

      // query the pinned heap-resident segment: rows must match a fresh mmap load of the same dir
      final List<String> fromHeap = readAllRows(pinned.index());
      try (QueryableIndex mmap = io.loadIndex(d2)) {
        Assert.assertEquals(readAllRows(mmap), fromHeap);
        Assert.assertFalse(fromHeap.isEmpty());
      }
    }
    Assert.assertTrue(cache.usedBytes() <= 2 * one);
    cache.close();
    Assert.assertEquals(0, cache.size());
  }

  private static List<String> readAllRows(QueryableIndex index)
  {
    final List<String> rows = new ArrayList<>();
    for (Rowboat row : new QueryableIndexIndexableAdapter(index).getRows()) {
      rows.add(Arrays.deepToString(row.getDims()) + "|" + Arrays.toString(row.getMetrics()) + "@" + row.getTimestamp());
    }
    return rows;
  }

  private static InputRow row(long ts, String dim, long val)
  {
    return new MapBasedInputRow(ts, ImmutableList.of("dim"), ImmutableMap.of("dim", dim, "val", val));
  }
}
