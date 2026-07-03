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
import com.google.common.io.Files;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proves the cold-segment lazy-fetch entry: on a cache miss the segment's files are fetched (here, copied to a
 * fresh temp dir), read fully into heap, and the fetched files are DELETED — residency is zero-disk. A second
 * acquire is a cache hit (no re-fetch), and the heap-resident segment queries identically to a mmap load.
 */
public class HeapSegmentCacheFetchTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testFetchOnMissThenZeroDiskResidency() throws Exception
  {
    final IndexIO io = TestHelper.getTestIndexIO();
    final File source = persistSegment();   // stands in for deep storage

    final AtomicInteger fetches = new AtomicInteger();
    final List<File> fetchedDirs = new ArrayList<>();
    final HeapSegmentCache.Fetcher fetcher = () -> {
      fetches.incrementAndGet();
      final File dest = temp.newFolder();   // a fresh "download" dir each fetch
      for (File f : source.listFiles()) {
        Files.copy(f, new File(dest, f.getName()));
      }
      fetchedDirs.add(dest);
      return dest;
    };

    try (HeapSegmentCache cache = new HeapSegmentCache(io, Long.MAX_VALUE)) {
      try (HeapSegmentCache.Handle h = cache.acquire("s1", fetcher)) {
        Assert.assertEquals(1, fetches.get());
        Assert.assertTrue(cache.isCached("s1"));
        Assert.assertTrue(cache.usedBytes() > 0);

        // the fetched files were deleted after heap-load: residency is zero-disk
        Assert.assertFalse("fetched temp dir must be deleted", fetchedDirs.get(0).exists());

        // still queryable purely from heap, identical to a mmap load of the source
        final List<String> fromHeap = readAllRows(h.index());
        try (QueryableIndex mmap = io.loadIndex(source)) {
          Assert.assertEquals(readAllRows(mmap), fromHeap);
          Assert.assertFalse(fromHeap.isEmpty());
        }
      }

      // second acquire is a cache hit — no re-fetch
      cache.acquire("s1", fetcher).close();
      Assert.assertEquals("cache hit must not re-fetch", 1, fetches.get());
    }
  }

  private File persistSegment() throws Exception
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val"))
        .withRollup(false)
        .build();
    final File dir = new File(temp.newFolder(), "seg");
    try (IncrementalIndex incremental = IndexBuilder.create()
        .schema(schema)
        .add(row(1, "a", 10), row(2, "b", 20), row(3, "c", 30))
        .buildIncrementalIndex()) {
      TestHelper.getTestIndexMergerV9().persist(incremental, dir, IndexSpec.DEFAULT);
    }
    return dir;
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
