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
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end shape of the lazy path: a {@link LazySegment} materialized through a {@link HeapSegmentCache}.
 * Metadata is served with zero fetches; the first query fetches-from-"deep-storage" + heap-loads; a second
 * query is a cache hit (no re-fetch). This is exactly what a lazy SegmentLoader.getSegment will return.
 */
public class LazySegmentHeapCacheTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testLazySegmentBackedByHeapCache() throws Exception
  {
    final IndexIO io = TestHelper.getTestIndexIO();
    final File source = persistSegment();

    final AtomicInteger fetches = new AtomicInteger();
    final HeapSegmentCache.Fetcher fetcher = () -> {
      fetches.incrementAndGet();
      final File dest = temp.newFolder();
      for (File f : source.listFiles()) {
        Files.copy(f, new File(dest, f.getName()));
      }
      return dest;
    };

    final Interval interval = new Interval("2020-01-01/2021-01-01");
    final DataSegment descriptor = new DataSegment(
        "ds", interval, "v1", ImmutableMap.of(),
        ImmutableList.of("dim"), ImmutableList.of("cnt", "val"), LinearShardSpec.of(0), 9, 0L, 3
    );

    try (HeapSegmentCache cache = new HeapSegmentCache(io, Long.MAX_VALUE)) {
      final LazySegment segment = new LazySegment(descriptor, () -> {
        try {
          return cache.getOrLoad(descriptor.getIdentifier(), fetcher);
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // metadata: zero fetches
      Assert.assertEquals(3, segment.getNumRows());
      Assert.assertEquals(interval, segment.getInterval());
      Assert.assertEquals(0, fetches.get());

      // first query access fetches + heap-loads
      Assert.assertNotNull(segment.asStorageAdapter(true));
      Assert.assertEquals(1, fetches.get());
      Assert.assertTrue(cache.isCached(descriptor.getIdentifier()));

      // second query access is a cache hit — no re-fetch
      Assert.assertNotNull(segment.asStorageAdapter(true));
      Assert.assertEquals(1, fetches.get());
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

  private static InputRow row(long ts, String dim, long val)
  {
    return new MapBasedInputRow(ts, ImmutableList.of("dim"), ImmutableMap.of("dim", dim, "val", val));
  }
}
