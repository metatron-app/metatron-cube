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
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
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
 * Proves the LOAD TIMING of a lazy segment: metadata (identity/interval/numRows) is served with zero loads, and
 * the underlying index is materialized only on the first QUERY access — not at assignment/registration time.
 */
public class LazySegmentTimingTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testLoadsOnlyOnFirstQueryAccess() throws Exception
  {
    // persist a small segment on disk (stands in for deep storage)
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

    final IndexIO io = TestHelper.getTestIndexIO();
    final Interval interval = new Interval("2020-01-01/2021-01-01");
    final DataSegment descriptor = new DataSegment(
        "ds", interval, "v1", ImmutableMap.of(),
        ImmutableList.of("dim"), ImmutableList.of("cnt", "val"),
        LinearShardSpec.of(0), 9, 0L, 3
    );

    // loader = "fetch + heap-load" stand-in; counts how many times the index is actually materialized
    final AtomicInteger loads = new AtomicInteger();
    final LazySegment segment = new LazySegment(descriptor, () -> {
      loads.incrementAndGet();
      try {
        return io.loadIndex(dir, false, SmooshedFileMapper.loadHeap(dir));
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // metadata must be answered WITHOUT any load (this is what lets a historical announce without downloading)
    Assert.assertEquals(interval, segment.getInterval());
    Assert.assertEquals(3, segment.getNumRows());
    Assert.assertEquals(descriptor.getIdentifier(), segment.getIdentifier());
    Assert.assertEquals("no load may happen before a query touches the segment", 0, loads.get());

    // first QUERY access materializes it
    final StorageAdapter adapter = segment.asStorageAdapter(true);
    Assert.assertNotNull(adapter);
    Assert.assertEquals("index materialized on first query access", 1, loads.get());
  }

  private static InputRow row(long ts, String dim, long val)
  {
    return new MapBasedInputRow(ts, ImmutableList.of("dim"), ImmutableMap.of("dim", dim, "val", val));
  }
}
