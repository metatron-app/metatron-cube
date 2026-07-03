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
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proves the header-first / range-read container: a segment loads from a small front block (meta.smoosh +
 * version.bin) plus RANGE fetches of only index.drd/metadata.drd at boot; column chunks are range-fetched only
 * when a column is actually read. Never downloads the whole segment; rows match a mmap load.
 */
public class RangeSmooshLoadTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testHeaderFirstThenRangeColumns() throws Exception
  {
    final IndexIO io = TestHelper.getTestIndexIO();
    final File dir = persistSegment();   // stands in for the chunk objects in deep storage

    // the small front block is fetched cheaply; chunk ranges come via the fetcher (here, reads of the dir files)
    final byte[] metaSmoosh = Files.toByteArray(new File(dir, "meta.smoosh"));
    final byte[] version = Files.toByteArray(new File(dir, "version.bin"));
    final AtomicInteger rangeFetches = new AtomicInteger();
    final SmooshedFileMapper.RangeFetcher fetcher = (fileNum, offset, len) -> {
      rangeFetches.incrementAndGet();
      try (RandomAccessFile raf = new RandomAccessFile(new File(dir, String.format("%05d.smoosh", fileNum)), "r")) {
        raf.seek(offset);
        final byte[] b = new byte[len];
        raf.readFully(b);
        return ByteBuffer.wrap(b);
      }
    };

    final SmooshedFileMapper mapper = SmooshedFileMapper.fromRange(metaSmoosh, version, fetcher);
    try (QueryableIndex ranged = io.loadIndex(null, false, mapper)) {
      // boot fetched only the header entries (index.drd + metadata.drd) — no column payload
      final int bootFetches = rangeFetches.get();
      Assert.assertEquals("boot range-fetches only index.drd + metadata.drd", 2, bootFetches);

      // reading rows range-fetches the columns lazily
      final List<String> rows = readAllRows(ranged);
      Assert.assertTrue("columns range-fetched on read", rangeFetches.get() > bootFetches);

      try (QueryableIndex mmap = io.loadIndex(dir)) {
        Assert.assertEquals(readAllRows(mmap), rows);
        Assert.assertFalse(rows.isEmpty());
      }
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
        .add(row(1, "a", 10), row(2, "b", 20), row(3, "c", 30), row(4, "a", 40))
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
