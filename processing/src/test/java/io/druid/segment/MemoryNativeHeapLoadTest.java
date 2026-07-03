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
import io.druid.common.utils.CompressionUtils;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Proves the segment can be loaded straight from index.zip BYTES into heap — no temp file, no mmap. Unzips in
 * memory (SmooshedFileMapper.loadHeapFromZip) and queries identically to a mmap load.
 */
public class MemoryNativeHeapLoadTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testLoadFromZipBytes() throws Exception
  {
    final IndexIO io = TestHelper.getTestIndexIO();
    final File dir = persistSegment();

    // the segment's index.zip as BYTES (stands in for an S3 GET) — no file is written below this point
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    CompressionUtils.store(dir, bout, 65536);
    final byte[] indexZip = bout.toByteArray();

    // direct: memory mapper + loadIndex with a null dir (version comes from the mapper)
    try (QueryableIndex heap = io.loadIndex(null, false, SmooshedFileMapper.loadHeapFromZip(indexZip));
         QueryableIndex mmap = io.loadIndex(dir)) {
      final List<String> fromBytes = readAllRows(heap);
      Assert.assertEquals(readAllRows(mmap), fromBytes);
      Assert.assertFalse(fromBytes.isEmpty());
    }

    // through the cache's no-temp-file path
    try (HeapSegmentCache cache = new HeapSegmentCache(io, Long.MAX_VALUE)) {
      final QueryableIndex heap = cache.getOrLoad("s1", () -> indexZip);
      Assert.assertTrue(cache.isCached("s1"));
      try (QueryableIndex mmap = io.loadIndex(dir)) {
        Assert.assertEquals(readAllRows(mmap), readAllRows(heap));
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
