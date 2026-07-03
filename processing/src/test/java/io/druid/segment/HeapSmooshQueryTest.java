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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Proves a Druid segment can be queried purely from HEAP ByteBuffers (no file/mmap/tmpfs): load the same
 * persisted segment both mmap'd and heap-backed (SmooshedFileMapper.loadHeap) and assert identical row reads.
 */
public class HeapSmooshQueryTest
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testQueryFromHeapMatchesMmap() throws Exception
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val"))
        .withRollup(false)
        .build();

    final File dir = new File(temp.newFolder(), "seg");
    try (IncrementalIndex incremental = IndexBuilder.create()
        .schema(schema)
        .add(row(1, "a", 10), row(2, "b", 20), row(3, "a", 30), row(4, "c", 40), row(5, "a", 50))
        .buildIncrementalIndex()) {
      TestHelper.getTestIndexMergerV9().persist(incremental, dir, IndexSpec.DEFAULT);
    }

    final IndexIO io = TestHelper.getTestIndexIO();

    // sanity: a heap-loaded mapper really serves HEAP buffers (mmap ones are direct; heap ones are not)
    final SmooshedFileMapper probe = SmooshedFileMapper.loadHeap(dir);
    final ByteBuffer any = probe.mapFile(probe.getInternalFilenames().iterator().next());
    Assert.assertFalse("expected a heap (non-direct) ByteBuffer", any.isDirect());
    probe.close();

    try (QueryableIndex mmap = io.loadIndex(dir);
         QueryableIndex heap = io.loadIndex(dir, false, SmooshedFileMapper.loadHeap(dir))) {
      final List<String> fromMmap = readAllRows(mmap);
      final List<String> fromHeap = readAllRows(heap);
      Assert.assertFalse(fromMmap.isEmpty());
      Assert.assertEquals("heap-served rows must match mmap-served rows", fromMmap, fromHeap);
    }
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
