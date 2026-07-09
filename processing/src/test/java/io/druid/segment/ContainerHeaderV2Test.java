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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proves the readable v2 container header (ContainerHeader): the header is text (no binary blobs), boot fetches
 * NOTHING, getColumnCapabilities/schema is answered from the header front-index (still zero fetches), and only a
 * column a query actually reads gets its payload range-fetched. Rows match a mmap load.
 */
public class ContainerHeaderV2Test
{
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testReadableHeaderCapabilitiesThenRangeColumns() throws Exception
  {
    final ObjectMapper mapper = TestHelper.getTestObjectMapper();
    final IndexIO io = TestHelper.getTestIndexIO();
    final File dir = persistSegment();

    final byte[] header = ContainerHeader.write(dir, mapper);

    // header is fully readable text, no binary blobs
    Assert.assertTrue(ContainerHeader.isV2(header));
    final String text = new String(header, StandardCharsets.UTF_8);
    Assert.assertTrue(text.startsWith("smoosh-header,v2\n"));
    Assert.assertTrue("carries a capability front-index", text.contains("\ncaps,"));
    Assert.assertTrue("dim capability row is readable CSV", text.contains("\ndim,STRING,"));

    final AtomicInteger fetches = new AtomicInteger();
    final SmooshedFileMapper.RangeFetcher fetcher = (fileNum, offset, len) -> {
      fetches.incrementAndGet();
      try (RandomAccessFile raf = new RandomAccessFile(new File(dir, String.format("%05d.smoosh", fileNum)), "r")) {
        raf.seek(offset);
        final byte[] b = new byte[len];
        raf.readFully(b);
        return ByteBuffer.wrap(b);
      }
    };

    try (QueryableIndex ranged = ContainerHeader.load(header, fetcher, mapper)) {
      Assert.assertEquals("boot fetches nothing", 0, fetches.get());

      // capabilities of EVERY column are served from the header front-index -> still zero fetches
      final List<String> names = new ArrayList<>();
      names.add(Column.TIME_COLUMN_NAME);
      for (int i = 0; i < ranged.getColumnNames().size(); i++) {
        names.add(ranged.getColumnNames().get(i));
      }
      for (String name : names) {
        Assert.assertNotNull("caps for " + name, ranged.getColumn(name).getCapabilities());
      }
      Assert.assertEquals("capability probes fetch nothing", 0, fetches.get());

      // reading ONE column's data fetches exactly that one column (unqueried columns skipped)
      ranged.getColumn("dim").getDictionary();
      Assert.assertEquals("only the read column is fetched", 1, fetches.get());

      // full row scan matches a mmap load
      try (QueryableIndex mmap = io.loadIndex(dir)) {
        Assert.assertEquals(readAllRows(mmap), readAllRows(ranged));
        Assert.assertFalse(readAllRows(ranged).isEmpty());
        // capabilities equal the mmap-loaded segment's
        for (String name : names) {
          Assert.assertEquals(
              "type of " + name,
              mmap.getColumn(name).getCapabilities().getType(),
              ranged.getColumn(name).getCapabilities().getType()
          );
        }
      }
    }
  }

  @Test
  public void testHeaderDictRangesFetchDictionaryOnly() throws Exception
  {
    final ObjectMapper mapper = TestHelper.getTestObjectMapper();
    final File dir = persistSegment();
    final byte[] header = ContainerHeader.write(dir, mapper);

    // the header carries a dict sub-range for the plain dict-encoded dim, but NOT for the metrics/time
    final java.util.Map<String, long[]> ranges = ContainerHeader.dictRanges(header);
    Assert.assertTrue("dim has a dict range", ranges.containsKey("dim"));
    Assert.assertFalse("metric val is not dict-encoded", ranges.containsKey("val"));
    Assert.assertFalse("metric cnt is not dict-encoded", ranges.containsKey("cnt"));

    final AtomicInteger fetches = new AtomicInteger();
    final SmooshedFileMapper.RangeFetcher fetcher = (fileNum, offset, len) -> {
      fetches.incrementAndGet();
      try (RandomAccessFile raf = new RandomAccessFile(new File(dir, String.format("%05d.smoosh", fileNum)), "r")) {
        raf.seek(offset);
        final byte[] b = new byte[len];
        raf.readFully(b);
        return ByteBuffer.wrap(b);
      }
    };

    // fetch JUST the dict range (one small ranged read) and enumerate -> the column's distinct values
    final long[] r = ranges.get("dim");
    final ByteBuffer dictBuf = fetcher.fetch((int) r[0], r[1], (int) r[2]);
    final io.druid.segment.data.Dictionary<String> dict =
        io.druid.segment.serde.DictionaryEncodedColumnPartSerde.readDictionary(dictBuf);
    Assert.assertEquals("one ranged read for the dict only", 1, fetches.get());

    final List<String> values = new ArrayList<>();
    for (int i = 0; i < dict.size(); i++) {
      values.add(dict.get(i));
    }
    // rows were a,b,c,a -> sorted distinct dictionary [a,b,c]
    Assert.assertEquals(Arrays.asList("a", "b", "c"), values);

    // and the dict bytes we fetched match the same column's dictionary from a full mmap load
    try (QueryableIndex mmap = TestHelper.getTestIndexIO().loadIndex(dir)) {
      final io.druid.segment.data.Dictionary<String> full = mmap.getColumn("dim").getDictionary();
      final List<String> fullValues = new ArrayList<>();
      for (int i = 0; i < full.size(); i++) {
        fullValues.add(full.get(i));
      }
      Assert.assertEquals(fullValues, values);
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
