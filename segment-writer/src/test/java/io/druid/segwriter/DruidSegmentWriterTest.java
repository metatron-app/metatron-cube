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

package io.druid.segwriter;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link DruidSegmentWriter#persist} was split into {@link DruidSegmentWriter#newIndex}/{@link
 * DruidSegmentWriter#addRow}/{@link DruidSegmentWriter#persistIndex} so a reader can hand a filled index to another
 * thread instead of going quiet for the whole write. These pin the split to the fused path it replaced.
 */
public class DruidSegmentWriterTest
{
  private static final Interval IV = new Interval("2026-01-01T00:00:00Z/2026-01-02T00:00:00Z");
  private static final String VERSION = "2026-01-02T00:00:00.000Z";

  private static SegmentSpec spec()
  {
    return new SegmentSpec(
        "test_ds",
        ImmutableList.of("dim"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        Granularities.NONE,
        false,                     // the trino path runs rollup=false (sort-on-persist requires it)
        "ts"
    );
  }

  private static List<Map<String, Object>> rows(int n)
  {
    final List<Map<String, Object>> rows = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final Map<String, Object> row = new HashMap<>();
      row.put("ts", IV.getStartMillis() + i * 1000L);
      row.put("dim", "d" + (i % 7));
      rows.add(row);
    }
    return rows;
  }

  /** The split path must produce the same segment the fused path did. */
  @Test
  public void splitPathMatchesFusedPath() throws Exception
  {
    final SegmentSpec spec = spec();
    final List<Map<String, Object>> rows = rows(1000);
    final File fusedDir = Files.createTempDir();
    final File splitDir = Files.createTempDir();
    try {
      final DruidSegmentWriter.Persisted fused = DruidSegmentWriter.persist(
          spec, IV, VERSION, NoneShardSpec.instance(), rows, fusedDir, IndexSpec.DEFAULT
      );

      final IncrementalIndex index = DruidSegmentWriter.newIndex(spec, IV);
      for (Map<String, Object> row : rows) {
        DruidSegmentWriter.addRow(index, spec, row);
      }
      final DruidSegmentWriter.Persisted split = DruidSegmentWriter.persistIndex(
          spec, index, IV, VERSION, NoneShardSpec.instance(), splitDir, IndexSpec.DEFAULT
      );

      Assert.assertEquals(1000, fused.template.getNumRows());
      Assert.assertEquals(fused.template.getNumRows(), split.template.getNumRows());
      Assert.assertEquals(fused.template.getIdentifier(), split.template.getIdentifier());
      Assert.assertEquals(fused.template.getDimensions(), split.template.getDimensions());
      Assert.assertEquals(fused.template.getMetrics(), split.template.getMetrics());
      Assert.assertTrue(new File(split.dir, "00000.smoosh").exists());
    }
    finally {
      FileUtils.deleteQuietly(fusedDir);
      FileUtils.deleteQuietly(splitDir);
    }
  }

  /** Shards are handed off one at a time, so a per-shard index must persist standalone at any row count. */
  @Test
  public void persistsASingleRowShard() throws Exception
  {
    final SegmentSpec spec = spec();
    final File dir = Files.createTempDir();
    try {
      final IncrementalIndex index = DruidSegmentWriter.newIndex(spec, IV);
      for (Map<String, Object> row : rows(1)) {
        DruidSegmentWriter.addRow(index, spec, row);
      }
      final DruidSegmentWriter.Persisted p =
          DruidSegmentWriter.persistIndex(spec, index, IV, VERSION, NoneShardSpec.instance(), dir, IndexSpec.DEFAULT);
      Assert.assertEquals(1, p.template.getNumRows());
      Assert.assertTrue(new File(p.dir, "00000.smoosh").exists());
    }
    finally {
      FileUtils.deleteQuietly(dir);
    }
  }

  /** A row whose timestamp column is missing or not epoch-millis must fail loudly, as it did when fused. */
  @Test
  public void addRowRejectsNonNumericTimestamp()
  {
    final SegmentSpec spec = spec();
    final IncrementalIndex index = DruidSegmentWriter.newIndex(spec, IV);
    final Map<String, Object> row = new HashMap<>();
    row.put("ts", "2026-01-01T00:00:00Z");   // a String, not epoch-millis
    row.put("dim", "d0");
    try {
      DruidSegmentWriter.addRow(index, spec, row);
      Assert.fail("expected IllegalArgumentException for a non-Number timestamp");
    }
    catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("must be epoch-millis Number"));
    }
  }
}
