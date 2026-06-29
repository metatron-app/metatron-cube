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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.utils.CompressionUtils;
import org.joda.time.Interval;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained smoke test for the shaded jar: builds a segment with the local pusher and
 * reads it back with IndexIO to prove it is a valid, readable Druid segment. No cluster needed.
 *
 *   java -cp druid-segment-writer-<ver>-spark.jar io.druid.segwriter.SegmentWriterSmoke
 */
public class SegmentWriterSmoke
{
  public static void main(String[] args) throws Exception
  {
    final File tmp = Files.createTempDir();
    final File deep = new File(tmp, "deep");
    deep.mkdirs();

    final SegmentSpec spec = new SegmentSpec(
        "smoke",
        Arrays.asList("country", "page"),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("hits", "hits")
        },
        QueryGranularities.NONE,
        true,
        "__time"
    );

    final Interval interval = new Interval("2020-01-01/2020-01-02");
    final long t = interval.getStartMillis();
    final List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(row(t,        "KR", "Seoul", 3));
    rows.add(row(t + 1000, "KR", "Busan", 1));
    rows.add(row(t + 2000, "US", "NYC",   5));
    rows.add(row(t + 3000, "JP", "Tokyo", 2));

    final DataSegment seg = DruidSegmentWriter.write(
        spec, interval, "v1", NoneShardSpec.instance(), rows, DataSegmentPushers.local(deep), tmp
    );

    System.out.println("WROTE segment: " + seg.getIdentifier());
    System.out.println("  loadSpec=" + seg.getLoadSpec());
    System.out.println("  size=" + seg.getSize() + " binaryVersion=" + seg.getBinaryVersion());

    // read it back to prove it is a valid segment
    final File zip = new File((String) seg.getLoadSpec().get("path"));
    final File unpacked = new File(tmp, "unpacked");
    unpacked.mkdirs();
    CompressionUtils.unzip(zip, unpacked);
    final IndexIO indexIO = new IndexIO(Json.mapper());
    try (QueryableIndex index = indexIO.loadIndex(unpacked)) {
      System.out.println("READBACK numRows=" + index.getNumRows()
                         + " columns=" + index.getColumnNames());
      System.out.println("SMOKE OK");
    }
  }

  private static Map<String, Object> row(long ts, String country, String page, long hits)
  {
    return ImmutableMap.<String, Object>of("__time", ts, "country", country, "page", page, "hits", hits);
  }
}
