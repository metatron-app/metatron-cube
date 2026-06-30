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
        spec, interval, "v1", NoneShardSpec.instance(), rows, DataSegmentPushers.local(deep), tmp,
        io.druid.segment.IndexSpec.DEFAULT
    );

    System.out.println("WROTE segment: " + seg.getIdentifier());
    System.out.println("  loadSpec=" + seg.getLoadSpec());
    System.out.println("  size=" + seg.getSize() + " binaryVersion=" + seg.getBinaryVersion() + " numRows=" + seg.getNumRows());

    // read it back to prove it is a valid segment
    final File zip = new File((String) seg.getLoadSpec().get("path"));
    final File unpacked = new File(tmp, "unpacked");
    unpacked.mkdirs();
    CompressionUtils.unzip(zip, unpacked);
    final IndexIO indexIO = new IndexIO(Json.indexMapper());
    try (QueryableIndex index = indexIO.loadIndex(unpacked)) {
      System.out.println("READBACK numRows=" + index.getNumRows()
                         + " columns=" + index.getColumnNames());
      System.out.println("SMOKE OK");
    }

    // --- lucene secondary-index path: build via SegmentIngestor with a raw secondaryIndexing spec.
    //     secondaryIndexing attaches to METRIC columns, so "page" is ingested as a string "relay"
    //     metric (a passthrough) and the lucene text index is built on it during persist. ---
    final String ispecJson =
        "{\"dataSource\":\"smoke_lucene\",\"timestampColumn\":\"__time\","
        + "\"dimensions\":[\"country\"],"
        + "\"metrics\":[{\"type\":\"count\",\"name\":\"count\"},"
        + "{\"type\":\"relay\",\"name\":\"page\",\"columnName\":\"page\",\"typeName\":\"string\"}],"
        + "\"segmentGranularity\":\"DAY\",\"queryGranularity\":\"NONE\",\"rollup\":false,\"numShards\":1,"
        + "\"bucket\":\"x\",\"baseKey\":\"x\","
        + "\"secondaryIndexing\":{\"page\":{\"type\":\"lucene10\",\"strategies\":[{\"type\":\"text\",\"fieldName\":\"page\"}]}}}";
    final SegmentIngestSpec ispec = Json.mapper().readValue(ispecJson, SegmentIngestSpec.class);
    final File ldeep = new File(tmp, "ldeep");
    ldeep.mkdirs();
    final DataSegment lseg = SegmentIngestor.buildSegment(
        ispec, interval, "v1", 0, 1, rows.iterator(), tmp, DataSegmentPushers.local(ldeep)
    );
    System.out.println("LUCENE segment: " + lseg.getIdentifier()
                       + " size=" + lseg.getSize() + " numRows=" + lseg.getNumRows());
    final File lzip = new File((String) lseg.getLoadSpec().get("path"));
    final File lun = new File(tmp, "lunpacked");
    lun.mkdirs();
    CompressionUtils.unzip(lzip, lun);
    try (QueryableIndex li = indexIO.loadIndex(lun)) {
      System.out.println("LUCENE READBACK numRows=" + li.getNumRows() + " columns=" + li.getColumnNames());
      // assert the lucene index was actually built on the "page" metric column
      final io.druid.segment.column.Column col = li.getColumn("page");
      final boolean hasLucene = col != null
          && col.getExternalIndexKeys().contains(io.druid.segment.column.LuceneIndex.class);
      System.out.println("LUCENE index present on [page]: " + hasLucene);
      if (!hasLucene) {
        throw new IllegalStateException("lucene index was NOT built on [page]");
      }
      System.out.println("LUCENE SMOKE OK");
    }

    // --- index-only path: same lucene index but the base value column is NOT stored (indexOnly:true).
    //     Search works; SELECT page returns null. Exercises numRows-from-maxDoc (no base column). ---
    final String ioJson =
        "{\"dataSource\":\"smoke_io\",\"timestampColumn\":\"__time\","
        + "\"dimensions\":[\"country\"],"
        + "\"metrics\":[{\"type\":\"count\",\"name\":\"count\"},"
        + "{\"type\":\"relay\",\"name\":\"page\",\"columnName\":\"page\",\"typeName\":\"string\"}],"
        + "\"segmentGranularity\":\"DAY\",\"queryGranularity\":\"NONE\",\"rollup\":false,\"numShards\":1,"
        + "\"bucket\":\"x\",\"baseKey\":\"x\","
        + "\"secondaryIndexing\":{\"page\":{\"type\":\"lucene10\",\"indexOnly\":true,"
        + "\"strategies\":[{\"type\":\"text\",\"fieldName\":\"page\"}]}}}";
    final SegmentIngestSpec iospec = Json.mapper().readValue(ioJson, SegmentIngestSpec.class);
    final File iodeep = new File(tmp, "iodeep");
    iodeep.mkdirs();
    final DataSegment ioseg = SegmentIngestor.buildSegment(
        iospec, interval, "v1", 0, 1, rows.iterator(), tmp, DataSegmentPushers.local(iodeep)
    );
    System.out.println("INDEXONLY segment size=" + ioseg.getSize() + " (vs stored-value lucene size=" + lseg.getSize() + ")");
    final File iozip = new File((String) ioseg.getLoadSpec().get("path"));
    final File ioun = new File(tmp, "iounpacked");
    ioun.mkdirs();
    CompressionUtils.unzip(iozip, ioun);
    try (QueryableIndex li = indexIO.loadIndex(ioun)) {
      final io.druid.segment.column.Column col = li.getColumn("page");
      final boolean hasLucene = col != null
          && col.getExternalIndexKeys().contains(io.druid.segment.column.LuceneIndex.class);
      final boolean baseAbsent = col != null && !col.hasGenericColumn();
      System.out.println("INDEXONLY lucene present=" + hasLucene + " baseValueAbsent=" + baseAbsent);
      // query() internally does searcher.search(query, numRows) -> exercises numRows==maxDoc path
      final io.druid.segment.column.LuceneIndex idx =
          col.getExternalIndex(io.druid.segment.column.LuceneIndex.class).get();
      final org.apache.lucene.search.TopDocs td =
          idx.query(new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("page", "seoul")));
      System.out.println("INDEXONLY query(page:seoul) hits=" + td.scoreDocs.length);
      if (!hasLucene || !baseAbsent) {
        throw new IllegalStateException("index-only column wrong: hasLucene=" + hasLucene + " baseAbsent=" + baseAbsent);
      }
      System.out.println("INDEXONLY SMOKE OK");
    }
  }

  private static Map<String, Object> row(long ts, String country, String page, long hits)
  {
    return ImmutableMap.<String, Object>of("__time", ts, "country", country, "page", page, "hits", hits);
  }
}
