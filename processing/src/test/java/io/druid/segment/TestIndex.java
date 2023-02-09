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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class TestIndex
{
  public static final String[] COLUMNS = new String[]{
      "ts",
      "market",
      "quality",
      "placement",
      "placementish",
      "index",
      "partial_null_column",
      "null_column",
      "quality_uniques",
      "indexMin",
      "indexMaxPlusTen",
      "indexDecimal"
  };
  public static final String[] DIMENSIONS = new String[]{
      "market",
      "quality",
      "placement",
      "placementish",
      "partial_null_column",
      "null_column",
      };
  public static final String[] METRICS = new String[]{"index", "indexMin", "indexMaxPlusTen"};
  public static final StringInputRowParser PARSER = new StringInputRowParser(
      new DelimitedParseSpec(
          new DefaultTimestampSpec("ts", "iso", null),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList(DIMENSIONS)), null, null),
          "\t",
          "\u0001",
          Arrays.asList(COLUMNS)
      )
      , "utf8"
  );
  private static final Logger log = new Logger(TestIndex.class);

  public static final Interval INTERVAL = new Interval("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  public static final Interval INTERVAL_TOP = new Interval("2011-01-12T00:00:00.000Z/2011-03-01T00:00:00.000Z");
  public static final Interval INTERVAL_BOTTOM = new Interval("2011-03-01T00:00:00.000Z/2011-05-01T00:00:00.000Z");

  public static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new GenericSumAggregatorFactory("index", "index", ValueDesc.FLOAT),
      new GenericMinAggregatorFactory("indexMin", "index", ValueDesc.FLOAT),
      new DoubleMaxAggregatorFactory("indexMaxPlusTen", null, "index + 10"),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality"),
      new GenericSumAggregatorFactory("indexDecimal", "index", ValueDesc.DECIMAL)
  };
  private static final IndexSpec indexSpec = IndexSpec.DEFAULT;

  private static final IndexMerger INDEX_MERGER = TestHelper.getTestIndexMergerV9();
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  static {
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
    }
  }

  private static IncrementalIndex realtimeIndex = null;
  private static IncrementalIndex noRollupRealtimeIndex = null;
  private static QueryableIndex mmappedIndex = null;
  private static QueryableIndex noRollupMmappedIndex = null;
  private static QueryableIndex topIndex = null;
  private static QueryableIndex bottomIndex = null;
  private static QueryableIndex mergedRealtime = null;

  public static TestQuerySegmentWalker segmentWalker = TestHelper.newWalker();

  public static final IncrementalIndexSchema SAMPLE_SCHEMA = new IncrementalIndexSchema.Builder()
      .withMinTimestamp(new DateTime("2011-01-01T00:00:00.000Z").getMillis())
      .withQueryGranularity(QueryGranularities.NONE)
      .withDimensions(Arrays.asList(DIMENSIONS))
      .withMetrics(METRIC_AGGS)
      .withRollup(true)
      .build();

  public static final DataSegment SEGMENT = new DataSegment(
      "realtime", INTERVAL, "0", null, Arrays.asList(DIMENSIONS), Arrays.asList(METRICS), null, null, 0
  );

  public static final String REALTIME = "realtime";
  public static final String REALTIME_NOROLLUP = "realtime_norollup";
  public static final String MMAPPED_SPLIT = "mmapped-split";

  public static final String[] DS_NAMES = new String[]{
      "realtime", "realtime_norollup", "mmapped", "mmapped_norollup", "mmapped-split", "mmapped_merged"
  };

  static {
    segmentWalker.add(SEGMENT.withDataSource("realtime"), getIncrementalTestIndex());
    segmentWalker.add(SEGMENT.withDataSource("realtime_norollup"), getNoRollupIncrementalTestIndex());
    segmentWalker.add(SEGMENT.withDataSource("mmapped"), getMMappedTestIndex());
    segmentWalker.add(SEGMENT.withDataSource("mmapped_norollup"), getNoRollupMMappedTestIndex());
    mergedRealtimeIndex();
    segmentWalker.add(SEGMENT.withDataSource("mmapped-split").withInterval(INTERVAL_TOP), topIndex);
    segmentWalker.add(SEGMENT.withDataSource("mmapped-split").withInterval(INTERVAL_BOTTOM), bottomIndex);
    segmentWalker.add(SEGMENT.withDataSource("mmapped_merged"), mergedRealtime);
    segmentWalker.addSalesIndex()
                 .addCategoryAliasIndex()
                 .addTpchIndex()
                 .addSsbIndex();
  }

  public static synchronized IncrementalIndex getIncrementalTestIndex()
  {
    if (realtimeIndex == null) {
      realtimeIndex = makeRealtimeIndex("druid.sample.tsv", true);
    }
    return realtimeIndex;
  }

  public static synchronized IncrementalIndex getNoRollupIncrementalTestIndex()
  {
    if (noRollupRealtimeIndex == null) {
      noRollupRealtimeIndex = makeRealtimeIndex("druid.sample.tsv", false);
    }
    return noRollupRealtimeIndex;
  }

  public static synchronized QueryableIndex getMMappedTestIndex()
  {
    if (mmappedIndex == null) {
      mmappedIndex = TestHelper.persistRealtimeAndLoadMMapped(getIncrementalTestIndex(), indexSpec);
    }
    return mmappedIndex;
  }

  public static synchronized QueryableIndex getNoRollupMMappedTestIndex()
  {
    if (noRollupMmappedIndex == null) {
      noRollupMmappedIndex = TestHelper.persistRealtimeAndLoadMMapped(getNoRollupIncrementalTestIndex(), indexSpec);
    }
    return noRollupMmappedIndex;
  }

  public static TestQuerySegmentWalker addBasicTestIndex(TestQuerySegmentWalker segmentWalker)
  {
    return segmentWalker.add(SEGMENT.withDataSource("mmapped"), mmappedIndex)
                        .add(SEGMENT.withDataSource("mmapped_norollup"), noRollupMmappedIndex)
                        .add(SEGMENT.withDataSource("realtime"), realtimeIndex)
                        .add(SEGMENT.withDataSource("realtime_norollup"), noRollupRealtimeIndex)
                        .add(SEGMENT.withDataSource("mmapped-split").withInterval(INTERVAL_TOP), topIndex)
                        .add(SEGMENT.withDataSource("mmapped-split").withInterval(INTERVAL_BOTTOM), bottomIndex)
                        .add(SEGMENT.withDataSource("mmapped_merged"), mergedRealtime);
  }

  public static synchronized QueryableIndex mergedRealtimeIndex()
  {
    if (mergedRealtime == null) {
      try {
        IncrementalIndex top = makeRealtimeIndex("druid.sample.tsv.top", true);
        IncrementalIndex bottom = makeRealtimeIndex("druid.sample.tsv.bottom", true);

        File tmpFile = File.createTempFile("yay", "who");
        tmpFile.delete();

        File topFile = new File(tmpFile, "top");
        File bottomFile = new File(tmpFile, "bottom");
        File mergedFile = new File(tmpFile, "merged");

        topFile.mkdirs();
        topFile.deleteOnExit();
        bottomFile.mkdirs();
        bottomFile.deleteOnExit();
        mergedFile.mkdirs();
        mergedFile.deleteOnExit();

        topIndex = INDEX_IO.loadIndex(INDEX_MERGER.persist(top, INTERVAL_TOP, topFile, indexSpec));
        bottomIndex = INDEX_IO.loadIndex(INDEX_MERGER.persist(bottom, INTERVAL_BOTTOM, bottomFile, indexSpec));
        mergedRealtime = INDEX_IO.loadIndex(INDEX_MERGER.mergeQueryableIndex(Arrays.asList(topIndex, bottomIndex), true, METRIC_AGGS, mergedFile, indexSpec));
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return mergedRealtime;
  }

  public static IncrementalIndex makeRealtimeIndex(String resourceFilename, boolean rollup)
  {
    return makeRealtimeIndex(asCharSource(resourceFilename), rollup);
  }

  public static CharSource asCharSource(String resourceFilename)
  {
    return asCharSource(TestIndex.class.getClassLoader(), resourceFilename);
  }

  public static CharSource asCharSource(ClassLoader loader, String resourceFilename)
  {
    final URL resource = loader.getResource(resourceFilename);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceFilename);
    }
    log.info("Realtime loading index file[%s]", resource);
    return Resources.asByteSource(resource).asCharSource(Charsets.UTF_8);
  }

  public static IncrementalIndex makeRealtimeIndex(
      CharSource source,
      IncrementalIndexSchema schema,
      StringInputRowParser parser
  )
  {
    final IncrementalIndex retVal = new OnheapIncrementalIndex(schema, true, 10000);

    try {
      return loadIncrementalIndex(retVal, source, parser);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static IncrementalIndex makeRealtimeIndex(CharSource source, boolean rollup)
  {
    final IncrementalIndex retVal = new OnheapIncrementalIndex(SAMPLE_SCHEMA.withRollup(rollup), true, 10000);

    try {
      return loadIncrementalIndex(retVal, source);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static IncrementalIndex loadIncrementalIndex(
      final IncrementalIndex retVal,
      final CharSource source
  ) throws IOException
  {

    return loadIncrementalIndex(retVal, source, PARSER);
  }

  public static IncrementalIndex loadIncrementalIndex(
      final IncrementalIndex retVal,
      final CharSource source,
      final StringInputRowParser parser
  ) throws IOException
  {
    final AtomicLong startTime = new AtomicLong();
    int lineCount = source.readLines(
        new LineProcessor<Integer>()
        {
          boolean runOnce = false;
          int lineCount = 0;

          @Override
          public boolean processLine(String line) throws IOException
          {
            if (!runOnce) {
              startTime.set(System.currentTimeMillis());
              runOnce = true;
            }
            retVal.add(parser.parse(line));

            ++lineCount;
            return true;
          }

          @Override
          public Integer getResult()
          {
            return lineCount;
          }
        }
    );

    log.info("Loaded %,d lines in %,d millis.", lineCount, System.currentTimeMillis() - startTime.get());

    return retVal;
  }
}
