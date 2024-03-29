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

package io.druid.segment.filter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.collections.spatial.search.RadiusBound;
import com.metamx.collections.spatial.search.RectangularBound;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.SpatialDimFilter;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 */
@RunWith(Parameterized.class)
public class SpatialFilterBonusTest
{
  public static final int NUM_POINTS = 5000;
  private static Interval DATA_INTERVAL = new Interval("2013-01-01/2013-01-07");
  private static AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("val", "val")
  };
  private static List<String> DIMS = Lists.newArrayList("dim", "dim.geo");
  private static final IndexMerger INDEX_MERGER = TestHelper.getTestIndexMerger();
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  private final Segment segment;

  public SpatialFilterBonusTest(Segment segment)
  {
    this.segment = segment;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final IndexSpec indexSpec = IndexSpec.DEFAULT;
    final IncrementalIndex rtIndex = makeIncrementalIndex();
    final QueryableIndex mMappedTestIndex = makeQueryableIndex(indexSpec);
    final QueryableIndex mergedRealtimeIndex = makeMergedQueryableIndex(indexSpec);
    return Arrays.asList(
        new Object[][]{
            {
                new IncrementalIndexSegment(rtIndex, DataSegment.asKey("incremental"))
            },
            {
                new QueryableIndexSegment(mMappedTestIndex, DataSegment.asKey("mmaped"))
            },
            {
                new QueryableIndexSegment(mergedRealtimeIndex, DataSegment.asKey("merged"))
            }
        }
    );
  }

  private static IncrementalIndex makeIncrementalIndex() throws IOException
  {
    IncrementalIndex theIndex = new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                            .withQueryGranularity(QueryGranularities.DAY)
                                            .withMetrics(METRIC_AGGS)
                                            .withDimensionsSpec(
                                                new DimensionsSpec(
                                                    null,
                                                    null,
                                                    Arrays.asList(
                                                        new SpatialDimensionSchema(
                                                            "dim.geo",
                                                            Lists.<String>newArrayList()
                                                        )
                                                    )
                                                )
                                            ).build(),
        false,
        NUM_POINTS
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-01").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-01").toString(),
                "dim", "foo",
                "dim.geo", "0.0,0.0",
                "val", 17L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-02").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-02").toString(),
                "dim", "foo",
                "dim.geo", "1.0,3.0",
                "val", 29L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-03").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-03").toString(),
                "dim", "foo",
                "dim.geo", "4.0,2.0",
                "val", 13L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-04").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-04").toString(),
                "dim", "foo",
                "dim.geo", "7.0,3.0",
                "val", 91L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "dim.geo", "8.0,6.0",
                "val", 47L
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "dim.geo", "_mmx.unknown",
                "val", 501L
            )
        )
    );

    // Add a bunch of random points, without replacement
    Set<String> alreadyChosen = Sets.newHashSet();
    Random rand = new Random();
    for (int i = 6; i < NUM_POINTS; i++) {
      String coord = null;
      while (coord == null) {
        coord = String.format(
            "%s,%s",
            (float) (rand.nextFloat() * 10 + 10.0),
            (float) (rand.nextFloat() * 10 + 10.0)
        );
        if (!alreadyChosen.add(coord)) {
          coord = null;
        }
      }
      theIndex.add(
          new MapBasedInputRow(
              new DateTime("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-01").toString(),
                  "dim", "boo",
                  "dim.geo", coord,
                  "val", i
              )
          )
      );
    }

    return theIndex;
  }

  private static QueryableIndex makeQueryableIndex(IndexSpec indexSpec) throws IOException
  {
    IncrementalIndex theIndex = makeIncrementalIndex();
    File tmpFile = File.createTempFile("billy", "yay");
    tmpFile.delete();
    tmpFile.mkdirs();
    tmpFile.deleteOnExit();

    INDEX_MERGER.persist(theIndex, tmpFile, indexSpec);
    return INDEX_IO.loadIndex(tmpFile);
  }

  private static QueryableIndex makeMergedQueryableIndex(final IndexSpec indexSpec)
  {
    try {
      IncrementalIndex first = new OnheapIncrementalIndex(
          new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                              .withQueryGranularity(QueryGranularities.DAY)
                                              .withMetrics(METRIC_AGGS)
                                              .withDimensionsSpec(
                                                  new DimensionsSpec(
                                                      null,
                                                      null,
                                                      Arrays.asList(
                                                          new SpatialDimensionSchema(
                                                              "dim.geo",
                                                              Lists.<String>newArrayList()
                                                          )
                                                      )
                                                  )

                                              ).build(),
          false,
          NUM_POINTS
      );
      IncrementalIndex second = new OnheapIncrementalIndex(
          new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                              .withQueryGranularity(QueryGranularities.DAY)
                                              .withMetrics(METRIC_AGGS)
                                              .withDimensionsSpec(
                                                  new DimensionsSpec(
                                                      null,
                                                      null,
                                                      Arrays.asList(
                                                          new SpatialDimensionSchema(
                                                              "dim.geo",
                                                              Lists.<String>newArrayList()
                                                          )
                                                      )
                                                  )
                                              ).build(),
          false,
          NUM_POINTS
      );
      IncrementalIndex third = new OnheapIncrementalIndex(
          new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                              .withQueryGranularity(QueryGranularities.DAY)
                                              .withMetrics(METRIC_AGGS)
                                              .withDimensionsSpec(
                                                  new DimensionsSpec(
                                                      null,
                                                      null,
                                                      Arrays.asList(
                                                          new SpatialDimensionSchema(
                                                              "dim.geo",
                                                              Lists.<String>newArrayList()
                                                          )
                                                      )
                                                  )

                                              ).build(),
          false,
          NUM_POINTS
      );


      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-01").toString(),
                  "dim", "foo",
                  "dim.geo", "0.0,0.0",
                  "val", 17L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-02").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-02").toString(),
                  "dim", "foo",
                  "dim.geo", "1.0,3.0",
                  "val", 29L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-03").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-03").toString(),
                  "dim", "foo",
                  "dim.geo", "4.0,2.0",
                  "val", 13L
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "dim.geo", "_mmx.unknown",
                  "val", 501L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-04").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-04").toString(),
                  "dim", "foo",
                  "dim.geo", "7.0,3.0",
                  "val", 91L
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "dim.geo", "8.0,6.0",
                  "val", 47L
              )
          )
      );

      // Add a bunch of random points
      Random rand = new Random();
      for (int i = 6; i < NUM_POINTS; i++) {
        third.add(
            new MapBasedInputRow(
                new DateTime("2013-01-01").getMillis(),
                DIMS,
                ImmutableMap.<String, Object>of(
                    "timestamp", new DateTime("2013-01-01").toString(),
                    "dim", "boo",
                    "dim.geo", String.format(
                        "%s,%s",
                        (float) (rand.nextFloat() * 10 + 10.0),
                        (float) (rand.nextFloat() * 10 + 10.0)
                    ),
                    "val", i
                )
            )
        );
      }


      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();

      File firstFile = new File(tmpFile, "first");
      File secondFile = new File(tmpFile, "second");
      File thirdFile = new File(tmpFile, "third");
      File mergedFile = new File(tmpFile, "merged");

      firstFile.mkdirs();
      firstFile.deleteOnExit();
      secondFile.mkdirs();
      secondFile.deleteOnExit();
      thirdFile.mkdirs();
      thirdFile.deleteOnExit();
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      INDEX_MERGER.persist(first, DATA_INTERVAL, firstFile, indexSpec);
      INDEX_MERGER.persist(second, DATA_INTERVAL, secondFile, indexSpec);
      INDEX_MERGER.persist(third, DATA_INTERVAL, thirdFile, indexSpec);

      QueryableIndex mergedRealtime = INDEX_IO.loadIndex(
          INDEX_MERGER.mergeQueryableIndex(
              Arrays.asList(
                  INDEX_IO.loadIndex(firstFile),
                  INDEX_IO.loadIndex(secondFile),
                  INDEX_IO.loadIndex(thirdFile)
              ),
              true,
              METRIC_AGGS,
              mergedFile,
              indexSpec
          )
      );

      return mergedRealtime;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testSpatialQuery()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(QueryGranularities.ALL)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RadiusBound(new float[]{0.0f, 0.0f}, 5)
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2013-01-01T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 3L)
                .put("val", 59L)
                .build()
        )
    );
    try {
      TimeseriesQueryQueryToolChest toolChest = new TimeseriesQueryQueryToolChest();
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
          toolChest,
          new TimeseriesQueryEngine(),
          new QueryConfig(),
          TestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          toolChest.mergeResults(factory.createRunner(segment, null)),
          factory.getToolchest()
      );
      HashMap<String, Object> context = new HashMap<String, Object>();
      TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "");
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testSpatialQueryMorePoints()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(QueryGranularities.DAY)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RectangularBound(new float[]{0.0f, 0.0f}, new float[]{9.0f, 9.0f})
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2013-01-01T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 17L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-02T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 29L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-03T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 13L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-04T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 91L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-05T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 47L)
                .build()
        )
    );
    try {
      TimeseriesQueryQueryToolChest toolChest = new TimeseriesQueryQueryToolChest();
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
          toolChest,
          new TimeseriesQueryEngine(),
          new QueryConfig(),
          TestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          toolChest.mergeResults(factory.createRunner(segment, null)),
          factory.getToolchest()
      );
      HashMap<String, Object> context = new HashMap<String, Object>();
      TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "");
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testSpatialQueryFilteredAggregator()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(QueryGranularities.DAY)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .aggregators(
                                      Arrays.asList(
                                          new CountAggregatorFactory("rows"),
                                          new FilteredAggregatorFactory(
                                              new LongSumAggregatorFactory("valFiltered", "val"),
                                              new SpatialDimFilter(
                                                  "dim.geo",
                                                  new RectangularBound(new float[]{0.0f, 0.0f}, new float[]{9.0f, 9.0f})
                                              )
                                          ),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2013-01-01T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 4995L)
                .put("val", 12497502L)
                .put("valFiltered", 17L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-02T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 29L)
                .put("valFiltered", 29L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-03T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 13L)
                .put("valFiltered", 13L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-04T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 1L)
                .put("val", 91L)
                .put("valFiltered", 91L)
                .build()
        ),
        new MapBasedRow(
            new DateTime("2013-01-05T00:00:00.000Z"),
            ImmutableMap.<String, Object>builder()
                .put("rows", 2L)
                .put("val", 548L)
                .put("valFiltered", 47L)
                .build()
        )
    );
    try {
      TimeseriesQueryQueryToolChest toolChest = new TimeseriesQueryQueryToolChest();
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
          toolChest,
          new TimeseriesQueryEngine(),
          new QueryConfig(),
          TestHelper.NOOP_QUERYWATCHER
      );

      QueryRunner runner = new FinalizeResultsQueryRunner(
          toolChest.mergeResults(factory.createRunner(segment, null)),
          factory.getToolchest()
      );
      HashMap<String, Object> context = new HashMap<String, Object>();
      TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "");
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
