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

package io.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.ValueDesc;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class SegmentMetadataQueryTest
{
  private static final SegmentMetadataQueryRunnerFactory FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      TestHelper.NOOP_QUERYWATCHER
  );
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @SuppressWarnings("unchecked")
  public static QueryRunner makeMMappedQueryRunner(
      String segmentId,
      boolean rollup,
      QueryRunnerFactory factory
  )
  {
    QueryableIndex index = rollup ? TestIndex.getMMappedTestIndex() : TestIndex.getNoRollupMMappedTestIndex();
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        new QueryableIndexSegment(index, DataSegment.asKey(segmentId))
    );
  }

  @SuppressWarnings("unchecked")
  public static QueryRunner makeIncrementalIndexQueryRunner(
      String segmentId,
      boolean rollup,
      QueryRunnerFactory factory
  )
  {
    IncrementalIndex index = rollup ? TestIndex.getIncrementalTestIndex() : TestIndex.getNoRollupIncrementalTestIndex();
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        new IncrementalIndexSegment(index, DataSegment.asKey(segmentId))
    );
  }

  private final QueryRunner runner1;
  private final QueryRunner runner2;
  private final boolean mmap1;
  private final boolean mmap2;
  private final boolean rollup1;
  private final boolean rollup2;
  private final boolean differentIds;
  private final SegmentMetadataQuery testQuery;
  private final SegmentAnalysis expectedSegmentAnalysis1;
  private final SegmentAnalysis expectedSegmentAnalysis2;

  @Parameterized.Parameters(name = "mmap1 = {0}, mmap2 = {1}, rollup1 = {2}, rollup2 = {3}, differentIds = {4}")
  public static Collection<Object[]> constructorFeeder()
  {
    return Arrays.asList(
        new Object[]{true, true, true, true, false},
        new Object[]{true, false, true, false, false},
        new Object[]{false, true, true, false, false},
        new Object[]{false, false, false, false, false},
        new Object[]{false, false, true, true, false},
        new Object[]{false, false, false, true, true}
    );
  }

  public SegmentMetadataQueryTest(
      boolean mmap1,
      boolean mmap2,
      boolean rollup1,
      boolean rollup2,
      boolean differentIds
  )
  {
    final String id1 = differentIds ? "testSegment1" : "testSegment";
    final String id2 = differentIds ? "testSegment2" : "testSegment";
    this.runner1 = mmap1 ? makeMMappedQueryRunner(id1, rollup1, FACTORY) : makeIncrementalIndexQueryRunner(id1, rollup1, FACTORY);
    this.runner2 = mmap2 ? makeMMappedQueryRunner(id2, rollup2, FACTORY) : makeIncrementalIndexQueryRunner(id2, rollup2, FACTORY);
    this.mmap1 = mmap1;
    this.mmap2 = mmap2;
    this.rollup1 = rollup1;
    this.rollup2 = rollup2;
    this.differentIds = differentIds;
    testQuery = Druids.newSegmentMetadataQueryBuilder()
                      .dataSource("testing")
                      .intervals("2013/2014")
                      .toInclude(new ListColumnIncluderator(Arrays.asList("__time", "index", "placement")))
                      .merge(true)
                      .build();

    expectedSegmentAnalysis1 = new SegmentAnalysis(
        id1,
        Arrays.asList(
            new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")
        ),
        Arrays.asList("__time", "placement", "index"),
        Arrays.asList(
            new ColumnAnalysis(
                ValueDesc.LONG_TYPE,
                false,
                mmap1 ? 1081 : 0,
                null,
                1294790400000L,
                1302825600000L,
                null
            ),
            new ColumnAnalysis(
                ValueDesc.STRING_DIMENSION_TYPE,
                false,
                mmap1 ? 501 : 0,
                new long[]{1, 1},
                "preferred",
                "preferred",
                null
            ),
            new ColumnAnalysis(
                ValueDesc.DOUBLE_TYPE,
                false,
                mmap1 ? 6670 : 0,
                null,
                59.02102279663086D,
                1870.06103515625D,
                null
            )
        ),
        mmap1 ? 63874 : 0,
        1209,
        null,
        null,
        1
    );
    expectedSegmentAnalysis2 = new SegmentAnalysis(
        id2,
        Arrays.asList(
            new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")
        ),
        Arrays.asList("__time", "index", "placement"),
        Arrays.asList(
            new ColumnAnalysis(
                ValueDesc.LONG_TYPE,
                false,
                mmap2 ? 1081 : 0,
                null,
                1294790400000L,
                1302825600000L,
                null
            ),
            new ColumnAnalysis(
                ValueDesc.STRING_DIMENSION_TYPE,
                false,
                mmap2 ? 501 : 0,
                new long[]{1, 1},
                "preferred",
                "preferred",
                null
            ),
            new ColumnAnalysis(
                ValueDesc.DOUBLE_TYPE,
                false,
                mmap2 ? 6670 : 0,
                null,
                59.02102279663086D,
                1870.06103515625D,
                null
            )
            // null_column will be included only for incremental index, which makes a little bigger result than expected
        ),
        mmap2 ? 63874 : 0,
        1209,
        null,
        null,
        1
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSegmentMetadataQuery()
  {
    List<SegmentAnalysis> results = Sequences.toList(
        runner1.run(testQuery, Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expectedSegmentAnalysis1), results);

    results = Sequences.toList(
        runner1.run(testQuery.withMoreAnalysis(SegmentMetadataQuery.AnalysisType.INGESTED_NUMROW), Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expectedSegmentAnalysis1.withIngestedNumRows(1209)), results);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullCount()
  {
    Query<SegmentAnalysis> query = testQuery
        .withColumns(new ListColumnIncluderator(Arrays.asList("partial_null_column")))
        .withMoreAnalysis(
            SegmentMetadataQuery.AnalysisType.CARDINALITY,
            SegmentMetadataQuery.AnalysisType.NULL_COUNT
        );

    SegmentAnalysis expected = new SegmentAnalysis(
        expectedSegmentAnalysis1.getId(),
        Arrays.asList(new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        Arrays.asList("partial_null_column"),
        Arrays.asList(
            new ColumnAnalysis(
                ValueDesc.STRING_DIMENSION_TYPE, null, false, mmap1 ? 2956 : 0, new long[]{2, 2}, 1023, "value", "value", null
            )
        ),
        mmap1 ? 63874 : 0,
        1209,
        null,
        null,
        1
    );
    List<SegmentAnalysis> results = Sequences.toList(
        runner1.run(query, Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expected), results);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testVC()
  {
    Query<SegmentAnalysis> query = testQuery
        .withColumns(new ListColumnIncluderator(Arrays.asList("numeric-expr", "string-expr")))
        .withVirtualColumns(
            Arrays.<VirtualColumn>asList(
                new ExprVirtualColumn("index + 1", "numeric-expr"),
                new ExprVirtualColumn("concat(market, '|', quality)", "string-expr")
            )
        );

    SegmentAnalysis expected = new SegmentAnalysis(
        expectedSegmentAnalysis1.getId(),
        Arrays.asList(new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        Arrays.asList("numeric-expr", "string-expr"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.DOUBLE_TYPE, null, false, 0, null, -1, 60.02102279663086D, 1871.06103515625D, null),
            new ColumnAnalysis(ValueDesc.STRING_TYPE, null, false, 0, null, -1, "spot|automotive", "upfront|premium", null)
        ),
        mmap1 ? 63874 : 0,
        1209,
        null,
        null,
        1
    );
    List<SegmentAnalysis> results = Sequences.toList(
        runner1.run(query, Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expected), results);
  }

  @Test
  public void testSegmentMetadataQueryWithRollupMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        Arrays.asList("placement", "placementish"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.STRING_DIMENSION_TYPE, false, -1, null, null, null, null),
            new ColumnAnalysis(ValueDesc.STRING_MV_DIMENSION_TYPE, true, -1, null, null, null, null)
        ),
        (mmap1 ? 63874 : 0) + (mmap2 ? 63874 : 0),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        -1L,
        -1L,
        null,
        null,
        null,
        2,
        rollup1 != rollup2 ? null : rollup1
    );

    final SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                             .dataSource("testing")
                                             .intervals("2013/2014")
                                             .toInclude(new ListColumnIncluderator(Arrays.asList(
                                                 "placement",
                                                 "placementish"
                                             )))
                                             .analysisTypes(SegmentMetadataQuery.AnalysisType.ROLLUP)
                                             .merge(true)
                                             .build();

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithHasMultipleValuesMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        Arrays.asList("placement", "placementish"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.STRING_DIMENSION_TYPE, false, -1, new long[]{1, 2}, null, null, null),
            new ColumnAnalysis(ValueDesc.STRING_MV_DIMENSION_TYPE, true, -1, new long[]{9, 18}, null, null, null)
        ),
        (mmap1 ? 63874 : 0) + (mmap2 ? 63874 : 0),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        1
    );

    SegmentMetadataQuery query =
        Druids.newSegmentMetadataQueryBuilder()
              .dataSource("testing")
              .intervals("2013/2014")
              .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "placementish")))
              .analysisTypes(SegmentMetadataQuery.AnalysisType.CARDINALITY)
              .merge(true)
              .build();

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );

    mergedSegmentAnalysis = mergedSegmentAnalysis.withIngestedNumRows(1209 * 2);
    query = query.withMoreAnalysis(SegmentMetadataQuery.AnalysisType.INGESTED_NUMROW);
    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithComplexColumnMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        Arrays.asList("placement", "quality_uniques"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.STRING_DIMENSION_TYPE, false, -1, new long[]{1, 2}, null, null, null),
            new ColumnAnalysis("hyperUnique", false, -1, null, null, null, null)
        ),
        (mmap1 ? 63874 : 0) + (mmap2 ? 63874 : 0),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        1
    );
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource("testing")
                                       .intervals("2013/2014")
                                       .toInclude(new ListColumnIncluderator(Arrays.asList(
                                           "placement",
                                           "quality_uniques"
                                       )))
                                       .analysisTypes(SegmentMetadataQuery.AnalysisType.CARDINALITY)
                                       .merge(true)
                                       .build();

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ValueDesc.STRING_DIMENSION_TYPE,
        false,
        (mmap1 ? 501 : 0) + (mmap2 ? 501 : 0),
        new long[]{1, 2},
        "preferred",
        "preferred",
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("placement", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge2()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ValueDesc.STRING_DIMENSION_TYPE,
        false,
        (mmap1 ? 3010 : 0) + (mmap2 ? 3010 : 0),
        new long[]{3, 6},
        "spot",
        "upfront",
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("market", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge3()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ValueDesc.STRING_DIMENSION_TYPE,
        false,
        (mmap1 ? 3227 : 0) + (mmap2 ? 3227 : 0),
        new long[]{9, 18},
        "automotive",
        "travel",
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("quality", analysis);
  }

  private void testSegmentMetadataQueryWithDefaultAnalysisMerge(
      String column,
      ColumnAnalysis analysis
  )
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        Arrays.asList(expectedSegmentAnalysis1.getIntervals().get(0)),
        Arrays.asList("__time", column, "index"),
        Arrays.asList(
            new ColumnAnalysis(
                ValueDesc.LONG_TYPE,
                false,
                (mmap1 ? 1081 : 0) + (mmap2 ? 1081 : 0),
                null,
                1294790400000L,
                1302825600000L,
                null
            ),
            analysis,
            new ColumnAnalysis(
                ValueDesc.DOUBLE_TYPE,
                false,
                (mmap1 ? 6670 : 0) + (mmap2 ? 6670 : 0),
                null,
                59.02102279663086D,
                1870.06103515625D,
                null
            )
        ),
        expectedSegmentAnalysis1.getSerializedSize() + expectedSegmentAnalysis2.getSerializedSize(),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        1
    );

    SegmentMetadataQuery query = testQuery.withColumns(new ListColumnIncluderator(Arrays.asList("__time", "index", column)));

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithNoAnalysisTypesMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        Arrays.asList("placement"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.STRING_DIMENSION_TYPE, false, -1, null, null, null, null)
        ),
        (mmap1 ? 63874 : 0) + (mmap2 ? 63874 : 0),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        1
    );
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource("testing")
                                       .intervals("2013/2014")
                                       .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                                       .analysisTypes()
                                       .merge(true)
                                       .build();

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorsMerge()
  {
    final Map<String, AggregatorFactory> expectedAggregators = Maps.newHashMap();
    for (AggregatorFactory agg : TestIndex.METRIC_AGGS) {
      expectedAggregators.put(agg.getName(), agg.getCombiningFactory());
    }
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        Arrays.asList("placement"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.STRING_DIMENSION_TYPE, false, -1, null, null, null, null)
        ),
        (mmap1 ? 63874 : 0) + (mmap2 ? 63874 : 0),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        expectedAggregators,
        null,
        1
    );
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource("testing")
                                       .intervals("2013/2014")
                                       .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                                       .analysisTypes(SegmentMetadataQuery.AnalysisType.AGGREGATORS)
                                       .merge(true)
                                       .build();

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }


  @Test
  public void testSegmentMetadataQueryWithQueryGranularityMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        Arrays.asList("placement"),
        Arrays.asList(
            new ColumnAnalysis(ValueDesc.STRING_DIMENSION_TYPE, false, -1, null, null, null, null)
        ),
        (mmap1 ? 63874 : 0) + (mmap2 ? 63874 : 0),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        QueryGranularities.NONE,
        1
    );
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource("testing")
                                       .intervals("2013/2014")
                                       .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                                       .analysisTypes(SegmentMetadataQuery.AnalysisType.QUERYGRANULARITY)
                                       .merge(true)
                                       .build();

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner<SegmentAnalysis> myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                ),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(mergedSegmentAnalysis),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResults()
  {
    Result<BySegmentResultValue> bySegmentResult = new Result<BySegmentResultValue>(
        expectedSegmentAnalysis1.getIntervals().get(0).getStart(),
        new BySegmentResultValueClass(
            Arrays.asList(
                expectedSegmentAnalysis1
            ), expectedSegmentAnalysis1.getId(), testQuery.getIntervals().get(0)
        )
    );

    SegmentMetadataQuery query = testQuery.withOverriddenContext(ImmutableMap.<String, Object>of("bySegment", true));

    QueryToolChest toolChest = FACTORY.getToolchest();

    QueryRunner<SegmentAnalysis> singleSegmentQueryRunner = toolChest.preMergeQueryDecoration(runner1);
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                query,
                Execs.newDirectExecutorService(),
                //Note: It is essential to have atleast 2 query runners merged to reproduce the regression bug described in
                //https://github.com/druid-io/druid/pull/1172
                //the bug surfaces only when ordering is used which happens only when you have 2 things to compare
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(singleSegmentQueryRunner, singleSegmentQueryRunner),
                null
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        Arrays.asList(bySegmentResult, bySegmentResult),
        QueryRunners.run(query, myRunner),
        "failed SegmentMetadata bySegment query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSerde() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"intervals\":[\"2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z\"],\n"
                      + "  \"analysisTypes\":[\"cardinality\",\"serialized_size\"]\n"
                      + "}";

    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysisTypes = EnumSet.of(
        SegmentMetadataQuery.AnalysisType.CARDINALITY,
        SegmentMetadataQuery.AnalysisType.SERIALIZED_SIZE
    );

    Query query = MAPPER.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"), query.getIntervals().get(0));
    Assert.assertEquals(expectedAnalysisTypes, ((SegmentMetadataQuery) query).getAnalysisTypes());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));
  }

  @Test
  public void testSerdeWithDefaultInterval() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\"\n"
                      + "}";
    Query query = MAPPER.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT), query.getIntervals().get(0));
    Assert.assertTrue(((SegmentMetadataQuery) query).isUsingDefaultInterval());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));
  }

  @Test
  public void testCacheKeyWithListColumnIncluderator()
  {
    SegmentMetadataQuery oneColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource("testing")
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("foo")))
                                                .build();

    SegmentMetadataQuery twoColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource("testing")
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("fo", "o")))
                                                .build();

    SegmentMetadataQueryQueryToolChest toolChest = new SegmentMetadataQueryQueryToolChest(null);
    final byte[] oneColumnQueryCacheKey = toolChest.getCacheStrategyIfExists(oneColumnQuery)
                                                   .computeCacheKey(oneColumnQuery, 1000);

    final byte[] twoColumnQueryCacheKey = toolChest.getCacheStrategyIfExists(twoColumnQuery)
                                                   .computeCacheKey(twoColumnQuery, 1000);

    Assert.assertFalse(Arrays.equals(oneColumnQueryCacheKey, twoColumnQueryCacheKey));
  }
}
