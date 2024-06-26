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

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.DateTimes;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.js.JavaScriptConfig;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@RunWith(Parameterized.class)
public class TopNQueryRunnerTest extends QueryRunnerTestHelper
{
  public static final Map<String, Object> CONTEXT = ImmutableMap.of();

  @Parameterized.Parameters(name = "{0}:{1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            Arrays.<Set<Object>>asList(
                Sets.<Object>newHashSet(TestIndex.DS_NAMES),
                Sets.<Object>newHashSet(Direction.ASCENDING, Direction.DESCENDING)
            )
        ), new Function<List<Object>, Object[]>()
        {
          @Override
          public Object[] apply(List<Object> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private final String dataSource;
  private final Direction direction;

  public TopNQueryRunnerTest(String dataSource, Direction direction)
  {
    this.dataSource = dataSource;
    this.direction = direction;
  }

  private List<Result<TopNResultValue>> assertExpectedResults(
      Iterable<Result<TopNResultValue>> expectedResults,
      TopNQuery query
  )
  {
    List<Result<TopNResultValue>> results = Sequences.toList(query.run(TestIndex.segmentWalker, CONTEXT));
    TestHelper.assertExpectedResults(expectedResults, results);
    return results;
  }

  private List<Map<String, Object>> createExpected(String[] columnNames, Object[]... values)
  {
    List<Map<String, Object>> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < columnNames.length; i++) {
        theVals.put(columnNames[i], value[i]);
      }
      expected.add(theVals);
    }
    return expected;
  }

  @Test
  public void testFullOnTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(indexMetric, direction))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Map<String, Object>> expectedRows = createExpected(
        new String[]{"market", "rows", "index", "addRowsIndexConstant", "uniques", "maxIndex", "minIndex"},
        new Object[]{"total_market", 186, 215679.82879638672D, 215866.82879638672D, UNIQUES_2, 1743.9217529296875D, 792.3260498046875D},
        new Object[]{"upfront", 186, 192046.1060180664D, 192233.1060180664D, UNIQUES_2, 1870.06103515625D, 545.9906005859375D},
        new Object[]{"spot", 837, 95606.57232284546D, 96444.57232284546D, UNIQUES_9, 277.2735290527344D, 59.02102279663086D}
    );
    if (direction == Direction.ASCENDING) {
      Collections.reverse(expectedRows);
    }

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );

    assertExpectedResults(expectedResults, query);

    QueryToolChest toolChest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        TestHelper.testTopNQueryEngine()
    );

    // with projection
    query = query.withOutputColumns(Arrays.asList("market", "rows", "index"));
    expectedRows = createExpected(
        new String[]{"market", "rows", "index"},
        new Object[]{"total_market", 186, 215679.82879638672D},
        new Object[]{"upfront", 186, 192046.1060180664D},
        new Object[]{"spot", 837, 95606.57232284546D}
    );
    if (direction == Direction.ASCENDING) {
      Collections.reverse(expectedRows);
    }
    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNOverPostAggs()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(addRowsIndexConstantMetric, direction))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Map<String, Object>> expectedRows = createExpected(
        new String[]{"market", "rows", "index", "addRowsIndexConstant", "uniques", "maxIndex", "minIndex"},
        new Object[]{"total_market", 186, 215679.82879638672D, 215866.82879638672D, UNIQUES_2, 1743.9217529296875D, 792.3260498046875D},
        new Object[]{"upfront", 186, 192046.1060180664D, 192233.1060180664D, UNIQUES_2, 1870.06103515625D, 545.9906005859375D},
        new Object[]{"spot", 837, 95606.57232284546D, 96444.57232284546D, UNIQUES_9, 277.2735290527344D, 59.02102279663086D}
    );
    if (direction == Direction.ASCENDING) {
      Collections.reverse(expectedRows);
    }

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testFullOnTopNOverUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(uniqueMetric, direction))
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Map<String, Object>> expectedRows;
    if (direction == Direction.DESCENDING) {
      expectedRows = createExpected(
          new String[]{"market", "rows", "index", "addRowsIndexConstant", "uniques", "maxIndex", "minIndex"},
          new Object[]{"spot", 837, 95606.57232284546D, 96444.57232284546D, UNIQUES_9, 277.2735290527344D, 59.02102279663086D},
          new Object[]{"total_market", 186, 215679.82879638672D, 215866.82879638672D, UNIQUES_2, 1743.9217529296875D, 792.3260498046875D},
          new Object[]{"upfront", 186, 192046.1060180664D, 192233.1060180664D, UNIQUES_2, 1870.06103515625D, 545.9906005859375D}
      );
    } else {
      expectedRows = createExpected(
          new String[]{"market", "rows", "index", "addRowsIndexConstant", "uniques", "maxIndex", "minIndex"},
          new Object[]{"total_market", 186, 215679.82879638672D, 215866.82879638672D, UNIQUES_2, 1743.9217529296875D, 792.3260498046875D},
          new Object[]{"upfront", 186, 192046.1060180664D, 192233.1060180664D, UNIQUES_2, 1870.06103515625D, 545.9906005859375D},
          new Object[]{"spot", 837, 95606.57232284546D, 96444.57232284546D, UNIQUES_9, 277.2735290527344D, 59.02102279663086D}
      );
    }

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverMissingUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(new HyperUniquesAggregatorFactory("uniques", "missingUniques"))
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("uniques", 0d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("uniques", 0d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("uniques", 0d)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverHyperUniqueFinalizingPostAggregator()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(hyperUniqueFinalizingPostAggMetric, direction))
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.qualityUniques)
        )
        .postAggregators(
            Arrays.<PostAggregator>asList(new HyperUniqueFinalizingPostAggregator(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                QueryRunnerTestHelper.uniqueMetric
            ))
        )
        .build();

    List<Map<String, Object>> expectedRows;
    if (direction == Direction.DESCENDING) {
      expectedRows = createExpected(
          new String[]{"market", "uniques", "hyperUniqueFinalizingPostAggMetric"},
          new Object[]{"spot", UNIQUES_9, UNIQUES_9},
          new Object[]{"total_market", UNIQUES_2, UNIQUES_2},
          new Object[]{"upfront", UNIQUES_2, UNIQUES_2}
      );
    } else {
      expectedRows = createExpected(
          new String[]{"market", "uniques", "hyperUniqueFinalizingPostAggMetric"},
          new Object[]{"total_market", UNIQUES_2, UNIQUES_2},
          new Object[]{"upfront", UNIQUES_2, UNIQUES_2},
          new Object[]{"spot", UNIQUES_9, UNIQUES_9}
      );
    }

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverHyperUniqueExpression()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(hyperUniqueFinalizingPostAggMetric, direction))
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.qualityUniques)
        )
        .postAggregators(
            Collections.<PostAggregator>singletonList(new MathPostAggregator(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                "uniques + 1"
            ))
        )
        .build();

    List<Map<String, Object>> expectedRows;
    if (direction == Direction.DESCENDING) {
      expectedRows = createExpected(
          new String[]{"market", "uniques", "hyperUniqueFinalizingPostAggMetric"},
          new Object[]{"spot", UNIQUES_9, UNIQUES_9 + 1},
          new Object[]{"total_market", UNIQUES_2, UNIQUES_2 + 1},
          new Object[]{"upfront", UNIQUES_2, UNIQUES_2 + 1}
      );
    } else {
      expectedRows = createExpected(
          new String[]{"market", "uniques", "hyperUniqueFinalizingPostAggMetric"},
          new Object[]{"total_market", UNIQUES_2, UNIQUES_2 + 1},
          new Object[]{"upfront", UNIQUES_2, UNIQUES_2 + 1},
          new Object[]{"spot", UNIQUES_9, UNIQUES_9 + 1}
      );
    }

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverHyperUniqueExpressionRounded()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(hyperUniqueFinalizingPostAggMetric, direction))
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.qualityUniquesRounded)
        )
        .postAggregators(
            Collections.<PostAggregator>singletonList(new MathPostAggregator(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                "uniques + 1"
            ))
        )
        .build();

    List<Map<String, Object>> expectedRows;
    if (direction == Direction.DESCENDING) {
      expectedRows = createExpected(
          new String[]{"market", "uniques", "hyperUniqueFinalizingPostAggMetric"},
          new Object[]{"spot", 9L, 10L},
          new Object[]{"total_market", 2L, 3L},
          new Object[]{"upfront", 2L, 3L}
      );
    } else {
      expectedRows = createExpected(
          new String[]{"market", "uniques", "hyperUniqueFinalizingPostAggMetric"},
          new Object[]{"total_market", 2L, 3L},
          new Object[]{"upfront", 2L, 3L},
          new Object[]{"spot", 9L, 10L}
      );
    }

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-01-12T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopNBySegment()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(indexMetric, direction))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Map<String, Object>> expectedRows = createExpected(
        new String[]{"market", "index", "addRowsIndexConstant", "rows", "uniques"},
        new Object[]{"total_market", 5351.814697265625D, 5356.814697265625D, 4, UNIQUES_2},
        new Object[]{"upfront", 4875.669677734375D, 4880.669677734375D, 4, UNIQUES_2},
        new Object[]{"spot", 2231.8768157958984D, 2250.8768157958984D, 18, UNIQUES_9}
    );
    if (direction == Direction.ASCENDING) {
      Collections.reverse(expectedRows);
    }
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(new DateTime("2011-04-01T00:00:00.000Z"), new TopNResultValue(expectedRows))
    );
    assertExpectedResults(expectedResults, query);

    boolean mmapedSplit = dataSource.equals(TestIndex.MMAPPED_SPLIT);
    DateTime timestamp = DateTimes.utc(mmapedSplit ? "2011-03-01" : "2011-01-12");
    String segmentId = mmapedSplit ?
                       dataSource + "_2011-03-01T00:00:00.000Z_2011-05-01T00:00:00.000Z_0" :
                       dataSource + "_2011-01-12T00:00:00.000Z_2011-05-01T00:00:00.000Z_0";
    List<Result<BySegmentTopNResultValue>> expected = Arrays.asList(
        new Result<BySegmentTopNResultValue>(
            timestamp,
            new BySegmentTopNResultValue(expectedResults, segmentId, new Interval("2011-04-01/2011-04-03"))
        )
    );
    Query bySegmentQuery = query.withOverriddenContext("bySegment", true);
    List<Result<BySegmentTopNResultValue>> bySegment = Sequences.toList(
        bySegmentQuery.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    TestHelper.assertExpectedResults(expected, bySegment);
  }

  @Test
  public void testTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test(expected=IllegalArgumentException.class)
  @Ignore("now it's resolved in query runner with resolver")
  public void testTopNOnVirtual()
  {
    // cannot do this, currently.. use group-by query
    new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .virtualColumn(new ExprVirtualColumn("index", "metric"))
        .dimension("metric")
        .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
        .metric("count")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .build();
  }

  @Test
  public void testTopNByUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec("uniques"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithOrFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.marketDimension, "total_market", "upfront", "spot")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithOrFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.marketDimension, "total_market", "upfront")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.marketDimension, "upfront")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 2,
                        "index", 2591.68359375D,
                        "addRowsIndexConstant", 2594.68359375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 2,
                        "index", 2508.39599609375D,
                        "addRowsIndexConstant", 2511.39599609375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 2,
                        "index", 220.63774871826172D,
                        "addRowsIndexConstant", 223.63774871826172D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithFilter2OneDay()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(
            new MultipleIntervalSegmentSpec(
                Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"))
            )
        )
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 1,
                        "index", new Float(1447.341160).doubleValue(),
                        "addRowsIndexConstant", new Float(1449.341160).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 1,
                        "index", new Float(1314.839715).doubleValue(),
                        "addRowsIndexConstant", new Float(1316.839715).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 1,
                        "index", new Float(109.705815).doubleValue(),
                        "addRowsIndexConstant", new Float(111.705815).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentFilterInOr()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.marketDimension, "total_market", "upfront", "billyblank")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.marketDimension, "billyblank")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    HashMap<String, Object> context = new HashMap<String, Object>();
    assertExpectedResults(
        Arrays.asList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ), query
    );
  }

  @Test
  public void testTopNWithNonExistentFilterMultiDim()
  {
    DimFilter andDimFilter = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "billyblank"),
        SelectorDimFilter.of(QueryRunnerTestHelper.qualityDimension, "mezzanine")
    );
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(andDimFilter)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    assertExpectedResults(
        Arrays.asList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ), query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.placementishDimension, "m")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final List<Result<TopNResultValue>> expected = Sequences.toList(
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(Granularities.ALL)
            .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
            .dimension(QueryRunnerTestHelper.marketDimension)
            .metric(QueryRunnerTestHelper.indexMetric)
            .threshold(4)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(QueryRunnerTestHelper.commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
            .build()
            .run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );

    assertExpectedResults(expected, query);
  }

  @Test
  public void testTopNWithMultiValueDimFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.placementishDimension, "m", "a", "b")
        .dimension(QueryRunnerTestHelper.qualityDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expected = Sequences.toList(
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(Granularities.ALL)
            .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine", "automotive", "business")
            .dimension(QueryRunnerTestHelper.qualityDimension)
            .metric(QueryRunnerTestHelper.indexMetric)
            .threshold(4)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(QueryRunnerTestHelper.commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
            .build()
            .run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    assertExpectedResults(expected, query);
  }

  @Test
  public void testTopNWithMultiValueDimFilter3()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.placementishDimension, "a")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "a",
                        "rows", 2,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 2,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithMultiValueDimFilter4()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.placementishDimension, "a", "b")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 4,
                        "index", 514.868408203125D,
                        "addRowsIndexConstant", 519.868408203125D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish",
                        "a", "rows", 2,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "b",
                        "rows", 2,
                        "index", 231.557373046875D,
                        "addRowsIndexConstant", 234.557373046875D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithMultiValueDimFilter5()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 26,
                        "index", 12459.361190795898D,
                        "addRowsIndexConstant", 12486.361190795898D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "p",
                        "rows", 6,
                        "index", 5407.213653564453D,
                        "addRowsIndexConstant", 5414.213653564453D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "m",
                        "rows", 6,
                        "index", 5320.717338562012D,
                        "addRowsIndexConstant", 5327.717338562012D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "t",
                        "rows", 4,
                        "index", 422.3440856933594D,
                        "addRowsIndexConstant", 427.3440856933594D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(1)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new LinkedHashMap<String, Object>()
                    {{
                        put("doesn't exist", null);
                        put("rows", 26);
                        put("index", 12459.361190795898D);
                        put("addRowsIndexConstant", 12486.361190795898D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_9);
                      }}
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentDimensionAndActualFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters(QueryRunnerTestHelper.marketDimension, "upfront")
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new LinkedHashMap<String, Object>()
                    {{
                        put("doesn't exist", null);
                        put("rows", 4);
                        put("index", 4875.669677734375D);
                        put("addRowsIndexConstant", 4880.669677734375D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_2);
                      }}
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentDimensionAndNonExistentFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .filters("doesn't exist", null)
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(1)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new LinkedHashMap<String, Object>()
                    {{
                        put("doesn't exist", null);
                        put("rows", 26);
                        put("index", 12459.361190795898D);
                        put("addRowsIndexConstant", 12486.361190795898D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_9);
                      }}
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographic()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new LexicographicTopNMetricSpec(""))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new LexicographicTopNMetricSpec("spot"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new LexicographicTopNMetricSpec("t"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNInvertedLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("upfront")))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNInvertedLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("u")))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNDimExtractionToOne() throws IOException
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new JavaScriptExtractionFn("function(f) { return \"POTATO\"; }", false, JavaScriptConfig.getDefault()),
                null
            )
        )
        .metric("rows")
        .threshold(10)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    Granularity gran = QueryGranularities.DAY;
    TimeseriesQuery tsQuery = Druids.newTimeseriesQueryBuilder()
                                    .dataSource(dataSource)
                                    .granularity(gran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .aggregators(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.rowsCount,
                                            QueryRunnerTestHelper.indexDoubleSum,
                                            QueryRunnerTestHelper.qualityUniques
                                        )
                                    )
                                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                    .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 504542.5071372986D,
                        "index", 503332.5071372986D,
                        QueryRunnerTestHelper.marketDimension, "POTATO",
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 1209
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> list = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals("Didn't merge results", list.get(0).getValue().getValue().size(), 1);
    TestHelper.assertExpectedResults(expectedResults, list, "Failed to match");
  }

  @Test
  public void testTopNCollapsingDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.qualityDimension, QueryRunnerTestHelper.qualityDimension,
                new RegexDimExtractionFn(".(.)", false, null), null
            )
        )
        .metric("index")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.qualityDimension, "e",
                        "rows", 558,
                        "index", 246645.1204032898,
                        "addRowsIndexConstant", 247204.1204032898
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.qualityDimension, "r",
                        "rows", 372,
                        "index", 222051.08961486816,
                        "addRowsIndexConstant", 222424.08961486816
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);

    query = query.withAggregatorSpecs(
        Arrays.asList(
            QueryRunnerTestHelper.rowsCount,
            new DoubleSumAggregatorFactory("index", null, "-index + 100", null)
        )
    );

    expectedResults = Arrays.asList(
        TopNQueryRunnerTestHelper.createExpectedRows(
            "2011-01-12T00:00:00.000Z",
            new String[]{QueryRunnerTestHelper.qualityDimension, "rows", "index", "addRowsIndexConstant"},
            Arrays.asList(
                new Object[]{"n", 93, -2786.472755432129, -2692.472755432129},
                new Object[]{"u", 186, -3949.824363708496, -3762.824363708496}
            )
        )
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNDimExtractionFastTopNOptimalWithReplaceMissing()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot", "2spot0",
                            "total_market", "1total_market0",
                            "upfront", "3upfront0"
                        ),
                        false
                    ), false, "MISSING", true,
                    false
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNDimExtractionFastTopNUnOptimalWithReplaceMissing()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot", "2spot0",
                            "total_market", "1total_market0",
                            "upfront", "3upfront0"
                        ),
                        false
                    ), false, "MISSING", false,
                    false
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  // Test a "direct" query
  public void testTopNDimExtractionFastTopNOptimal()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot", "2spot0",
                            "total_market", "1total_market0",
                            "upfront", "3upfront0"
                        ),
                        false
                    ), true, null, true,
                    false
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  // Test query path that must rebucket the data
  public void testTopNDimExtractionFastTopNUnOptimal()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot",
                            "spot0",
                            "total_market",
                            "total_market0",
                            "upfront",
                            "upfront0"
                        ),
                        false
                    ), true, null, false,
                    false
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot0",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market0",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront0",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionOptimalNamespace()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot",
                            "2spot",
                            "total_market",
                            "3total_market",
                            "upfront",
                            "1upfront"
                        ),
                        false
                    ), true, null, true,
                    false
                ),
                null
            )
        )
        .metric(new LexicographicTopNMetricSpec(null))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionUnOptimalNamespace()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot",
                            "2spot",
                            "total_market",
                            "3total_market",
                            "upfront",
                            "1upfront"
                        ),
                        false
                    ), true, null, false,
                    false
                ),
                null
            )
        )
        .metric(new LexicographicTopNMetricSpec(null))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNLexicographicDimExtractionOptimalNamespaceWithRunner()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.<Object, String>of(
                            "spot",
                            "2spot",
                            "total_market",
                            "3total_market",
                            "upfront",
                            "1upfront"
                        ),
                        false
                    ), true, null, true,
                    false
                ),
                null
            )
        )
        .metric(new LexicographicTopNMetricSpec(null))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric(new LexicographicTopNMetricSpec(null))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testInvertedTopNLexicographicDimExtraction2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("..(.)", false, null),
                null
            )
        )
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec(null)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "o",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "f",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric(new LexicographicTopNMetricSpec("s"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionWithSortingPreservedAndPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension, QueryRunnerTestHelper.marketDimension,
                new ExtractionFn()
                {
                  @Override
                  public KeyBuilder getCacheKey(KeyBuilder builder)
                  {
                    return builder;
                  }

                  @Override
                  public String apply(String value)
                  {
                    return value.substring(0, 1);
                  }

                  @Override
                  public boolean preservesOrdering()
                  {
                    return true;
                  }
                }, null
            )
        )
        .metric(new LexicographicTopNMetricSpec("s"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testInvertedTopNLexicographicDimExtractionWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("u")))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testInvertedTopNLexicographicDimExtractionWithPreviousStop2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("..(.)", false, null),
                null
            )
        )
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("p")))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "o",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "f",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNullProducingDimExtractionFn()
  {
    final ExtractionFn nullStringDimExtraction = new ExtractionFn()
    {
      @Override
      public KeyBuilder getCacheKey(KeyBuilder builder)
      {
        return builder.append((byte) 0xFF);
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals("total_market") ? null : dimValue;
      }
    };

    final TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                nullStringDimExtraction,
                null
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    new LinkedHashMap<String, Object>()
                    {{
                        put(QueryRunnerTestHelper.marketDimension, null);
                        put("rows", 4);
                        put("index", 5351.814697265625D);
                        put("addRowsIndexConstant", 5356.814697265625D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_2);
                      }},
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);

  }

  /**
   * This test exists only to show what the current behavior is and not necessarily to define that this is
   * correct behavior.  In fact, the behavior when returning the empty string from a DimExtractionFn is, by
   * contract, undefined, so this can do anything.
   */
  @Test
  public void testTopNWithEmptyStringProducingDimExtractionFn()
  {
    final ExtractionFn emptyStringDimExtraction = new ExtractionFn()
    {
      @Override
      public KeyBuilder getCacheKey(KeyBuilder builder)
      {
        return builder.append((byte) 0xFF);
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals("total_market") ? "" : dimValue;
      }
    };

    final TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                emptyStringDimExtraction,
                null
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    new LinkedHashMap<String, Object>()
                    {{
                        put(QueryRunnerTestHelper.marketDimension, "");
                        put("rows", 4);
                        put("index", 5351.814697265625D);
                        put("addRowsIndexConstant", 5356.814697265625D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_2);
                      }},
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testInvertedTopNQuery()
  {
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(Granularities.ALL)
            .dimension(QueryRunnerTestHelper.marketDimension)
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec(QueryRunnerTestHelper.indexMetric)))
            .threshold(3)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(QueryRunnerTestHelper.commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNQueryByComplexMetric()
  {
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(Granularities.ALL)
            .dimension(QueryRunnerTestHelper.marketDimension)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(
                Lists.<AggregatorFactory>newArrayList(
                    new CardinalityAggregatorFactory(
                        "numVals", ImmutableList.of(QueryRunnerTestHelper.marketDimension), false
                    )
                )
            )
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "numVals", 1.0002442201269182d
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "numVals", 1.0002442201269182d
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "upfront",
                        "numVals", 1.0002442201269182d
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNQueryCardinalityAggregatorWithExtractionFn()
  {
    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getDefault());

    DimensionSpec dimSpec = new ExtractionDimensionSpec(QueryRunnerTestHelper.marketDimension,
                                                        QueryRunnerTestHelper.marketDimension,
                                                        helloFn);

    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(Granularities.ALL)
            .dimension(dimSpec)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(
                Lists.<AggregatorFactory>newArrayList(
                    new CardinalityAggregatorFactory(
                        "numVals",
                        null,
                        ImmutableList.<DimensionSpec>of(new ExtractionDimensionSpec(
                            QueryRunnerTestHelper.qualityDimension,
                            QueryRunnerTestHelper.qualityDimension,
                            helloFn
                        )),
                        null,
                        null,
                        false,
                        false,
                        0
                    )
                )
            )
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "hello",
                        "numVals", 1.0002442201269182d
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDependentPostAgg()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAgg
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 186)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
                                .put("rows", 837)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_9 + 1.0
                                )
                                .put("maxIndex", 277.2735290527344D)
                                .put("minIndex", 59.02102279663086D)
                                .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNBySegmentResults()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg
            )
        )
        .context(ImmutableMap.<String, Object>of("finalize", true, "bySegment", true))
        .build();
    TopNResultValue topNResult = new TopNResultValue(
        Arrays.<Map<String, Object>>asList(
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "total_market")
                        .put("rows", 186)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "upfront")
                        .put("rows", 186)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "spot")
                        .put("rows", 837)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
        )
    );

    List<Result<BySegmentTopNResultValue>> expectedResults = Collections.singletonList(
        new Result<BySegmentTopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new BySegmentTopNResultValue(
                Arrays.asList(
                    new Result<TopNResultValue>(
                        new DateTime("2011-01-12T00:00:00.000Z"),
                        topNResult
                    )
                ),
                QueryRunnerTestHelper.descriptor.getIdentifier(),
                new Interval("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z")
            )
        )
    );
    List<Result<TopNResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    for (Result<TopNResultValue> result : results) {
      Assert.assertEquals(result.getValue(), result.getValue()); // TODO: fix this test
    }
  }

  @Test
  public void testTopNWithTimeColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                QueryRunnerTestHelper.__timeLongSum
            )
        )
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("ntimestamps")
        .threshold(3)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows",
                        18,
                        "ntimestamps",
                        9.0,
                        "sumtime",
                        23429865600000L
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows",
                        4,
                        "ntimestamps",
                        2.0,
                        "sumtime",
                        5206636800000L
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "upfront",
                        "rows",
                        4,
                        "ntimestamps",
                        2.0,
                        "sumtime",
                        5206636800000L
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testNumericDimensionTopNWithNullPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryGranularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC_NAME))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.secondOnly)
        .aggregators(Lists.<AggregatorFactory>newArrayList(QueryRunnerTestHelper.rowsCount))
        .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 9
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 2
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTopNTimeExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null, null),
                null
            )
        )
        .metric("index")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "dayOfWeek", "Wednesday",
                        "rows", 182,
                        "index", 76010.28100585938,
                        "addRowsIndexConstant", 76193.28100585938
                    ),
                    ImmutableMap.<String, Object>of(
                        "dayOfWeek", "Thursday",
                        "rows", 182,
                        "index", 75203.26300811768,
                        "addRowsIndexConstant", 75386.26300811768
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverNullDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverNullDimensionWithFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension("null_column")
        .filters(
            new SelectorDimFilter("null_column", null, null)
        )
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverPartialNullDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryGranularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("partial_null_column", null);
    map.put("rows", 22);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map,
                    ImmutableMap.<String, Object>of(
                        "partial_null_column", "value",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverPartialNullDimensionWithFilterOnNullValue()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryGranularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .filters(new SelectorDimFilter("partial_null_column", null, null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("partial_null_column", null);
    map.put("rows", 22);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverPartialNullDimensionWithFilterOnNOTNullValue()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryGranularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .filters(new SelectorDimFilter("partial_null_column", "value", null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "partial_null_column", "value",
                        "rows", 4,
                        "index", 4875.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testAlphaNumericTopNWithNullPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryGranularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new AlphaNumericTopNMetricSpec(null))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.secondOnly)
        .aggregators(Lists.<AggregatorFactory>newArrayList(QueryRunnerTestHelper.rowsCount))
        .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 9
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 2
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTopNWithExtractionFilter()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("spot", "spot0");
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    TopNQuery query = new TopNQueryBuilder().dataSource(dataSource)
                                            .granularity(Granularities.ALL)
                                            .dimension(QueryRunnerTestHelper.marketDimension)
                                            .metric("rows")
                                            .threshold(3)
                                            .intervals(QueryRunnerTestHelper.firstToThird)
                                            .aggregators(QueryRunnerTestHelper.commonAggregators)
                                            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                            .filters(
                                                new SelectorDimFilter(
                                                    QueryRunnerTestHelper.marketDimension,
                                                    "spot0",
                                                    lookupExtractionFn
                                                )
                                            )
                                            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithExtractionFilterAndFilteredAggregatorCaseNoExistingValue()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("", "NULL");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
    DimFilter extractionFilter = new SelectorDimFilter("null_column", "NULL", lookupExtractionFn);
    TopNQueryBuilder topNQueryBuilder = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators, Lists.newArrayList(
                        new FilteredAggregatorFactory(
                            new DoubleMaxAggregatorFactory("maxIndex", "index"),
                            extractionFilter
                        ),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant));
    TopNQuery topNQueryWithNULLValueExtraction = topNQueryBuilder
        .filters(extractionFilter)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, topNQueryWithNULLValueExtraction);
  }

  @Test
  public void testTopNWithExtractionFilterNoExistingValue()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("","NULL");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    DimFilter extractionFilter = new SelectorDimFilter("null_column", "NULL", lookupExtractionFn);
    TopNQueryBuilder topNQueryBuilder = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.ALL)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(Lists.newArrayList(Iterables.concat(QueryRunnerTestHelper.commonAggregators, Lists.newArrayList(
            new FilteredAggregatorFactory(new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                          extractionFilter),
            //new DoubleMaxAggregatorFactory("maxIndex", "index"),
            new DoubleMinAggregatorFactory("minIndex", "index")))))
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant));
    TopNQuery topNQueryWithNULLValueExtraction = topNQueryBuilder
        .filters(extractionFilter)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, topNQueryWithNULLValueExtraction);
  }
}
