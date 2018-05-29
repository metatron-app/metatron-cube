/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby;

import com.google.common.base.Predicate;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularities;
import io.druid.js.JavaScriptConfig;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.PostAggregationsPostProcessor;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.ExpressionDimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.having.AndHavingSpec;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;
import io.druid.query.groupby.orderby.FlattenSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.PartitionExpression;
import io.druid.query.groupby.orderby.PivotColumnSpec;
import io.druid.query.groupby.orderby.PivotSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerTest extends GroupByQueryRunnerTestHelper
{
  private final QueryRunner<Row> runner;
  private QueryRunnerFactory<Row, Query<Row>> factory;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return GroupByQueryRunnerTestHelper.createRunners();
  }

  public GroupByQueryRunnerTest(QueryRunnerFactory<Row, Query<Row>> factory, QueryRunner<Row> runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  private QueryRunner<Row> toBrokerRunner(QueryRunner<Row> runner)
  {
    return QueryRunnerTestHelper.toBrokerRunner(runner, factory.getToolchest());
  }

  private QueryRunner<Row> toBrokerRunner(QueryRunner<Row> runner, int segmentCount, ExecutorService exec)
  {
    return QueryRunnerTestHelper.toBrokerRunner(runner, factory, segmentCount, exec);
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withOutputColumns(Arrays.asList("alias", "rows"));

    String[] columnNames = {"__time", "alias", "rows"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        new Object[]{"2011-04-01", "automotive", 1L},
        new Object[]{"2011-04-01", "business", 1L},
        new Object[]{"2011-04-01", "entertainment", 1L},
        new Object[]{"2011-04-01", "health", 1L},
        new Object[]{"2011-04-01", "mezzanine", 3L},
        new Object[]{"2011-04-01", "news", 1L},
        new Object[]{"2011-04-01", "premium", 3L},
        new Object[]{"2011-04-01", "technology", 1L},
        new Object[]{"2011-04-01", "travel", 1L},
        new Object[]{"2011-04-02", "automotive", 1L},
        new Object[]{"2011-04-02", "business", 1L},
        new Object[]{"2011-04-02", "entertainment", 1L},
        new Object[]{"2011-04-02", "health", 1L},
        new Object[]{"2011-04-02", "mezzanine", 3L},
        new Object[]{"2011-04-02", "news", 1L},
        new Object[]{"2011-04-02", "premium", 3L},
        new Object[]{"2011-04-02", "technology", 1L},
        new Object[]{"2011-04-02", "travel", 1L}
    );

    results = Sequences.toList(runner.run(query, Maps.<String, Object>newHashMap()), Lists.<Row>newArrayList());
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // add post processing
    QueryRunner<Row> broker = toBrokerRunner(runner);
    query = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(
            Query.POST_PROCESSING,
            new LimitingPostProcessor(
                LimitSpecs.of(10, OrderByColumnSpec.desc("alias")),
                Suppliers.ofInstance(new GroupByQueryConfig()))
        )
    );

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "travel", 1L),
        array("2011-04-01T00:00:00.000Z", "technology", 1L),
        array("2011-04-01T00:00:00.000Z", "premium", 3L),
        array("2011-04-01T00:00:00.000Z", "news", 1L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 3L),
        array("2011-04-01T00:00:00.000Z", "health", 1L),
        array("2011-04-01T00:00:00.000Z", "entertainment", 1L),
        array("2011-04-01T00:00:00.000Z", "business", 1L),
        array("2011-04-01T00:00:00.000Z", "automotive", 1L),
        array("2011-04-02T00:00:00.000Z", "travel", 1L)
    );

    results = Sequences.toList(broker.run(query, Maps.<String, Object>newHashMap()), Lists.<Row>newArrayList());
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // post-aggregations post processor
    query = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(
            Query.POST_PROCESSING,
            new PostAggregationsPostProcessor(
                Arrays.<PostAggregator>asList(
                    new MathPostAggregator("daily = time_format(__time,'MMM dd','UTC','en')")
                )
            )
        )
    );
    columnNames = new String[] {"__time", "alias", "rows", "daily"};
    query = query.withOutputColumns(Arrays.asList("alias", "rows", "daily"));

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "automotive", 1L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "business", 1L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "entertainment", 1L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "health", 1L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 3L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "news", 1L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "premium", 3L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "technology", 1L, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "travel", 1L, "Apr 01"),
        array("2011-04-02T00:00:00.000Z", "automotive", 1L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "business", 1L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "entertainment", 1L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "health", 1L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "mezzanine", 3L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "news", 1L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "premium", 3L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "technology", 1L, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "travel", 1L, "Apr 02")
    );

    results = Sequences.toList(broker.run(query, Maps.<String, Object>newHashMap()), Lists.<Row>newArrayList());
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByGroupingSet()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setGroupingSets(
            new GroupingSetSpec.Names.Builder().add("quality").add("alias").add("quality", "alias").build()
        )
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    String[] columnNames = {"__time", "quality", "alias", "rows", "idx"};
    List<Row> expectedResults = expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", null, "a", 2L, 282L),
        array("2011-04-01T00:00:00.000Z", null, "b", 2L, 230L),
        array("2011-04-01T00:00:00.000Z", null, "e", 2L, 324L),
        array("2011-04-01T00:00:00.000Z", null, "h", 2L, 233L),
        array("2011-04-01T00:00:00.000Z", null, "m", 6L, 5317L),
        array("2011-04-01T00:00:00.000Z", null, "n", 2L, 235L),
        array("2011-04-01T00:00:00.000Z", null, "p", 6L, 5405L),
        array("2011-04-01T00:00:00.000Z", null, "preferred", 26L, 12446L),
        array("2011-04-01T00:00:00.000Z", null, "t", 4L, 420L),
        array("2011-04-01T00:00:00.000Z", "automotive", null, 4L, 564L),
        array("2011-04-01T00:00:00.000Z", "automotive", "a", 2L, 282L),
        array("2011-04-01T00:00:00.000Z", "automotive", "preferred", 2L, 282L),
        array("2011-04-01T00:00:00.000Z", "business", null, 4L, 460L),
        array("2011-04-01T00:00:00.000Z", "business", "b", 2L, 230L),
        array("2011-04-01T00:00:00.000Z", "business", "preferred", 2L, 230L),
        array("2011-04-01T00:00:00.000Z", "entertainment", null, 4L, 648L),
        array("2011-04-01T00:00:00.000Z", "entertainment", "e", 2L, 324L),
        array("2011-04-01T00:00:00.000Z", "entertainment", "preferred", 2L, 324L),
        array("2011-04-01T00:00:00.000Z", "health", null, 4L, 466L),
        array("2011-04-01T00:00:00.000Z", "health", "h", 2L, 233L),
        array("2011-04-01T00:00:00.000Z", "health", "preferred", 2L, 233L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", null, 12L, 10634L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "m", 6L, 5317L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "preferred", 6L, 5317L),
        array("2011-04-01T00:00:00.000Z", "news", null, 4L, 470L),
        array("2011-04-01T00:00:00.000Z", "news", "n", 2L, 235L),
        array("2011-04-01T00:00:00.000Z", "news", "preferred", 2L, 235L),
        array("2011-04-01T00:00:00.000Z", "premium", null, 12L, 10810L),
        array("2011-04-01T00:00:00.000Z", "premium", "p", 6L, 5405L),
        array("2011-04-01T00:00:00.000Z", "premium", "preferred", 6L, 5405L),
        array("2011-04-01T00:00:00.000Z", "technology", null, 4L, 350L),
        array("2011-04-01T00:00:00.000Z", "technology", "preferred", 2L, 175L),
        array("2011-04-01T00:00:00.000Z", "technology", "t", 2L, 175L),
        array("2011-04-01T00:00:00.000Z", "travel", null, 4L, 490L),
        array("2011-04-01T00:00:00.000Z", "travel", "preferred", 2L, 245L),
        array("2011-04-01T00:00:00.000Z", "travel", "t", 2L, 245L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByGroupingSetRollup()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setGroupingSets(new GroupingSetSpec.Rollup())
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    String[] columnNames = {"__time", "quality", "alias", "rows", "idx"};
    List<Row> expectedResults = expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", null, null, 52L, 24892L),
        array("2011-04-01T00:00:00.000Z", "automotive", null, 4L, 564L),
        array("2011-04-01T00:00:00.000Z", "automotive", "a", 2L, 282L),
        array("2011-04-01T00:00:00.000Z", "automotive", "preferred", 2L, 282L),
        array("2011-04-01T00:00:00.000Z", "business", null, 4L, 460L),
        array("2011-04-01T00:00:00.000Z", "business", "b", 2L, 230L),
        array("2011-04-01T00:00:00.000Z", "business", "preferred", 2L, 230L),
        array("2011-04-01T00:00:00.000Z", "entertainment", null, 4L, 648L),
        array("2011-04-01T00:00:00.000Z", "entertainment", "e", 2L, 324L),
        array("2011-04-01T00:00:00.000Z", "entertainment", "preferred", 2L, 324L),
        array("2011-04-01T00:00:00.000Z", "health", null, 4L, 466L),
        array("2011-04-01T00:00:00.000Z", "health", "h", 2L, 233L),
        array("2011-04-01T00:00:00.000Z", "health", "preferred", 2L, 233L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", null, 12L, 10634L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "m", 6L, 5317L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "preferred", 6L, 5317L),
        array("2011-04-01T00:00:00.000Z", "news", null, 4L, 470L),
        array("2011-04-01T00:00:00.000Z", "news", "n", 2L, 235L),
        array("2011-04-01T00:00:00.000Z", "news", "preferred", 2L, 235L),
        array("2011-04-01T00:00:00.000Z", "premium", null, 12L, 10810L),
        array("2011-04-01T00:00:00.000Z", "premium", "p", 6L, 5405L),
        array("2011-04-01T00:00:00.000Z", "premium", "preferred", 6L, 5405L),
        array("2011-04-01T00:00:00.000Z", "technology", null, 4L, 350L),
        array("2011-04-01T00:00:00.000Z", "technology", "preferred", 2L, 175L),
        array("2011-04-01T00:00:00.000Z", "technology", "t", 2L, 175L),
        array("2011-04-01T00:00:00.000Z", "travel", null, 4L, 490L),
        array("2011-04-01T00:00:00.000Z", "travel", "preferred", 2L, 245L),
        array("2011-04-01T00:00:00.000Z", "travel", "t", 2L, 245L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByOnMetric()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(new Interval("2011-01-12/2011-01-13"))
        .setDimensions(DefaultDimensionSpec.toSpec("index"))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.rowsCount))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{Column.TIME_COLUMN_NAME, "index", "rows"},
        array("2011-01-12T00:00:00.000Z", 100.0d, 9L),
        array("2011-01-12T00:00:00.000Z", 800.0d, 2L),
        array("2011-01-12T00:00:00.000Z", 1000.0d, 2L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByOnTime()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(new Interval("2011-01-12/2011-01-14"))
        .setDimensions(new DefaultDimensionSpec(Column.TIME_COLUMN_NAME, "time"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{Column.TIME_COLUMN_NAME, "time", "rows"},
        array("2011-01-12T00:00:00.000Z", 1294790400000L, 13L),
        array("2011-01-12T00:00:00.000Z", 1294876800000L, 13L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByOnTimeVC()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(new Interval("2011-01-12/2011-01-31"))
        .setVirtualColumns(new ExprVirtualColumn("bucketStart(__time, 'WEEK')", "_week"))
        .setDimensions(DefaultDimensionSpec.toSpec("_week"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setPostAggregatorSpecs(new MathPostAggregator("week = time_format(_week, out.format='ww xxxx')"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{Column.TIME_COLUMN_NAME, "week", "rows"},
        array("2011-01-12T00:00:00.000Z", "02 2011", 65L),
        array("2011-01-12T00:00:00.000Z", "03 2011", 78L),
        array("2011-01-12T00:00:00.000Z", "04 2011", 91L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMultiValueDimension()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("placementish", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "b", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "e", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "h", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "m", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "n", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "p", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "preferred", "rows", 26L, "idx", 12446L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "t", "rows", 4L, "idx", 420L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValue1()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new DefaultDimensionSpec("placementish", "alias"),
            new DefaultDimensionSpec("quality", "quality")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "alias", "a", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "alias", "b", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "alias", "e", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "alias", "h", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "alias", "m", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "alias", "n", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "alias", "p", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "alias", "preferred", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "alias", "preferred", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "alias", "preferred", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "alias", "preferred", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "alias", "preferred", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "alias", "preferred", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "alias", "preferred", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "alias", "preferred", "rows", 2L, "idx", 175L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "alias", "preferred", "rows", 2L, "idx", 245L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "alias", "t", "rows", 2L, "idx", 175L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "alias", "t", "rows", 2L, "idx", 245L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValueDifferentOrder()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "alias", "a", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "alias", "preferred", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "alias", "b", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "alias", "preferred", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "alias", "e", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "alias", "preferred", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "alias", "h", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "alias", "preferred", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "alias", "m", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "alias", "preferred", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "alias", "n", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "alias", "preferred", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "alias", "p", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "alias", "preferred", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "alias", "preferred", "rows", 2L, "idx", 175L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "alias", "t", "rows", 2L, "idx", 175L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "alias", "preferred", "rows", 2L, "idx", 245L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "alias", "t", "rows", 2L, "idx", 245L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test(expected = ISE.class)
  public void testGroupByMaxRowsLimitContextOverrid()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.<String, Object>of("maxResults", 1))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testGroupByWithRebucketRename()
  {
    Map<Object, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissingNonInjective()
  {
    Map<Object, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, false, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissing()
  {
    Map<Object, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, true, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithSimpleRenameAndMissingString()
  {
    Map<Object, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(map, false), false, "MISSING", true, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithSimpleRename()
  {
    Map<Object, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, true, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithUniques()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithUniquesAndPostAggWithSameName()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new HyperUniquesAggregatorFactory(
                    "quality_uniques",
                    "quality_uniques"
                )
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "quality_uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithCardinality()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.qualityCardinality
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "cardinality",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01", "null", 22L, "not_null", 4L
        )
    );

    query = query
        .withVirtualColumns(
            Arrays.<VirtualColumn>asList(new ExprVirtualColumn("partial_null_column + ''", "PN"))
        ).withAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new CountAggregatorFactory("not_null", "!isnull(PN)"), new CountAggregatorFactory("null", "isnull(PN)"))
        );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    expectedResults = Arrays.asList(GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "rows", 4L));

    query = query
        .withVirtualColumns(Arrays.<VirtualColumn>asList(new ExprVirtualColumn("market + 'AA'", "marketAA")))
        .withDimFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    BoundDimFilter.lt("marketAA", "spotAA"),
                    BoundDimFilter.gt("marketAA", "total_marketAA")
                )
            )
        )
        .withAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.rowsCount)
        );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    for (Object x : results) {
      System.out.println(x);
    }
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithNullProducingDimExtractionFn()
  {
    final ExtractionFn nullExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals("mezzanine") ? null : super.apply(dimValue);
      }
    };
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec("quality", "alias", nullExtractionFn, null)
            )
        )
        .build();

    final String[] columns = new String[]{"__time", "alias", "rows", "idx"};
    final List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", null, 3L, 2870L),
        array("2011-04-01", "a", 1L, 135L),
        array("2011-04-01", "b", 1L, 118L),
        array("2011-04-01", "e", 1L, 158L),
        array("2011-04-01", "h", 1L, 120L),
        array("2011-04-01", "n", 1L, 121L),
        array("2011-04-01", "p", 3L, 2900L),
        array("2011-04-01", "t", 2L, 197L),

        array("2011-04-02", null, 3L, 2447L),
        array("2011-04-02", "a", 1L, 147L),
        array("2011-04-02", "b", 1L, 112L),
        array("2011-04-02", "e", 1L, 166L),
        array("2011-04-02", "h", 1L, 113L),
        array("2011-04-02", "n", 1L, 114L),
        array("2011-04-02", "p", 3L, 2505L),
        array("2011-04-02", "t", 2L, 223L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        ""
    );
  }

  @Test
  /**
   * This test exists only to show what the current behavior is and not necessarily to define that this is
   * correct behavior.  In fact, the behavior when returning the empty string from a DimExtractionFn is, by
   * contract, undefined, so this can do anything.
   */
  public void testGroupByWithEmptyStringProducingDimExtractionFn()
  {
    final ExtractionFn emptyStringExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals("mezzanine") ? "" : super.apply(dimValue);
      }
    };

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec("quality", "alias", emptyStringExtractionFn, null)
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "b", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "e", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "h", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "n", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "p", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "t", "rows", 2L, "idx", 197L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "b", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "e", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "h", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "n", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "p", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "t", "rows", 2L, "idx", 223L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        ""
    );
  }

  @Test
  public void testGroupByWithTimeZone()
  {
    DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");

    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
        .setDimensions(
            Lists.newArrayList(
                (DimensionSpec) new DefaultDimensionSpec(
                    "quality",
                    "alias"
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory(
                    "idx",
                    "index"
                )
            )
        )
        .setGranularity(
            new PeriodGranularity(
                new Period("P1D"),
                null,
                tz
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2870L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2900L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            78L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            97L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            126L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMergeResults()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery fullQuery = builder.build();
    final GroupByQuery allGranQuery = builder.setGranularity(QueryGranularities.ALL).build();

    QueryRunner mergedRunner = toBrokerRunner(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return new MergeSequence(
                query.getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(query1, responseContext), runner.run(query2, responseContext))
                )
            );
          }
        }
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(allGranExpectedResults, mergedRunner.run(allGranQuery, context), "merged");
  }

  @Test
  public void testMergeResultsWithLimit()
  {
    for (int limit = 1; limit < 20; ++limit) {
      doTestMergeResultsWithValidLimit(limit);
    }
  }

  private void doTestMergeResultsWithValidLimit(final int limit)
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(limit);

    final GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), runner.run(fullQuery, context), String.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderBy()
  {
    final int limit = 14;
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryGranularities.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", Direction.DESCENDING);

    GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), runner.run(fullQuery, context), String.format("limit: %d", limit)
    );

    builder.setAggregatorSpecs(
        Arrays.asList(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", null, "index * 2 + indexMin / 10", null)
        )
    );
    fullQuery = builder.build();

    expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 6090L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 6030L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 333L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 285L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 255L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 252L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 251L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 248L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 165L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 5262L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 5141L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 348L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 309L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 265L)
    );

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), runner.run(fullQuery, context), String.format("limit: %d", limit)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeResultsWithNegativeLimit()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(-1);

    builder.build();
  }

  @Test
  public void testMergeResultsWithOrderBy()
  {
    LimitSpec[] orderBySpecs = new LimitSpec[]{
        new LimitSpec(OrderByColumnSpec.ascending("idx"), null),
        new LimitSpec(OrderByColumnSpec.ascending("rows", "idx"), null),
        new LimitSpec(OrderByColumnSpec.descending("idx"), null),
        new LimitSpec(OrderByColumnSpec.descending("rows", "idx"), null),
    };

    final Comparator<Row> idxComparator =
        new Comparator<Row>()
        {
          @Override
          public int compare(Row o1, Row o2)
          {
            return Float.compare(o1.getFloatMetric("idx"), o2.getFloatMetric("idx"));
          }
        };

    Comparator<Row> rowsIdxComparator =
        new Comparator<Row>()
        {

          @Override
          public int compare(Row o1, Row o2)
          {
            int value = Float.compare(o1.getFloatMetric("rows"), o2.getFloatMetric("rows"));
            if (value != 0) {
              return value;
            }

            return idxComparator.compare(o1, o2);
          }
        };

    List<Row> allResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    List<List<Row>> expectedResults = Lists.newArrayList(
        Ordering.from(idxComparator).sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).sortedCopy(allResults),
        Ordering.from(idxComparator).reverse().sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).reverse().sortedCopy(allResults)
    );

    for (int i = 0; i < orderBySpecs.length; ++i) {
      doTestMergeResultsWithOrderBy(orderBySpecs[i], expectedResults.get(i));
    }
  }

  private void doTestMergeResultsWithOrderBy(LimitSpec orderBySpec, List<Row> expectedResults)
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimitSpec(orderBySpec);

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = toBrokerRunner(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return new MergeSequence(
                query.getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(query1, responseContext), runner.run(query2, responseContext))
                )
            );
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testGroupByOrderLimit() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("rows")
        .addOrderByColumn("alias", Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "no-limit");

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), runner.run(builder.limit(5).build(), context), "limited"
    );

    builder.setAggregatorSpecs(
        Arrays.asList(
            QueryRunnerTestHelper.rowsCount,
            new CountAggregatorFactory("rows1", "index > 110"),
            new CountAggregatorFactory("rows2", "index > 130"),
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("idx2", "index", null, "index > 110"),
            new DoubleSumAggregatorFactory("idx3", "index", null, "index > 130")
        )
    );

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "alias", "rows", "rows1", "rows2", "idx", "idx2", "idx3"},
        new Object[]{"2011-04-01", "travel", 2L, 2L, 0L, 243L, 243L, 0D},
        new Object[]{"2011-04-01", "technology", 2L, 0L, 0L, 177L, 0L, 0D},
        new Object[]{"2011-04-01", "news", 2L, 1L, 0L, 221L, 114L, 0D},
        new Object[]{"2011-04-01", "health", 2L, 1L, 0L, 216L, 113L, 0D},
        new Object[]{"2011-04-01", "entertainment", 2L, 2L, 2L, 319L, 319L, 319.94403076171875D},
        new Object[]{"2011-04-01", "business", 2L, 1L, 0L, 217L, 112L, 0D},
        new Object[]{"2011-04-01", "automotive", 2L, 2L, 1L, 269L, 269L, 147.42593383789062D},
        new Object[]{"2011-04-01", "premium", 6L, 6L, 5L, 4416L, 4416L, 4296.4765625D},
        new Object[]{"2011-04-01", "mezzanine", 6L, 5L, 4L, 4420L, 4313L, 4205.673828125D}
    );

    TestHelper.assertExpectedObjects(
        expectedResults, runner.run(builder.limit(100).build(), context), "predicate"
    );

    builder.limit(Integer.MAX_VALUE)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx", null, "index / 2 + indexMin", null)
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new MathPostAggregator(
                    "MMM-yyyy", "time_format(__time, out.format='MMM yyyy', out.timezone='UTC', out.locale='en')"
                )
            )
        );

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "alias", "rows", "idx", "MMM-yyyy"},
        new Object[]{"2011-04-01", "travel", 2L, 365.4876403808594D, "Apr 2011"},
        new Object[]{"2011-04-01", "technology", 2L, 267.3737487792969D, "Apr 2011"},
        new Object[]{"2011-04-01", "news", 2L, 333.3147277832031D, "Apr 2011"},
        new Object[]{"2011-04-01", "health", 2L, 325.467529296875D, "Apr 2011"},
        new Object[]{"2011-04-01", "entertainment", 2L, 479.916015625D, "Apr 2011"},
        new Object[]{"2011-04-01", "business", 2L, 328.083740234375D, "Apr 2011"},
        new Object[]{"2011-04-01", "automotive", 2L, 405.5966796875D, "Apr 2011"},
        new Object[]{"2011-04-01", "premium", 6L, 6627.927734375D, "Apr 2011"},
        new Object[]{"2011-04-01", "mezzanine", 6L, 6635.47998046875D, "Apr 2011"}
    );

    TestHelper.assertExpectedObjects(expectedResults, runner.run(builder.build(), context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), runner.run(builder.limit(5).build(), context), "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit2() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("rows", "desc")
        .addOrderByColumn("alias", "desc")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), runner.run(builder.limit(5).build(), context), "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit3() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("idx", "desc")
        .addOrderByColumn("alias", "desc")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    GroupByQuery query = builder.build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "alias", "rows", "idx"},
        new Object[]{"2011-04-01", "mezzanine", 6L, 4423.6533203125D},
        new Object[]{"2011-04-01", "premium", 6L, 4418.61865234375D},
        new Object[]{"2011-04-01", "entertainment", 2L, 319.94403076171875D},
        new Object[]{"2011-04-01", "automotive", 2L, 270.3977966308594D},
        new Object[]{"2011-04-01", "travel", 2L, 243.65843200683594D},
        new Object[]{"2011-04-01", "news", 2L, 222.20980834960938D},
        new Object[]{"2011-04-01", "business", 2L, 218.7224884033203D},
        new Object[]{"2011-04-01", "health", 2L, 216.97836303710938D},
        new Object[]{"2011-04-01", "technology", 2L, 178.24917602539062D}
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), runner.run(builder.limit(5).build(), context), "limited"
    );
  }

  @Test
  public void testGroupByWithSameCaseOrdering()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "upfront",
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "total_market",
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "spot",
            "rows",
            837L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderLimit4()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(DefaultDimensionSpec.of(QueryRunnerTestHelper.marketDimension))
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.marketDimension,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "market", "rows"},
        array("1970-01-01T00:00:00.000Z", "upfront", 186L),
        array("1970-01-01T00:00:00.000Z", "total_market", 186L),
        array("1970-01-01T00:00:00.000Z", "spot", 837L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");

    query = (GroupByQuery) query.withOverriddenContext(Query.GBY_PRE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(null, new QueryConfig(), null);
    DimensionSpec dimensionSpec = query.getDimensions().get(0);
    Assert.assertTrue(dimensionSpec instanceof DimensionSpecWithOrdering);
    Assert.assertTrue(query.getLimitSpec().getColumns().isEmpty());

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderOnHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    QueryRunnerTestHelper.marketDimension
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.uniqueMetric,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "upfront",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithHavingOnHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    QueryRunnerTestHelper.marketDimension
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.uniqueMetric,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setHavingSpec(
            new GreaterThanHavingSpec(
                QueryRunnerTestHelper.uniqueMetric,
                8
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                ),
                new MathPostAggregator(
                    "auto_finalized", QueryRunnerTestHelper.uniqueMetric + " + 100"
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            "auto_finalized",
            QueryRunnerTestHelper.UNIQUES_9 + 100
        )
    );

    // havingSpec equalTo/greaterThan/lessThan do not work on complex aggregators, even if they could be finalized.
    // See also: https://github.com/druid-io/druid/issues/2507
//    expectedException.expect(ParseException.class);
//    expectedException.expectMessage("Unknown type[class io.druid.query.aggregation.hyperloglog.HLLCV1]");
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithHavingOnFinalizedHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    QueryRunnerTestHelper.marketDimension
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setHavingSpec(
            new GreaterThanHavingSpec(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                8
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");

    query = query.withHavingSpec(
        new ExpressionHavingSpec(
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric + "> 8"
        )
    );
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), "order-limit"
    );
  }

  @Test
  public void testGroupByWithLimitOnFinalizedHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    QueryRunnerTestHelper.marketDimension
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            LimitSpecs.of(
                3,
                OrderByColumnSpec.desc(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric),
                OrderByColumnSpec.desc(QueryRunnerTestHelper.marketDimension)
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "upfront",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithAlphaNumericDimensionOrder()
  {
    Map<Object, String> map = new HashMap<>();
    map.put("automotive", "health105");
    map.put("business", "health20");
    map.put("entertainment", "travel47");
    map.put("health", "health55");
    map.put("mezzanine", "health09");
    map.put("news", "health0000");
    map.put("premium", "health999");
    map.put("technology", "travel123");
    map.put("travel", "travel555");

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setLimitSpec(new LimitSpec(Lists.<OrderByColumnSpec>newArrayList(
            new OrderByColumnSpec("alias", Direction.DESCENDING, StringComparators.ALPHANUMERIC_NAME)), null))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel555", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel123", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel47", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health999", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health105", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health55", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health20", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health09", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0000", "rows", 1L, "idx", 121L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel555", "rows", 1L, "idx", 126L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel123", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel47", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health999", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health105", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health55", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health20", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health09", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0000", "rows", 1L, "idx", 114L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = (GroupByQuery) query.withOverriddenContext(Query.GBY_PRE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(null, new QueryConfig(), null);
    DimensionSpec dimensionSpec = query.getDimensions().get(0);
    Assert.assertTrue(dimensionSpec instanceof DimensionSpecWithOrdering);
    Assert.assertTrue(query.getLimitSpec().getColumns().isEmpty());

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Ignore
  @Test
  // This is a test to verify per limit groupings, but Druid currently does not support this functionality. At a point
  // in time when Druid does support this, we can re-evaluate this test.
  public void testLimitPerGrouping()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    QueryRunnerTestHelper.marketDimension
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.firstToThird)
        // Using a limitSpec here to achieve a per group limit is incorrect.
        // Limit is applied on the overall results.
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "rows",
                        Direction.DESCENDING
                    )
                ), 2
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01T00:00:00.000Z", "market", "spot", "rows", 9L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02T00:00:00.000Z", "market", "spot", "rows", 9L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Iterator resultsIter = results.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    final Object next1 = resultsIter.next();
    Object expectedNext1 = expectedResultsIter.next();
    Assert.assertEquals("order-limit", expectedNext1, next1);

    final Object next2 = resultsIter.next();
    Object expectedNext2 = expectedResultsIter.next();
    Assert.assertNotEquals("order-limit", expectedNext2, next2);
  }

  @Test
  public void testPostAggMergedHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4420L + 1L)
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4416L + 1L)
        )
    );

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("index", "index")
            )
        )
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = toBrokerRunner(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return new MergeSequence(
                query.getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(query1, responseContext), runner.run(query2, responseContext))
                )
            );
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testGroupByWithOrderLimitHavingSpec()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-01-25/2011-01-28")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("index", "index")
            )
        )
        .setGranularity(QueryGranularities.ALL)
        .setHavingSpec(new GreaterThanHavingSpec("index", 310L))
        .setLimitSpec(LimitSpecs.of(5, OrderByColumnSpec.asc("index")));

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "business",
            "rows",
            3L,
            "index",
            312.38165283203125
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "news",
            "rows",
            3L,
            "index",
            312.7834167480469
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "technology",
            "rows",
            3L,
            "index",
            324.6412353515625
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "travel",
            "rows",
            3L,
            "index",
            393.36322021484375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "health",
            "rows",
            3L,
            "index",
            511.2996826171875
        )
    );

    GroupByQuery fullQuery = builder.build();
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery);
    TestHelper.assertExpectedObjects(
        expectedResults,
        results,
        ""
    );
  }

  @Test
  public void testPostAggHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4420L + 1L)
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4416L + 1L)
        )
    );

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("index", "index")
            )
        )
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000L)
                )
            )
        );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build()),
        ""
    );

    builder.setHavingSpec(new ExpressionHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric + "> 1000"));
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build()),
        ""
    );
  }


  @Test
  public void testHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        ""
    );

    builder.setHavingSpec(new ExpressionHavingSpec("rows > 2 || idx == 217"));
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build()),
        ""
    );
  }

  @Test
  public void testMergedHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = toBrokerRunner(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return new MergeSequence(
                query.getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(query1, responseContext), runner.run(query2, responseContext))
                )
            );
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testMergedPostAggHavingSpec()
  {
    String[] columns = new String[]{"__time", "alias", "rows", "idx", "rows_times_10"};
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        new Object[]{"2011-04-01", "business", 2L, 217L, 20.0},
        new Object[]{"2011-04-01", "mezzanine", 6L, 4420L, 60.0},
        new Object[]{"2011-04-01", "premium", 6L, 4416L, 60.0}
    );

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "rows_times_10",
                    "*",
                    Arrays.<PostAggregator>asList(
                        new FieldAccessPostAggregator(
                            "rows",
                            "rows"
                        ),
                        new ConstantPostAggregator(
                            "const",
                            10L
                        )
                    )
                )
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec("rows_times_10", 20L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    GroupByQuery fullQuery = builder.build();

    QueryRunner<Row> baseRunner = new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(
          Query<Row> query, Map<String, Object> responseContext
      )
      {
        // simulate two daily segments
        final Query query1 = query.withQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
        );
        final Query query2 = query.withQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
        );
        return new MergeSequence(
            query.getResultOrdering(),
            Sequences.simple(
                Arrays.asList(runner.run(query1, responseContext), runner.run(query2, responseContext))
            )
        );
      }
    };

    Map<String, Object> context = Maps.newHashMap();
    // add an extra layer of merging, simulate broker forwarding query to historical
    QueryRunner<Row> brokerRunner = toBrokerRunner(baseRunner);

    TestHelper.assertExpectedObjects(expectedResults, brokerRunner.run(fullQuery, context), "merged");

    fullQuery = fullQuery
        .withPostAggregatorSpecs(Arrays.<PostAggregator>asList(new MathPostAggregator("rows_times_10", "rows * 10.0")))
        .withHavingSpec(new ExpressionHavingSpec("rows_times_10 > 20 || idx == 217"));

    TestHelper.assertExpectedObjects(expectedResults, brokerRunner.run(fullQuery, context), "merged");

    fullQuery = fullQuery.withPostAggregatorSpecs(
        Arrays.<PostAggregator>asList(new MathPostAggregator("rows_times_10 = rows * 10.0"))
    );

    TestHelper.assertExpectedObjects(expectedResults, brokerRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testGroupByWithRegEx() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimFilter(new RegexDimFilter("quality", "auto.*", null))
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "quality")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "rows", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, toBrokerRunner(runner).run(query, null), "no-limit");
  }

  @Test
  public void testGroupByWithNonexistentDimension() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("billy")
        .addDimension("quality")
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            "quality",
            "automotive",
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "business", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            "quality",
            "entertainment",
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "health", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "mezzanine", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "news", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "premium", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            "quality",
            "technology",
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "travel", "rows", 2L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "normal");
    TestHelper.assertExpectedObjects(expectedResults, toBrokerRunner(runner).run(query, context), "no-limit");
  }

  // A subquery identical to the query should yield identical results
  @Test
  public void testIdenticalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getDefault()
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQuery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getDefault()
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    new Interval("2011-04-01T00:00:00.000Z/2011-04-01T23:58:00.000Z"),
                    new Interval("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        )
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithExtractionFnInOuterQuery()
  {
    //https://github.com/druid-io/druid/issues/2556

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getDefault()
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        )
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new ExtractionDimensionSpec(
                "alias",
                "alias",
                new RegexDimExtractionFn("(a).*", true, "a")
            )
            )
        )
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 13L, "idx", 6619L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 13L, "idx", 5827L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    for (Object x : results) {
      System.out.println(x);
    }
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 6L, "idx", 771L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 6L, "idx", 778L)
    );

    query = query.withDimFilter(
        DimFilters.or(
            new InDimFilter("alias", Arrays.asList("a", "b"), null),
            new MathExprFilter("idx > 100 && idx < 200"),
            new InDimFilter("alias", Arrays.asList("b", "c"), null)));
    TestHelper.assertExpectedObjects(
        expectedResults, GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), ""
    );
  }

  @Test
  public void testDifferentGroupingSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleMaxAggregatorFactory("idx", "idx"),
                new DoubleMaxAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "rows", "idx", "indexMaxPlusTen"},
        new Object[]{"2011-04-01", 9L, 2900.0, 2930.0},
        new Object[]{"2011-04-02", 9L, 2505.0, 2535.0}
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), ""
    );

    subquery = subquery.withAggregatorSpecs(
        Arrays.asList(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", null, "-index + 100", null),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
    );
    query = (GroupByQuery) query.withDataSource(new QueryDataSource(subquery));

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "rows", "idx", "indexMaxPlusTen"},
        new Object[]{"2011-04-01", 9L, 21.0, 2930.0},
        new Object[]{"2011-04-02", 9L, 2.0, 2535.0}
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), ""
    );
  }

  @Test
  public void testDifferentGroupingSubqueryMultipleAggregatorsOnSameField()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new ArithmeticPostAggregator(
                    "post_agg",
                    "+",
                    Lists.<PostAggregator>newArrayList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new FieldAccessPostAggregator("idx", "idx")
                    )
                )
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx1", "idx"),
                new DoubleMaxAggregatorFactory("idx2", "idx"),
                new DoubleMaxAggregatorFactory("idx3", "post_agg"),
                new DoubleMaxAggregatorFactory("idx4", "post_agg")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01", "idx1", 2900.0, "idx2", 2900.0,
            "idx3", 5800.0, "idx4", 5800.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02", "idx1", 2505.0, "idx2", 2505.0,
            "idx3", 5010.0, "idx4", 5010.0
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testDifferentGroupingSubqueryWithFilter()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "quality")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setDimFilter(
            new OrDimFilter(
                Lists.<DimFilter>newArrayList(
                    new SelectorDimFilter("quality", "automotive", null),
                    new SelectorDimFilter("quality", "premium", null),
                    new SelectorDimFilter("quality", "mezzanine", null),
                    new SelectorDimFilter("quality", "business", null),
                    new SelectorDimFilter("quality", "entertainment", null),
                    new SelectorDimFilter("quality", "health", null),
                    new SelectorDimFilter("quality", "news", null),
                    new SelectorDimFilter("quality", "technology", null),
                    new SelectorDimFilter("quality", "travel", null)
                )
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx", 2900.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testDifferentIntervalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.secondOnly)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testEmptySubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }

  @Test
  public void testSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(new Interval("2011-01-12/2011-02-13"))
        .setDimensions(ExpressionDimensionSpec.of("cast(index, 'long')", "index"))  // add cast to skip schema query
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.rowsCount))
        .setGranularity(Granularities.WEEK)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setInterval(new Interval("2011-01-12/2011-02-13"))
        .setDimensions(DefaultDimensionSpec.toSpec("rows"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("index", "index")
        )
        .setGranularity(Granularities.ALL)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{Column.TIME_COLUMN_NAME, "rows", "index"},
        array("2011-01-12T00:00:00.000Z", 1L, 120628L),
        array("2011-01-12T00:00:00.000Z", 2L, 2483L),
        array("2011-01-12T00:00:00.000Z", 3L, 1278L),
        array("2011-01-12T00:00:00.000Z", 4L, 808L),
        array("2011-01-12T00:00:00.000Z", 5L, 208L),
        array("2011-01-12T00:00:00.000Z", 6L, 627L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithPostAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(
            new JavaScriptDimFilter(
                "quality",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx_subagg", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11135.0,
            "idx",
            1135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11118.0,
            "idx",
            1118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx_post",
            13870.0,
            "idx",
            3870L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            3L,
            "idx_post",
            13900.0,
            "idx",
            3900L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11147.0,
            "idx",
            1147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11112.0,
            "idx",
            1112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11166.0,
            "idx",
            1166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11113.0,
            "idx",
            1113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx_post",
            13447.0,
            "idx",
            3447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11114.0,
            "idx",
            1114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx_post",
            13505.0,
            "idx",
            3505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11097.0,
            "idx",
            1097L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11126.0,
            "idx",
            1126L
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(
            new JavaScriptDimFilter(
                "quality",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx_subagg", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public Predicate<Row> toEvaluator(TypeResolver resolver, List<AggregatorFactory> aggregators)
              {
                return new Predicate<Row>()
                {
                  @Override
                  public boolean apply(Row input)
                  {
                    return (input.getFloatMetric("idx_subpostagg") < 3800);
                  }
                };
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11135.0,
            "idx",
            1135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11118.0,
            "idx",
            1118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11147.0,
            "idx",
            1147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11112.0,
            "idx",
            1112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11166.0,
            "idx",
            1166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11113.0,
            "idx",
            1113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx_post",
            13447.0,
            "idx",
            3447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11114.0,
            "idx",
            1114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx_post",
            13505.0,
            "idx",
            3505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11097.0,
            "idx",
            1097L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11126.0,
            "idx",
            1126L
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithMultiColumnAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(
            new JavaScriptDimFilter(
                "market",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx_subagg", "index"),
                new JavaScriptAggregatorFactory(
                    "js_agg",
                    Arrays.asList("index", "market"),
                    "function(current, index, dim){return current + index + dim.length;}",
                    "function(){return 0;}",
                    "function(a,b){return a + b;}",
                    JavaScriptConfig.getDefault()
                )
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public Predicate<Row> toEvaluator(TypeResolver resolver, List<AggregatorFactory> aggregators)
              {
                return new Predicate<Row>()
                {
                  @Override
                  public boolean apply(Row input)
                  {
                    return (input.getFloatMetric("idx_subpostagg") < 3800);
                  }
                };
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg"),
                new DoubleSumAggregatorFactory("js_outer_agg", "js_agg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setLimitSpec(
            new LimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec(
                        "alias",
                        Direction.DESCENDING
                    )
                ),
                5
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L,
            "js_outer_agg",
            123.92274475097656
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L,
            "js_outer_agg",
            82.62254333496094
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L,
            "js_outer_agg",
            125.58358001708984
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L,
            "js_outer_agg",
            124.13470458984375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L,
            "js_outer_agg",
            162.74722290039062
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory, runner, query.withHavingSpec(new ExpressionHavingSpec("idx_subpostagg < 3800.0"))
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithHyperUniques()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new HyperUniquesAggregatorFactory("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx"),
                new HyperUniquesAggregatorFactory("uniq", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            2L,
            "idx",
            282L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            230L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            324L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            233L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            5317L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            235L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            5405L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            175L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            245L,
            "uniq",
            1.0002442201269182
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithHyperUniquesPostAggregator()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList())
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new HyperUniquesAggregatorFactory("quality_uniques_inner", "quality_uniques")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("quality_uniques_inner_post", "quality_uniques_inner")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList())
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx"),
                new HyperUniquesAggregatorFactory("quality_uniques_outer", "quality_uniques_inner_post")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques_outer_post", "quality_uniques_outer")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "idx",
            12446L,
            "quality_uniques_outer",
            9.019833517963864,
            "quality_uniques_outer_post",
            9.019833517963864
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithTimeColumn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                QueryRunnerTestHelper.__timeLongSum
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "ntimestamps",
            13.0,
            "sumtime",
            33843139200000L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByTimeExtraction()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter("market", "spot", null),
                    new MathExprFilter("market == 'upfront'")
                )
            )
        )
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "dayOfWeek", "market", "index", "rows", "addRowsIndexConstant"},
        new Object[]{"1970-01-01", "Friday", "spot", 13219.574157714844, 117L, 13337.574157714844},
        new Object[]{"1970-01-01", "Monday", "spot", 13557.738830566406, 117L, 13675.738830566406},
        new Object[]{"1970-01-01", "Saturday", "spot", 13493.751281738281, 117L, 13611.751281738281},
        new Object[]{"1970-01-01", "Sunday", "spot", 13585.541015625, 117L, 13703.541015625},
        new Object[]{"1970-01-01", "Thursday", "spot", 14279.127197265625, 126L, 14406.127197265625},
        new Object[]{"1970-01-01", "Tuesday", "spot", 13199.471435546875, 117L, 13317.471435546875},
        new Object[]{"1970-01-01", "Wednesday", "spot", 14271.368591308594, 126L, 14398.368591308594},
        new Object[]{"1970-01-01", "Friday", "upfront", 27297.8623046875, 26L, 27324.8623046875},
        new Object[]{"1970-01-01", "Monday", "upfront", 27619.58447265625, 26L, 27646.58447265625},
        new Object[]{"1970-01-01", "Saturday", "upfront", 27820.83154296875, 26L, 27847.83154296875},
        new Object[]{"1970-01-01", "Sunday", "upfront", 24791.223876953125, 26L, 24818.223876953125},
        new Object[]{"1970-01-01", "Thursday", "upfront", 28562.748901367188, 28L, 28591.748901367188},
        new Object[]{"1970-01-01", "Tuesday", "upfront", 26968.280639648438, 26L, 26995.280639648438},
        new Object[]{"1970-01-01", "Wednesday", "upfront", 28985.5751953125, 28L, 29014.5751953125}
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testWindowingSpec()
  {
    List<String> dayOfWeek = Arrays.asList("dayOfWeek");

    OrderByColumnSpec dayOfWeekAsc = new OrderByColumnSpec("dayOfWeek", Direction.ASCENDING);
    OrderByColumnSpec marketDsc = new OrderByColumnSpec("market", Direction.DESCENDING);
    OrderByColumnSpec indexDsc = new OrderByColumnSpec(
        "index", Direction.DESCENDING, StringComparators.FLOATING_POINT_NAME
    );
    OrderByColumnSpec rowsAsc = new OrderByColumnSpec("rows", Direction.ASCENDING);
    List<OrderByColumnSpec> dayPlusMarket = Arrays.asList(dayOfWeekAsc, marketDsc);
    List<OrderByColumnSpec> dayPlusRows = Arrays.asList(dayOfWeekAsc, rowsAsc);

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(QueryGranularities.ALL)
        .setDimFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null),
                    new SelectorDimFilter("market", "total_market", null)
                )
            )
        )
        .setLimitSpec(
            new LimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec("dayOfWeek", Direction.ASCENDING, StringComparators.DAY_OF_WEEK_NAME),
                    new OrderByColumnSpec("rows", Direction.ASCENDING)
                ),
                30
            )
        );

    String[] columnNames = {"__time", "dayOfWeek", "market", "rows", "index", "addRowsIndexConstant"};
    Iterable<Row> results;

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        new Object[]{"1970-01-01", "Monday", "total_market", 26L, 30468.77734375, 30495.77734375},
        new Object[]{"1970-01-01", "Monday", "upfront", 26L, 27619.58447265625, 27646.58447265625},
        new Object[]{"1970-01-01", "Monday", "spot", 117L, 13557.738830566406, 13675.738830566406},
        new Object[]{"1970-01-01", "Tuesday", "upfront", 26L, 26968.280639648438, 26995.280639648438},
        new Object[]{"1970-01-01", "Tuesday", "total_market", 26L, 29676.578125, 29703.578125},
        new Object[]{"1970-01-01", "Tuesday", "spot", 117L, 13199.471435546875, 13317.471435546875},
        new Object[]{"1970-01-01", "Wednesday", "upfront", 28L, 28985.5751953125, 29014.5751953125},
        new Object[]{"1970-01-01", "Wednesday", "total_market", 28L, 32753.337890625, 32782.337890625},
        new Object[]{"1970-01-01", "Wednesday", "spot", 126L, 14271.368591308594, 14398.368591308594},
        new Object[]{"1970-01-01", "Thursday", "upfront", 28L, 28562.748901367188, 28591.748901367188},
        new Object[]{"1970-01-01", "Thursday", "total_market", 28L, 32361.38720703125, 32390.38720703125},
        new Object[]{"1970-01-01", "Thursday", "spot", 126L, 14279.127197265625, 14406.127197265625},
        new Object[]{"1970-01-01", "Friday", "total_market", 26L, 30173.691650390625, 30200.691650390625},
        new Object[]{"1970-01-01", "Friday", "upfront", 26L, 27297.8623046875, 27324.8623046875},
        new Object[]{"1970-01-01", "Friday", "spot", 117L, 13219.574157714844, 13337.574157714844},
        new Object[]{"1970-01-01", "Saturday", "total_market", 26L, 30940.971923828125, 30967.971923828125},
        new Object[]{"1970-01-01", "Saturday", "upfront", 26L, 27820.83154296875, 27847.83154296875},
        new Object[]{"1970-01-01", "Saturday", "spot", 117L, 13493.751281738281, 13611.751281738281},
        new Object[]{"1970-01-01", "Sunday", "total_market", 26L, 29305.086059570312, 29332.086059570312},
        new Object[]{"1970-01-01", "Sunday", "upfront", 26L, 24791.223876953125, 24818.223876953125},
        new Object[]{"1970-01-01", "Sunday", "spot", 117L, 13585.541015625, 13703.541015625}
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    dayOfWeek, dayPlusMarket, "delta_week = $delta(rows)", "sum_week = $sum(rows)"
                ),
                new WindowingSpec(
                    null, null,
                    "delta_all = $delta(rows)",
                    "sum_all = $sum(rows)",
                    "sum_post = $sum(addRowsIndexConstant)"
                )
            )
        )
    );

    columnNames = new String[]{
        "__time", "dayOfWeek", "rows", "delta_week", "sum_week", "delta_all", "sum_all", "sum_post"
    };
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("1970-01-01", "Friday", 26L, 0L, 26L, 0L, 26L, 27324.8623046875),
        array("1970-01-01", "Friday", 26L, 0L, 52L, 0L, 52L, 57525.553955078125),
        array("1970-01-01", "Friday", 117L, 91L, 169L, 91L, 169L, 70863.12811279297),
        array("1970-01-01", "Monday", 26L, 0L, 26L, -91L, 195L, 98509.71258544922),
        array("1970-01-01", "Monday", 26L, 0L, 52L, 0L, 221L, 129005.48992919922),
        array("1970-01-01", "Monday", 117L, 91L, 169L, 91L, 338L, 142681.22875976562),
        array("1970-01-01", "Saturday", 26L, 0L, 26L, -91L, 364L, 170529.06030273438),
        array("1970-01-01", "Saturday", 26L, 0L, 52L, 0L, 390L, 201497.0322265625),
        array("1970-01-01", "Saturday", 117L, 91L, 169L, 91L, 507L, 215108.78350830078),
        array("1970-01-01", "Sunday", 26L, 0L, 26L, -91L, 533L, 239927.0073852539),
        array("1970-01-01", "Sunday", 26L, 0L, 52L, 0L, 559L, 269259.0934448242),
        array("1970-01-01", "Sunday", 117L, 91L, 169L, 91L, 676L, 282962.6344604492),
        array("1970-01-01", "Thursday", 28L, 0L, 28L, -89L, 704L, 311554.3833618164),
        array("1970-01-01", "Thursday", 28L, 0L, 56L, 0L, 732L, 343944.77056884766),
        array("1970-01-01", "Thursday", 126L, 98L, 182L, 98L, 858L, 358350.8977661133),
        array("1970-01-01", "Tuesday", 26L, 0L, 26L, -100L, 884L, 385346.1784057617),
        array("1970-01-01", "Tuesday", 26L, 0L, 52L, 0L, 910L, 415049.7565307617),
        array("1970-01-01", "Tuesday", 117L, 91L, 169L, 91L, 1027L, 428367.2279663086),
        array("1970-01-01", "Wednesday", 28L, 0L, 28L, -89L, 1055L, 457381.8031616211),
        array("1970-01-01", "Wednesday", 28L, 0L, 56L, 0L, 1083L, 490164.1410522461),
        array("1970-01-01", "Wednesday", 126L, 98L, 182L, 98L, 1209L, 504562.5096435547)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    GroupByQuery query = builder.build();
    query = (GroupByQuery) query.withOverriddenContext(Query.GBY_PRE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(null, new QueryConfig(), null);
    DimensionSpec dimensionSpec = query.getDimensions().get(1);
    Assert.assertTrue(dimensionSpec instanceof DimensionSpecWithOrdering);  // 0 is basic ordering
    WindowingSpec windowingSpec = query.getLimitSpec().getWindowingSpecs().get(0);
    Assert.assertTrue(GuavaUtils.isNullOrEmpty(windowingSpec.getSortingColumns()));

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setGranularity(QueryGranularities.MONTH);
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    dayOfWeek, dayPlusMarket, "delta_week = $delta(rows)", "sum_week = $sum(rows)"
                ),
                new WindowingSpec(
                    null, null, "delta_all = $delta(rows)", "sum_all = $sum(rows)"
                )
            )
        )
    );

    columnNames = new String[]{"__time", "dayOfWeek", "rows", "delta_week", "sum_week", "delta_all", "sum_all"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-01-01T00:00:00.000Z", "Friday", 4L, 0L, 4L, 0L, 4L),
        array("2011-02-01T00:00:00.000Z", "Friday", 8L, 4L, 12L, 4L, 12L),
        array("2011-03-01T00:00:00.000Z", "Friday", 8L, 0L, 20L, 0L, 20L),
        array("2011-04-01T00:00:00.000Z", "Friday", 6L, -2L, 26L, -2L, 26L),
        array("2011-01-01T00:00:00.000Z", "Friday", 4L, -2L, 30L, -2L, 30L),
        array("2011-02-01T00:00:00.000Z", "Friday", 8L, 4L, 38L, 4L, 38L),
        array("2011-03-01T00:00:00.000Z", "Friday", 8L, 0L, 46L, 0L, 46L),
        array("2011-04-01T00:00:00.000Z", "Friday", 6L, -2L, 52L, -2L, 52L),
        array("2011-01-01T00:00:00.000Z", "Friday", 18L, 12L, 70L, 12L, 70L),
        array("2011-02-01T00:00:00.000Z", "Friday", 36L, 18L, 106L, 18L, 106L),
        array("2011-03-01T00:00:00.000Z", "Friday", 36L, 0L, 142L, 0L, 142L),
        array("2011-04-01T00:00:00.000Z", "Friday", 27L, -9L, 169L, -9L, 169L),
        array("2011-01-01T00:00:00.000Z", "Monday", 6L, 0L, 6L, -21L, 175L),
        array("2011-02-01T00:00:00.000Z", "Monday", 8L, 2L, 14L, 2L, 183L),
        array("2011-03-01T00:00:00.000Z", "Monday", 8L, 0L, 22L, 0L, 191L),
        array("2011-04-01T00:00:00.000Z", "Monday", 4L, -4L, 26L, -4L, 195L),
        array("2011-01-01T00:00:00.000Z", "Monday", 6L, 2L, 32L, 2L, 201L),
        array("2011-02-01T00:00:00.000Z", "Monday", 8L, 2L, 40L, 2L, 209L),
        array("2011-03-01T00:00:00.000Z", "Monday", 8L, 0L, 48L, 0L, 217L),
        array("2011-04-01T00:00:00.000Z", "Monday", 4L, -4L, 52L, -4L, 221L),
        array("2011-01-01T00:00:00.000Z", "Monday", 27L, 23L, 79L, 23L, 248L),
        array("2011-02-01T00:00:00.000Z", "Monday", 36L, 9L, 115L, 9L, 284L),
        array("2011-03-01T00:00:00.000Z", "Monday", 36L, 0L, 151L, 0L, 320L),
        array("2011-04-01T00:00:00.000Z", "Monday", 18L, -18L, 169L, -18L, 338L),
        array("2011-01-01T00:00:00.000Z", "Saturday", 6L, 0L, 6L, -12L, 344L),
        array("2011-02-01T00:00:00.000Z", "Saturday", 8L, 2L, 14L, 2L, 352L),
        array("2011-03-01T00:00:00.000Z", "Saturday", 8L, 0L, 22L, 0L, 360L),
        array("2011-04-01T00:00:00.000Z", "Saturday", 4L, -4L, 26L, -4L, 364L),
        array("2011-01-01T00:00:00.000Z", "Saturday", 6L, 2L, 32L, 2L, 370L),
        array("2011-02-01T00:00:00.000Z", "Saturday", 8L, 2L, 40L, 2L, 378L),
        array("2011-03-01T00:00:00.000Z", "Saturday", 8L, 0L, 48L, 0L, 386L),
        array("2011-04-01T00:00:00.000Z", "Saturday", 4L, -4L, 52L, -4L, 390L),
        array("2011-01-01T00:00:00.000Z", "Saturday", 27L, 23L, 79L, 23L, 417L),
        array("2011-02-01T00:00:00.000Z", "Saturday", 36L, 9L, 115L, 9L, 453L),
        array("2011-03-01T00:00:00.000Z", "Saturday", 36L, 0L, 151L, 0L, 489L),
        array("2011-04-01T00:00:00.000Z", "Saturday", 18L, -18L, 169L, -18L, 507L),
        array("2011-01-01T00:00:00.000Z", "Sunday", 6L, 0L, 6L, -12L, 513L),
        array("2011-02-01T00:00:00.000Z", "Sunday", 8L, 2L, 14L, 2L, 521L),
        array("2011-03-01T00:00:00.000Z", "Sunday", 8L, 0L, 22L, 0L, 529L),
        array("2011-04-01T00:00:00.000Z", "Sunday", 4L, -4L, 26L, -4L, 533L),
        array("2011-01-01T00:00:00.000Z", "Sunday", 6L, 2L, 32L, 2L, 539L),
        array("2011-02-01T00:00:00.000Z", "Sunday", 8L, 2L, 40L, 2L, 547L),
        array("2011-03-01T00:00:00.000Z", "Sunday", 8L, 0L, 48L, 0L, 555L),
        array("2011-04-01T00:00:00.000Z", "Sunday", 4L, -4L, 52L, -4L, 559L),
        array("2011-01-01T00:00:00.000Z", "Sunday", 27L, 23L, 79L, 23L, 586L),
        array("2011-02-01T00:00:00.000Z", "Sunday", 36L, 9L, 115L, 9L, 622L),
        array("2011-03-01T00:00:00.000Z", "Sunday", 36L, 0L, 151L, 0L, 658L),
        array("2011-04-01T00:00:00.000Z", "Sunday", 18L, -18L, 169L, -18L, 676L),
        array("2011-01-01T00:00:00.000Z", "Thursday", 6L, 0L, 6L, -12L, 682L),
        array("2011-02-01T00:00:00.000Z", "Thursday", 8L, 2L, 14L, 2L, 690L),
        array("2011-03-01T00:00:00.000Z", "Thursday", 10L, 2L, 24L, 2L, 700L),
        array("2011-04-01T00:00:00.000Z", "Thursday", 4L, -6L, 28L, -6L, 704L),
        array("2011-01-01T00:00:00.000Z", "Thursday", 6L, 2L, 34L, 2L, 710L),
        array("2011-02-01T00:00:00.000Z", "Thursday", 8L, 2L, 42L, 2L, 718L),
        array("2011-03-01T00:00:00.000Z", "Thursday", 10L, 2L, 52L, 2L, 728L),
        array("2011-04-01T00:00:00.000Z", "Thursday", 4L, -6L, 56L, -6L, 732L),
        array("2011-01-01T00:00:00.000Z", "Thursday", 27L, 23L, 83L, 23L, 759L),
        array("2011-02-01T00:00:00.000Z", "Thursday", 36L, 9L, 119L, 9L, 795L),
        array("2011-03-01T00:00:00.000Z", "Thursday", 45L, 9L, 164L, 9L, 840L),
        array("2011-04-01T00:00:00.000Z", "Thursday", 18L, -27L, 182L, -27L, 858L),
        array("2011-01-01T00:00:00.000Z", "Tuesday", 4L, 0L, 4L, -14L, 862L),
        array("2011-02-01T00:00:00.000Z", "Tuesday", 8L, 4L, 12L, 4L, 870L),
        array("2011-03-01T00:00:00.000Z", "Tuesday", 10L, 2L, 22L, 2L, 880L),
        array("2011-04-01T00:00:00.000Z", "Tuesday", 4L, -6L, 26L, -6L, 884L),
        array("2011-01-01T00:00:00.000Z", "Tuesday", 4L, 0L, 30L, 0L, 888L),
        array("2011-02-01T00:00:00.000Z", "Tuesday", 8L, 4L, 38L, 4L, 896L),
        array("2011-03-01T00:00:00.000Z", "Tuesday", 10L, 2L, 48L, 2L, 906L),
        array("2011-04-01T00:00:00.000Z", "Tuesday", 4L, -6L, 52L, -6L, 910L),
        array("2011-01-01T00:00:00.000Z", "Tuesday", 18L, 14L, 70L, 14L, 928L),
        array("2011-02-01T00:00:00.000Z", "Tuesday", 36L, 18L, 106L, 18L, 964L),
        array("2011-03-01T00:00:00.000Z", "Tuesday", 45L, 9L, 151L, 9L, 1009L),
        array("2011-04-01T00:00:00.000Z", "Tuesday", 18L, -27L, 169L, -27L, 1027L),
        array("2011-01-01T00:00:00.000Z", "Wednesday", 6L, 0L, 6L, -12L, 1033L),
        array("2011-02-01T00:00:00.000Z", "Wednesday", 8L, 2L, 14L, 2L, 1041L),
        array("2011-03-01T00:00:00.000Z", "Wednesday", 10L, 2L, 24L, 2L, 1051L),
        array("2011-04-01T00:00:00.000Z", "Wednesday", 4L, -6L, 28L, -6L, 1055L),
        array("2011-01-01T00:00:00.000Z", "Wednesday", 6L, 2L, 34L, 2L, 1061L),
        array("2011-02-01T00:00:00.000Z", "Wednesday", 8L, 2L, 42L, 2L, 1069L),
        array("2011-03-01T00:00:00.000Z", "Wednesday", 10L, 2L, 52L, 2L, 1079L),
        array("2011-04-01T00:00:00.000Z", "Wednesday", 4L, -6L, 56L, -6L, 1083L),
        array("2011-01-01T00:00:00.000Z", "Wednesday", 27L, 23L, 83L, 23L, 1110L),
        array("2011-02-01T00:00:00.000Z", "Wednesday", 36L, 9L, 119L, 9L, 1146L),
        array("2011-03-01T00:00:00.000Z", "Wednesday", 45L, 9L, 164L, 9L, 1191L),
        array("2011-04-01T00:00:00.000Z", "Wednesday", 18L, -27L, 182L, -27L, 1209L)
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setGranularity(QueryGranularities.ALL);
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    dayOfWeek, dayPlusMarket, "delta_week = $delta(rows)", "sum_week = $sum(rows)"
                ),
                new WindowingSpec(
                    null, null, "delta_all = $delta(rows)", "sum_all = $sum(rows)"
                ),
                new WindowingSpec(
                    null, Arrays.asList(new OrderByColumnSpec("sum_all", Direction.DESCENDING))
                )
            )
        )
    );

    columnNames = new String[]{"__time", "dayOfWeek", "rows", "delta_week", "sum_week", "delta_all", "sum_all"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("1970-01-01T00:00:00.000Z", "Wednesday", 126L, 98L, 182L, 98L, 1209L),
        array("1970-01-01T00:00:00.000Z", "Wednesday", 28L, 0L, 56L, 0L, 1083L),
        array("1970-01-01T00:00:00.000Z", "Wednesday", 28L, 0L, 28L, -89L, 1055L),
        array("1970-01-01T00:00:00.000Z", "Tuesday", 117L, 91L, 169L, 91L, 1027L),
        array("1970-01-01T00:00:00.000Z", "Tuesday", 26L, 0L, 52L, 0L, 910L),
        array("1970-01-01T00:00:00.000Z", "Tuesday", 26L, 0L, 26L, -100L, 884L),
        array("1970-01-01T00:00:00.000Z", "Thursday", 126L, 98L, 182L, 98L, 858L),
        array("1970-01-01T00:00:00.000Z", "Thursday", 28L, 0L, 56L, 0L, 732L),
        array("1970-01-01T00:00:00.000Z", "Thursday", 28L, 0L, 28L, -89L, 704L),
        array("1970-01-01T00:00:00.000Z", "Sunday", 117L, 91L, 169L, 91L, 676L),
        array("1970-01-01T00:00:00.000Z", "Sunday", 26L, 0L, 52L, 0L, 559L),
        array("1970-01-01T00:00:00.000Z", "Sunday", 26L, 0L, 26L, -91L, 533L),
        array("1970-01-01T00:00:00.000Z", "Saturday", 117L, 91L, 169L, 91L, 507L),
        array("1970-01-01T00:00:00.000Z", "Saturday", 26L, 0L, 52L, 0L, 390L),
        array("1970-01-01T00:00:00.000Z", "Saturday", 26L, 0L, 26L, -91L, 364L),
        array("1970-01-01T00:00:00.000Z", "Monday", 117L, 91L, 169L, 91L, 338L),
        array("1970-01-01T00:00:00.000Z", "Monday", 26L, 0L, 52L, 0L, 221L),
        array("1970-01-01T00:00:00.000Z", "Monday", 26L, 0L, 26L, -91L, 195L),
        array("1970-01-01T00:00:00.000Z", "Friday", 117L, 91L, 169L, 91L, 169L),
        array("1970-01-01T00:00:00.000Z", "Friday", 26L, 0L, 52L, 0L, 52L),
        array("1970-01-01T00:00:00.000Z", "Friday", 26L, 0L, 26L, 0L, 26L)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "min_all = $min(index)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "min_week = $min(index)")
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "market", "index", "min_week", "min_all"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", "upfront", 27297.8623046875, 27297.8623046875, 27297.8623046875),
        array("Friday", "total_market", 30173.691650390625, 27297.8623046875, 27297.8623046875),
        array("Friday", "spot", 13219.574157714844, 13219.574157714844, 13219.574157714844),
        array("Monday", "upfront", 27619.58447265625, 27619.58447265625, 13219.574157714844),
        array("Monday", "total_market", 30468.77734375, 27619.58447265625, 13219.574157714844),
        array("Monday", "spot", 13557.738830566406, 13557.738830566406, 13219.574157714844),
        array("Saturday", "upfront", 27820.83154296875, 27820.83154296875, 13219.574157714844),
        array("Saturday", "total_market", 30940.971923828125, 27820.83154296875, 13219.574157714844),
        array("Saturday", "spot", 13493.751281738281, 13493.751281738281, 13219.574157714844),
        array("Sunday", "upfront", 24791.223876953125, 24791.223876953125, 13219.574157714844),
        array("Sunday", "total_market", 29305.086059570312, 24791.223876953125, 13219.574157714844),
        array("Sunday", "spot", 13585.541015625, 13585.541015625, 13219.574157714844),
        array("Thursday", "upfront", 28562.748901367188, 28562.748901367188, 13219.574157714844),
        array("Thursday", "total_market", 32361.38720703125, 28562.748901367188, 13219.574157714844),
        array("Thursday", "spot", 14279.127197265625, 14279.127197265625, 13219.574157714844),
        array("Tuesday", "upfront", 26968.280639648438, 26968.280639648438, 13219.574157714844),
        array("Tuesday", "total_market", 29676.578125, 26968.280639648438, 13219.574157714844),
        array("Tuesday", "spot", 13199.471435546875, 13199.471435546875, 13199.471435546875),
        array("Wednesday", "upfront", 28985.5751953125, 28985.5751953125, 13199.471435546875),
        array("Wednesday", "total_market", 32753.337890625, 28985.5751953125, 13199.471435546875),
        array("Wednesday", "spot", 14271.368591308594, 14271.368591308594, 13199.471435546875)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // don't know what the fuck is irr
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "irr_all = $irr(index)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "irr_week = $irr(index)")
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "market", "index", "irr_all", "irr_week"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", "upfront", 27297.8623046875, null, null),
        array("Friday", "total_market", 30173.691650390625, null, null),
        array("Friday", "spot", 13219.574157714844, null, Double.NaN),
        array("Monday", "upfront", 27619.58447265625, null, null),
        array("Monday", "total_market", 30468.77734375, null, null),
        array("Monday", "spot", 13557.738830566406, null, Double.NaN),
        array("Saturday", "upfront", 27820.83154296875, null, null),
        array("Saturday", "total_market", 30940.971923828125, null, null),
        array("Saturday", "spot", 13493.751281738281, null, Double.NaN),
        array("Sunday", "upfront", 24791.223876953125, null, null),
        array("Sunday", "total_market", 29305.086059570312, null, null),
        array("Sunday", "spot", 13585.541015625, null, Double.NaN),
        array("Thursday", "upfront", 28562.748901367188, null, null),
        array("Thursday", "total_market", 32361.38720703125, null, null),
        array("Thursday", "spot", 14279.127197265625, null, Double.NaN),
        array("Tuesday", "upfront", 26968.280639648438, null, null),
        array("Tuesday", "total_market", 29676.578125, null, null),
        array("Tuesday", "spot", 13199.471435546875, null, Double.NaN),
        array("Wednesday", "upfront", 28985.5751953125, null, null),
        array("Wednesday", "total_market", 32753.337890625, null, null),
        array("Wednesday", "spot", 14271.368591308594, Double.NaN, Double.NaN)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // don't know what the fuck is npv
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "npv_all = $npv(index, 0.1)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "npv_week = $npv(index, 0.1)")
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "market", "index", "npv_all", "npv_week"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", "upfront", 27297.8623046875, null, null),
        array("Friday", "total_market", 30173.691650390625, null, null),
        array("Friday", "spot", 13219.574157714844, null, 59685.2354333707),
        array("Monday", "upfront", 27619.58447265625, null, null),
        array("Monday", "total_market", 30468.77734375, null, null),
        array("Monday", "spot", 13557.738830566406, null, 60475.65072923024),
        array("Saturday", "upfront", 27820.83154296875, null, null),
        array("Saturday", "total_market", 30940.971923828125, null, null),
        array("Saturday", "spot", 13493.751281738281, null, 61000.77127343455),
        array("Sunday", "upfront", 24791.223876953125, null, null),
        array("Sunday", "total_market", 29305.086059570312, null, null),
        array("Sunday", "spot", 13585.541015625, null, 56963.573683144714),
        array("Thursday", "upfront", 28562.748901367188, null, null),
        array("Thursday", "total_market", 32361.38720703125, null, null),
        array("Thursday", "spot", 14279.127197265625, null, 63439.20307712568),
        array("Tuesday", "upfront", 26968.280639648438, null, null),
        array("Tuesday", "total_market", 29676.578125, null, null),
        array("Tuesday", "spot", 13199.471435546875, null, 58959.67464088766),
        array("Wednesday", "upfront", 28985.5751953125, null, null),
        array("Wednesday", "total_market", 32753.337890625, null, null),
        array("Wednesday", "spot", 14271.368591308594, 209577.55676702075, 64141.68764637431)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "min_all = $min(index)"),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(marketDsc), Arrays.asList("min_week = $min(index)"),
                    FlattenSpec.array(Arrays.asList("market", "index", "min_week", "min_all"), null)
                               .withExpression(
                                   "min_all[upfront]=min_all[market.upfront]",
                                   "min_week[spot]=min_week[market.spot]"
                               )
                )
            )
        )
    );

    columnNames = new String[] {"dayOfWeek", "market", "index", "min_week", "min_all", "min_all[upfront]", "min_week[spot]"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(27297.8623046875, 30173.691650390625, 13219.574157714844),
            Arrays.asList(27297.8623046875, 27297.8623046875, 13219.574157714844),
            Arrays.asList(27297.8623046875, 27297.8623046875, 13219.574157714844),
            27297.8623046875, 13219.574157714844),
        array("Monday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(27619.58447265625, 30468.77734375, 13557.738830566406),
            Arrays.asList(27619.58447265625, 27619.58447265625, 13557.738830566406),
            Arrays.asList(13219.574157714844, 13219.574157714844, 13219.574157714844),
            13219.574157714844, 13557.738830566406),
        array("Saturday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(27820.83154296875, 30940.971923828125, 13493.751281738281),
            Arrays.asList(27820.83154296875, 27820.83154296875, 13493.751281738281),
            Arrays.asList(13219.574157714844, 13219.574157714844, 13219.574157714844),
            13219.574157714844, 13493.751281738281),
        array("Sunday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(24791.223876953125, 29305.086059570312, 13585.541015625),
            Arrays.asList(24791.223876953125, 24791.223876953125, 13585.541015625),
            Arrays.asList(13219.574157714844, 13219.574157714844, 13219.574157714844),
            13219.574157714844, 13585.541015625),
        array("Thursday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(28562.748901367188, 32361.38720703125, 14279.127197265625),
            Arrays.asList(28562.748901367188, 28562.748901367188, 14279.127197265625),
            Arrays.asList(13219.574157714844, 13219.574157714844, 13219.574157714844),
            13219.574157714844, 14279.127197265625),
        array("Tuesday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(26968.280639648438, 29676.578125, 13199.471435546875),
            Arrays.asList(26968.280639648438, 26968.280639648438, 13199.471435546875),
            Arrays.asList(13219.574157714844, 13219.574157714844, 13199.471435546875),
            13219.574157714844, 13199.471435546875),
        array("Wednesday",
            Arrays.asList("upfront", "total_market", "spot"),
            Arrays.asList(28985.5751953125, 32753.337890625, 14271.368591308594),
            Arrays.asList(28985.5751953125, 28985.5751953125, 14271.368591308594),
            Arrays.asList(13199.471435546875, 13199.471435546875, 13199.471435546875),
            13199.471435546875, 14271.368591308594)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // order by on partition sum.. NMC requirement
    // changed identifier spec to accept index. use '_' for minus instead of '-'
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new OrderByColumnSpec(
                    "sum_week_last",
                    Direction.DESCENDING,
                    StringComparators.FLOATING_POINT_NAME
                )
            ),
            null,
            Arrays.asList(
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(indexDsc), Arrays.asList("sum_week = $sum(index)"),
                    FlattenSpec.array(Arrays.asList("market", "index", "sum_week"), null)
                               .withExpression(
                                   "sum_week_first=sum_week[0]", "sum_week_last=sum_week[_1]"
                               )
                )
            )
        )
    );

    columnNames = new String[] {"dayOfWeek", "market", "index", "sum_week", "sum_week_first", "sum_week_last"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Wednesday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(32753.337890625, 28985.5751953125, 14271.368591308594),
              Arrays.asList(32753.337890625, 61738.9130859375, 76010.2816772461),
              32753.337890625, 76010.2816772461),
        array("Thursday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(32361.38720703125, 28562.748901367188, 14279.127197265625),
              Arrays.asList(32361.38720703125, 60924.13610839844, 75203.26330566406),
              32361.38720703125, 75203.26330566406),
        array("Saturday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(30940.971923828125, 27820.83154296875, 13493.751281738281),
              Arrays.asList(30940.971923828125, 58761.803466796875, 72255.55474853516),
              30940.971923828125, 72255.55474853516),
        array("Monday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(30468.77734375, 27619.58447265625, 13557.738830566406),
              Arrays.asList(30468.77734375, 58088.36181640625, 71646.10064697266),
              30468.77734375, 71646.10064697266),
        array("Friday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(30173.691650390625, 27297.8623046875, 13219.574157714844),
              Arrays.asList(30173.691650390625, 57471.553955078125, 70691.12811279297),
              30173.691650390625, 70691.12811279297),
        array("Tuesday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(29676.578125, 26968.280639648438, 13199.471435546875),
              Arrays.asList(29676.578125, 56644.85876464844, 69844.33020019531),
              29676.578125, 69844.33020019531),
        array("Sunday",
              Arrays.asList("total_market", "upfront", "spot"),
              Arrays.asList(29305.086059570312, 24791.223876953125, 13585.541015625),
              Arrays.asList(29305.086059570312, 54096.30993652344, 67681.85095214844),
              29305.086059570312, 67681.85095214844)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // unstack, {d, m} + {}
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusMarket, Arrays.asList("min_all = $min(index)"),
                    FlattenSpec.array(
                        Arrays.asList("dayOfWeek", "market"), null, Arrays.asList("index", "min_all"), "-"
                    )
                )
            )
        )
    );

    columnNames = new String[] {"rows", "columns"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            Arrays.asList(
                "Friday-upfront", "Friday-total_market", "Friday-spot",
                "Monday-upfront", "Monday-total_market", "Monday-spot",
                "Saturday-upfront", "Saturday-total_market", "Saturday-spot",
                "Sunday-upfront", "Sunday-total_market", "Sunday-spot",
                "Thursday-upfront", "Thursday-total_market", "Thursday-spot",
                "Tuesday-upfront", "Tuesday-total_market", "Tuesday-spot",
                "Wednesday-upfront", "Wednesday-total_market", "Wednesday-spot"
            ),
            ImmutableMap.of(
                "index",
                Arrays.asList(
                    27297.8623046875, 30173.691650390625, 13219.574157714844,
                    27619.58447265625, 30468.77734375, 13557.738830566406,
                    27820.83154296875, 30940.971923828125, 13493.751281738281,
                    24791.223876953125, 29305.086059570312, 13585.541015625,
                    28562.748901367188, 32361.38720703125, 14279.127197265625,
                    26968.280639648438, 29676.578125, 13199.471435546875,
                    28985.5751953125, 32753.337890625, 14271.368591308594
                ),
                "min_all",
                Arrays.asList(
                    27297.8623046875, 27297.8623046875, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13199.471435546875,
                    13199.471435546875, 13199.471435546875, 13199.471435546875
                )
            )
        )
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // unstack, {d} + {m}
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusMarket, Arrays.asList("min_all = $min(index)"),
                    FlattenSpec.array(
                        Arrays.asList("dayOfWeek"), Arrays.asList("market"),
                        Arrays.asList("index", "min_all"),
                        "-"
                    )
                )
            )
        )
    );

    columnNames = new String[]{"rows", "columns"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            Arrays.asList("Friday", "Monday", "Saturday", "Sunday", "Thursday", "Tuesday", "Wednesday"),
            ImmutableMap.builder().put(
                "upfront-index",
                Arrays.asList(27297.8623046875, 27619.58447265625, 27820.83154296875, 24791.223876953125,
                    28562.748901367188, 26968.280639648438, 28985.5751953125)).put(
                "upfront-min_all",
                Arrays.asList(27297.8623046875, 13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13199.471435546875)).put(
                "spot-index",
                Arrays.asList(13219.574157714844, 13557.738830566406, 13493.751281738281, 13585.541015625,
                    14279.127197265625, 13199.471435546875, 14271.368591308594)).put(
                "spot-min_all",
                Arrays.asList(13219.574157714844, 13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13199.471435546875, 13199.471435546875)).put(
                "total_market-index",
                Arrays.asList(30173.691650390625, 30468.77734375, 30940.971923828125,
                    29305.086059570312, 32361.38720703125, 29676.578125, 32753.337890625)).put(
                "total_market-min_all",
                Arrays.asList(27297.8623046875, 13219.574157714844, 13219.574157714844, 13219.574157714844,
                    13219.574157714844, 13219.574157714844, 13199.471435546875)).build())
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // unstack, {m} + {d}
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusMarket, Arrays.asList("min_all = $min(index)"),
                    FlattenSpec.array(
                        Arrays.asList("market"), Arrays.asList("dayOfWeek"),
                        Arrays.asList("index", "min_all"),
                        "-"
                    )
                )
            )
        )
    );

    columnNames = new String[] {"rows", "columns"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames,
        array(
            Arrays.asList("upfront", "total_market", "spot"),
            ImmutableMap.builder().put(
                "Friday-index", list(27297.8623046875, 30173.691650390625, 13219.574157714844)).put(
                "Friday-min_all", list(27297.8623046875, 27297.8623046875, 13219.574157714844)).put(
                "Wednesday-index", list(28985.5751953125, 32753.337890625, 14271.368591308594)).put(
                "Wednesday-min_all", list(13199.471435546875, 13199.471435546875, 13199.471435546875)).put(
                "Thursday-index", list(28562.748901367188, 32361.38720703125, 14279.127197265625)).put(
                "Thursday-min_all", list(13219.574157714844, 13219.574157714844, 13219.574157714844)).put(
                "Sunday-index", list(24791.223876953125, 29305.086059570312, 13585.541015625)).put(
                "Sunday-min_all", list(13219.574157714844, 13219.574157714844, 13219.574157714844)).put(
                "Monday-index", list(27619.58447265625, 30468.77734375, 13557.738830566406)).put(
                "Monday-min_all", list(13219.574157714844, 13219.574157714844, 13219.574157714844)).put(
                "Tuesday-index", list(26968.280639648438, 29676.578125, 13199.471435546875)).put(
                "Tuesday-min_all", list(13219.574157714844, 13219.574157714844, 13199.471435546875)).put(
                "Saturday-index", list(27820.83154296875, 30940.971923828125, 13493.751281738281)).put(
                "Saturday-min_all", list(13219.574157714844, 13219.574157714844, 13219.574157714844)).build()
        )
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "sum_all = $sum(addRowsIndexConstant)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "sum_week = $sum(addRowsIndexConstant)")
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "addRowsIndexConstant", "sum_week", "sum_all"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27324.8623046875, 27324.8623046875, 27324.8623046875),
        array("Friday", 30200.691650390625, 57525.553955078125, 57525.553955078125),
        array("Friday", 13337.574157714844, 70863.12811279297, 70863.12811279297),
        array("Monday", 27646.58447265625, 27646.58447265625, 98509.71258544922),
        array("Monday", 30495.77734375, 58142.36181640625, 129005.48992919922),
        array("Monday", 13675.738830566406, 71818.10064697266, 142681.22875976562),
        array("Saturday", 27847.83154296875, 27847.83154296875, 170529.06030273438),
        array("Saturday", 30967.971923828125, 58815.803466796875, 201497.0322265625),
        array("Saturday", 13611.751281738281, 72427.55474853516, 215108.78350830078),
        array("Sunday", 24818.223876953125, 24818.223876953125, 239927.0073852539),
        array("Sunday", 29332.086059570312, 54150.30993652344, 269259.0934448242),
        array("Sunday", 13703.541015625, 67853.85095214844, 282962.6344604492),
        array("Thursday", 28591.748901367188, 28591.748901367188, 311554.3833618164),
        array("Thursday", 32390.38720703125, 60982.13610839844, 343944.77056884766),
        array("Thursday", 14406.127197265625, 75388.26330566406, 358350.8977661133),
        array("Tuesday", 26995.280639648438, 26995.280639648438, 385346.1784057617),
        array("Tuesday", 29703.578125, 56698.85876464844, 415049.7565307617),
        array("Tuesday", 13317.471435546875, 70016.33020019531, 428367.2279663086),
        array("Wednesday", 29014.5751953125, 29014.5751953125, 457381.8031616211),
        array("Wednesday", 32782.337890625, 61796.9130859375, 490164.1410522461),
        array("Wednesday", 14398.368591308594, 76195.2816772461, 504562.5096435547)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            dayPlusMarket, 17,
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusMarket,
                    "sum_all = $sum(index)", "sum_all_ratio_percent = sum_all / $last(sum_all) * 100"
                )
                ,
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(marketDsc),
                    "sum_week = $sum(index)", "sum_week_ratio_permil = sum_week / $last(sum_week) * 1000"
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "sum_week", "sum_week_ratio_permil", "sum_all", "sum_all_ratio_percent"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27297.8623046875, 386.1568351424768, 27297.8623046875, 5.423425227196043),
        array("Friday", 57471.553955078125, 812.9952865284308, 57471.553955078125, 11.418208213051406),
        array("Friday", 70691.12811279297, 1000.0, 70691.12811279297, 14.044617972889203),
        array("Monday", 27619.58447265625, 385.50017688678344, 98310.71258544922, 19.53196161620277),
        array("Monday", 58088.36181640625, 810.7679453851858, 128779.48992919922, 25.58537099469237),
        array("Monday", 71646.10064697266, 1000.0, 142337.22875976562, 28.278965898817994),
        array("Saturday", 27820.83154296875, 385.03381006195605, 170158.06030273438, 33.80629246921391),
        array("Saturday", 58761.803466796875, 813.2496341810753, 201099.0322265625, 39.95351549395745),
        array("Saturday", 72255.55474853516, 1000.0, 214592.78350830078, 42.634397619233674),
        array("Sunday", 24791.223876953125, 366.29057167010257, 239384.0073852539, 47.55981439680473),
        array("Sunday", 54096.30993652344, 799.2735005839294, 268689.0934448242, 53.382026453070154),
        array("Sunday", 67681.85095214844, 1000.0, 282274.6344604492, 56.081144979160555),
        array("Thursday", 28562.748901367188, 379.80730683552576, 310837.3833618164, 61.75587259045563),
        array("Thursday", 60924.13610839844, 810.1262289745588, 343198.77056884766, 68.18529778890917),
        array("Thursday", 75203.26330566406, 1000.0, 357477.8977661133, 71.02221511963704),
        array("Tuesday", 26968.280639648438, 386.1198262242484, 384446.1784057617, 76.38016043867606),
        array("Tuesday", 56644.85876464844, 811.0158491360268, 414122.7565307617, 82.27617898633873)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            dayPlusMarket, 18,
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusMarket, "mean_all = $mean(index, -1, 1)", "count_all = $size()"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(marketDsc), "mean_week = $mean(index, -1, 1)", "count_week = $size()"
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "index", "mean_all", "count_all", "mean_week", "count_week"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27297.8623046875, 28735.776977539062, 21L, 28735.776977539062, 3L),
        array("Friday", 30173.691650390625, 23563.70937093099, 21L, 23563.70937093099, 3L),
        array("Friday", 13219.574157714844, 23670.95009358724, 21L, 21696.632904052734, 3L),
        array("Monday", 27619.58447265625, 23769.3119913737, 21L, 29044.180908203125, 3L),
        array("Monday", 30468.77734375, 23882.033548990887, 21L, 23882.033548990887, 3L),
        array("Monday", 13557.738830566406, 23949.11590576172, 21L, 22013.258087158203, 3L),
        array("Saturday", 27820.83154296875, 24106.514099121094, 21L, 29380.901733398438, 3L),
        array("Saturday", 30940.971923828125, 24085.184916178387, 21L, 24085.184916178387, 3L),
        array("Saturday", 13493.751281738281, 23075.315694173176, 21L, 22217.361602783203, 3L),
        array("Sunday", 24791.223876953125, 22530.02040608724, 21L, 27048.15496826172, 3L),
        array("Sunday", 29305.086059570312, 22560.61698404948, 21L, 22560.61698404948, 3L),
        array("Sunday", 13585.541015625, 23817.7919921875, 21L, 21445.313537597656, 3L),
        array("Thursday", 28562.748901367188, 24836.559041341145, 21L, 30462.06805419922, 3L),
        array("Thursday", 32361.38720703125, 25067.754435221355, 21L, 25067.754435221355, 3L),
        array("Thursday", 14279.127197265625, 24536.265014648438, 21L, 23320.257202148438, 3L),
        array("Tuesday", 26968.280639648438, 23641.328653971355, 21L, 28322.42938232422, 3L),
        array("Tuesday", 29676.578125, 23281.443400065105, 21L, 23281.443400065105, 3L),
        array("Tuesday", 13199.471435546875, 23953.874918619793, 21L, 21438.024780273438, 3L)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 17,
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "row_num_all = $row_num(rows)",
                    "rank_all = $rank(rows)",
                    "dense_rank_all = $dense_rank(rows)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    "row_num_week = $row_num(rows)",
                    "rank_week = $rank(rows)",
                    "dense_rank_week = $dense_rank(rows)"
                )
            )
        )
    );

    columnNames = new String[]{
        "dayOfWeek", "rows", "row_num_all", "rank_all", "dense_rank_all", "row_num_week", "rank_week", "dense_rank_week"
    };

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        new Object[] {"Friday", 26L, 1L, 1L, 1L, 1L, 1L, 1L},
        new Object[] {"Friday", 26L, 2L, 1L, 1L, 2L, 1L, 1L},
        new Object[] {"Friday", 117L, 3L, 3L, 2L, 3L, 3L, 2L},
        new Object[] {"Monday", 26L, 4L, 4L, 3L, 1L, 1L, 1L},
        new Object[] {"Monday", 26L, 5L, 4L, 3L, 2L, 1L, 1L},
        new Object[] {"Monday", 117L, 6L, 6L, 4L, 3L, 3L, 2L},
        new Object[] {"Saturday", 26L, 7L, 7L, 5L, 1L, 1L, 1L},
        new Object[] {"Saturday", 26L, 8L, 7L, 5L, 2L, 1L, 1L},
        new Object[] {"Saturday", 117L, 9L, 9L, 6L, 3L, 3L, 2L},
        new Object[] {"Sunday", 26L, 10L, 10L, 7L, 1L, 1L, 1L},
        new Object[] {"Sunday", 26L, 11L, 10L, 7L, 2L, 1L, 1L},
        new Object[] {"Sunday", 117L, 12L, 12L, 8L, 3L, 3L, 2L},
        new Object[] {"Thursday", 28L, 13L, 13L, 9L, 1L, 1L, 1L},
        new Object[] {"Thursday", 28L, 14L, 13L, 9L, 2L, 1L, 1L},
        new Object[] {"Thursday", 126L, 15L, 15L, 10L, 3L, 3L, 2L},
        new Object[] {"Tuesday", 26L, 16L, 16L, 11L, 1L, 1L, 1L},
        new Object[] {"Tuesday", 26L, 17L, 16L, 11L, 2L, 1L, 1L}
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusRows, "lead_all = $lead(rows, 2)", "lag_all = $lag(rows, 2)"),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc), "lead_week = $lead(rows, 1)", "lag_week = $lag(rows, 1)"
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "rows", "lead_all", "lag_all", "lead_week", "lag_week"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 26L, 117L, null, 26L, null),
        array("Friday", 26L, 26L, null, 117L, 26L),
        array("Friday", 117L, 26L, 26L, null, 26L),
        array("Monday", 26L, 117L, 26L, 26L, null),
        array("Monday", 26L, 26L, 117L, 117L, 26L),
        array("Monday", 117L, 26L, 26L, null, 26L),
        array("Saturday", 26L, 117L, 26L, 26L, null),
        array("Saturday", 26L, 26L, 117L, 117L, 26L),
        array("Saturday", 117L, 26L, 26L, null, 26L),
        array("Sunday", 26L, 117L, 26L, 26L, null),
        array("Sunday", 26L, 28L, 117L, 117L, 26L),
        array("Sunday", 117L, 28L, 26L, null, 26L),
        array("Thursday", 28L, 126L, 26L, 28L, null),
        array("Thursday", 28L, 26L, 117L, 126L, 28L),
        array("Thursday", 126L, 26L, 28L, null, 28L),
        array("Tuesday", 26L, 117L, 28L, 26L, null),
        array("Tuesday", 26L, 28L, 126L, 117L, 26L),
        array("Tuesday", 117L, 28L, 26L, null, 26L),
        array("Wednesday", 28L, 126L, 26L, 28L, null),
        array("Wednesday", 28L, null, 117L, 126L, 28L),
        array("Wednesday", 126L, null, 28L, null, 28L)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "var_all = $variance(index, -2, 2)", "stddev_all = $stddev(index, -2, 2)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    "var_week = $variancePop(index)", "stddev_week = $stddevPop(index)"
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "var_all", "stddev_all", "var_week", "stddev_week"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 8.23184485883755E7, 9072.95148165003, 0.0, 0.0),
        array("Friday", 6.6798956652896374E7, 8173.062868527097, 2067598.606401816, 1437.9146728515625),
        array("Friday", 5.118463227752918E7, 7154.343595154567, 5.4878965725583665E7, 7408.033863690396),
        array("Monday", 6.9699684350707E7, 8348.633681669533, 0.0, 0.0),
        array("Monday", 8.121628016621359E7, 9012.007554713522, 2029475.0041728616, 1424.596435546875),
        array("Monday", 5.127456043179982E7, 7160.625701138122, 5.464851405255904E7, 7392.463327779114),
        array("Saturday", 7.166847475145732E7, 8465.723522030312, 0.0, 0.0),
        array("Saturday", 7.639395500419313E7, 8740.363551031109, 2433818.9990673214, 1560.0701904296875),
        array("Saturday", 4.846446084613509E7, 6961.642108449349, 5.771177921575277E7, 7596.826917585577),
        array("Sunday", 5.949881278610817E7, 7713.5473542403415, 0.0, 0.0),
        array("Sunday", 7.729272157966447E7, 8791.627925456381, 5093737.9509154, 2256.9310913085938),
        array("Sunday", 5.3287450041427605E7, 7299.825343213877, 4.3671819620105565E7, 6608.465753872495),
        array("Thursday", 7.151765629584774E7, 8456.811236858, 0.0, 0.0),
        array("Thursday", 8.136008163725007E7, 9019.98235238019, 3607413.244314585, 1899.3191528320312),
        array("Thursday", 4.95463678640273E7, 7038.918089026701, 6.060218100265652E7, 7784.740265587318),
        array("Tuesday", 6.5575344089929044E7, 8097.860463723059, 0.0, 0.0),
        array("Tuesday", 8.171276429409897E7, 9039.511286242137, 1833718.8172903992, 1354.1487426757812),
        array("Tuesday", 5.8082056421731636E7, 7621.158469795234, 5.204555855819271E7, 7214.260776974499),
        array("Wednesday", 7.966594384238192E7, 8925.578067687376, 0.0, 0.0),
        array("Wednesday", 1.0041590247200173E8, 10020.773546588194, 3549008.9320471287, 1883.88134765625),
        array("Wednesday", 9.538118348239581E7, 9766.329068918158, 6.358745565493054E7, 7974.174292986738)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "p5_all = $percentile(index, 0.5)", "p5_all_win = $percentile(index, 0.5, -5, 5)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    "p3_week = $percentile(index, 0.3)", "p7_week = $percentile(index, 0.7)"
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "p5_all", "p5_all_win", "p3_week", "p7_week"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 30173.691650390625, 27619.58447265625, 30173.691650390625, 30173.691650390625),
        array("Friday", 30173.691650390625, 27619.58447265625, 27297.8623046875, 30173.691650390625),
        array("Friday", 27297.8623046875, 27820.83154296875, 13219.574157714844, 30173.691650390625),
        array("Monday", 30173.691650390625, 27619.58447265625, 30468.77734375, 30468.77734375),
        array("Monday", 27619.58447265625, 27820.83154296875, 27619.58447265625, 30468.77734375),
        array("Monday", 27619.58447265625, 27619.58447265625, 13557.738830566406, 30468.77734375),
        array("Saturday", 27619.58447265625, 27297.8623046875, 30940.971923828125, 30940.971923828125),
        array("Saturday", 27820.83154296875, 27619.58447265625, 27820.83154296875, 30940.971923828125),
        array("Saturday", 27619.58447265625, 27820.83154296875, 13493.751281738281, 30940.971923828125),
        array("Sunday", 27820.83154296875, 27619.58447265625, 29305.086059570312, 29305.086059570312),
        array("Sunday", 27619.58447265625, 27820.83154296875, 24791.223876953125, 29305.086059570312),
        array("Sunday", 27619.58447265625, 27820.83154296875, 13585.541015625, 29305.086059570312),
        array("Thursday", 27619.58447265625, 26968.280639648438, 32361.38720703125, 32361.38720703125),
        array("Thursday", 27820.83154296875, 26968.280639648438, 28562.748901367188, 32361.38720703125),
        array("Thursday", 27619.58447265625, 28562.748901367188, 14279.127197265625, 32361.38720703125),
        array("Tuesday", 27820.83154296875, 26968.280639648438, 29676.578125, 29676.578125),
        array("Tuesday", 27619.58447265625, 28562.748901367188, 26968.280639648438, 29676.578125),
        array("Tuesday", 27619.58447265625, 28562.748901367188, 13199.471435546875, 29676.578125),
        array("Wednesday", 27619.58447265625, 28562.748901367188, 32753.337890625, 32753.337890625),
        array("Wednesday", 27820.83154296875, 26968.280639648438, 28985.5751953125, 32753.337890625),
        array("Wednesday", 27619.58447265625, 28985.5751953125, 14271.368591308594, 32753.337890625)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "p5_all = $percentile(index, 0.5)", "$assign(p5_all_first_two, 0, 2) = $last(p5_all)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    "p5_week = $percentile(index, 0.5)", "$assign(p5_week_last, -1) = $last(p5_week)"
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "p5_all", "p5_all_first_two", "p5_week", "p5_week_last"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 30173.691650390625, 27619.58447265625, 30173.691650390625, null),
        array("Friday", 30173.691650390625, 27619.58447265625, 30173.691650390625, null),
        array("Friday", 27297.8623046875, null, 27297.8623046875, 27297.8623046875),
        array("Monday", 30173.691650390625, null, 30468.77734375, null),
        array("Monday", 27619.58447265625, null, 30468.77734375, null),
        array("Monday", 27619.58447265625, null, 27619.58447265625, 27619.58447265625),
        array("Saturday", 27619.58447265625, null, 30940.971923828125, null),
        array("Saturday", 27820.83154296875, null, 30940.971923828125, null),
        array("Saturday", 27619.58447265625, null, 27820.83154296875, 27820.83154296875),
        array("Sunday", 27820.83154296875, null, 29305.086059570312, null),
        array("Sunday", 27619.58447265625, null, 29305.086059570312, null),
        array("Sunday", 27619.58447265625, null, 24791.223876953125, 24791.223876953125),
        array("Thursday", 27619.58447265625, null, 32361.38720703125, null),
        array("Thursday", 27820.83154296875, null, 32361.38720703125, null),
        array("Thursday", 27619.58447265625, null, 28562.748901367188, 28562.748901367188),
        array("Tuesday", 27820.83154296875, null, 29676.578125, null),
        array("Tuesday", 27619.58447265625, null, 29676.578125, null),
        array("Tuesday", 27619.58447265625, null, 26968.280639648438, 26968.280639648438),
        array("Wednesday", 27619.58447265625, null, 32753.337890625, null),
        array("Wednesday", 27820.83154296875, null, 32753.337890625, null),
        array("Wednesday", 27619.58447265625, null, 28985.5751953125, 28985.5751953125)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "p5_all = $percentile(index, 0.5)", "$assign(p5_all_first_two, 0, 2) = $last(p5_all)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    Arrays.asList("p5_week = $percentile(index, 0.5)", "$assign(p5_week_last, -1) = $last(p5_week)"),
                    FlattenSpec.array(Arrays.asList(columnNames), null)
                )
            )
        )
    );

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday",
            Arrays.asList(30173.691650390625, 30173.691650390625, 27297.8623046875),
            Arrays.asList(27619.58447265625, 27619.58447265625, null),
            Arrays.asList(30173.691650390625, 30173.691650390625, 27297.8623046875),
            Arrays.asList(null, null, 27297.8623046875)),
        array("Monday",
            Arrays.asList(30173.691650390625, 27619.58447265625, 27619.58447265625),
            Arrays.asList(null, null, null),
            Arrays.asList(30468.77734375, 30468.77734375, 27619.58447265625),
            Arrays.asList(null, null, 27619.58447265625)),
        array("Saturday",
            Arrays.asList(27619.58447265625, 27820.83154296875, 27619.58447265625),
            Arrays.asList(null, null, null),
            Arrays.asList(30940.971923828125, 30940.971923828125, 27820.83154296875),
            Arrays.asList(null, null, 27820.83154296875)),
        array("Sunday",
            Arrays.asList(27820.83154296875, 27619.58447265625, 27619.58447265625),
            Arrays.asList(null, null, null),
            Arrays.asList(29305.086059570312, 29305.086059570312, 24791.223876953125),
            Arrays.asList(null, null, 24791.223876953125)),
        array("Thursday",
            Arrays.asList(27619.58447265625, 27820.83154296875, 27619.58447265625),
            Arrays.asList(null, null, null),
            Arrays.asList(32361.38720703125, 32361.38720703125, 28562.748901367188),
            Arrays.asList(null, null, 28562.748901367188)),
        array("Tuesday",
            Arrays.asList(27820.83154296875, 27619.58447265625, 27619.58447265625),
            Arrays.asList(null, null, null),
            Arrays.asList(29676.578125, 29676.578125, 26968.280639648438),
            Arrays.asList(null, null, 26968.280639648438)),
        array("Wednesday",
            Arrays.asList(27619.58447265625, 27820.83154296875, 27619.58447265625),
            Arrays.asList(null, null, null),
            Arrays.asList(32753.337890625, 32753.337890625, 28985.5751953125),
            Arrays.asList(null, null, 28985.5751953125))
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "p5_all = $percentile(index, 0.5)", "$assign(p5_all_first_two, 0, 2) = $last(p5_all)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    Arrays.asList("p5_week = $percentile(index, 0.5)", "$assign(p5_week_last, -1) = $last(p5_week)"),
                    FlattenSpec.expand(Arrays.asList(columnNames), ".")
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek",
        "0.p5_all", "0.p5_all_first_two", "0.p5_week", "0.p5_week_last",
        "1.p5_all", "1.p5_all_first_two", "1.p5_week", "1.p5_week_last",
        "2.p5_all", "2.p5_all_first_two", "2.p5_week", "2.p5_week_last"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 30173.691650390625, 27619.58447265625, 30173.691650390625, null,
            30173.691650390625, 27619.58447265625, 30173.691650390625, null,
            27297.8623046875, null, 27297.8623046875, 27297.8623046875),
        array("Monday", 30173.691650390625, null, 30468.77734375, null,
            27619.58447265625, null, 30468.77734375, null,
            27619.58447265625, null, 27619.58447265625, 27619.58447265625),
        array("Saturday", 27619.58447265625, null, 30940.971923828125, null,
            27820.83154296875, null, 30940.971923828125, null,
            27619.58447265625, null, 27820.83154296875, 27820.83154296875),
        array("Sunday", 27820.83154296875, null, 29305.086059570312, null,
            27619.58447265625, null, 29305.086059570312, null,
            27619.58447265625, null, 24791.223876953125, 24791.223876953125),
        array("Thursday", 27619.58447265625, null, 32361.38720703125, null,
            27820.83154296875, null, 32361.38720703125, null,
            27619.58447265625, null, 28562.748901367188, 28562.748901367188),
        array("Tuesday", 27820.83154296875, null, 29676.578125, null,
            27619.58447265625, null, 29676.578125, null,
            27619.58447265625, null, 26968.280639648438, 26968.280639648438),
        array("Wednesday", 27619.58447265625, null, 32753.337890625, null,
            27820.83154296875, null, 32753.337890625, null,
            27619.58447265625, null, 28985.5751953125, 28985.5751953125)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec( null, dayPlusRows, "index_bin = $histogram(index, 3)")
            )
        )
    );

    columnNames = new String[]{"index", "index_bin"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(30173.691650390625, null),
        array(27297.8623046875, null),
        array(13219.574157714844, null),
        array(30468.77734375, null),
        array(27619.58447265625, null),
        array(13557.738830566406, null),
        array(30940.971923828125, null),
        array(27820.83154296875, null),
        array(13493.751281738281, null),
        array(29305.086059570312, null),
        array(24791.223876953125, null),
        array(13585.541015625, null),
        array(32361.38720703125, null),
        array(28562.748901367188, null),
        array(14279.127197265625, null),
        array(29676.578125, null),
        array(26968.280639648438, null),
        array(13199.471435546875, null),
        array(32753.337890625, null),
        array(28985.5751953125, null),
        array(
            14271.368591308594, ImmutableMap.of(
                "min", 13199.471435546875,
                "max", 32753.337890625,
                "breaks", Doubles.asList(13199.471435546875, 19717.426920572918, 26235.38240559896, 32753.337890625),
                "counts", Ints.asList(7, 1, 13)
            )
        )
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusRows, "index_bin = $histogram(index, 8, 26000, 1000)")
            )
        )
    );

    columnNames = new String[]{"index", "index_bin"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(30173.691650390625, null),
        array(27297.8623046875, null),
        array(13219.574157714844, null),
        array(30468.77734375, null),
        array(27619.58447265625, null),
        array(13557.738830566406, null),
        array(30940.971923828125, null),
        array(27820.83154296875, null),
        array(13493.751281738281, null),
        array(29305.086059570312, null),
        array(24791.223876953125, null),
        array(13585.541015625, null),
        array(32361.38720703125, null),
        array(28562.748901367188, null),
        array(14279.127197265625, null),
        array(29676.578125, null),
        array(26968.280639648438, null),
        array(13199.471435546875, null),
        array(32753.337890625, null),
        array(28985.5751953125, null),
        array(14271.368591308594, ImmutableMap.of(
            "min", 13199.471435546875,
            "max", 32753.337890625,
            "breaks", Doubles.asList(26000, 27000, 28000, 29000, 30000, 31000, 32000, 33000, 34000),
            "counts", Ints.asList(1, 3, 2, 2, 3, 0, 2, 0))
        )
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  @Test
  public void testPivot()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek");
    OrderByColumnSpec marketDesc = OrderByColumnSpec.desc("marketDesc");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(QueryRunnerTestHelper.addRowsIndexConstant)
        .setGranularity(QueryGranularities.ALL)
        .setDimFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null),
                    new SelectorDimFilter("market", "total_market", null)
                )
            )
        );

    // basic
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.of(PivotColumnSpec.toSpecs("market"), "index")
                )
            )
        )
    );
    String[] columnNames = new String[]{"dayOfWeek", "upfront", "spot", "total_market"};

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27297.8623046875, 13219.574157714844, 30173.691650390625),
        array("Monday", 27619.58447265625, 13557.738830566406, 30468.77734375),
        array("Saturday", 27820.83154296875, 13493.751281738281, 30940.971923828125),
        array("Sunday", 24791.223876953125, 13585.541015625, 29305.086059570312),
        array("Thursday", 28562.748901367188, 14279.127197265625, 32361.38720703125),
        array("Tuesday", 26968.280639648438, 13199.471435546875, 29676.578125),
        array("Wednesday", 28985.5751953125, 14271.368591308594, 32753.337890625)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // appendValueColumn = true
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.of(PivotColumnSpec.toSpecs("market"), "index").withAppendValueColumn(true)
                )
            )
        )
    );
    columnNames = new String[]{"dayOfWeek", "upfront-index", "spot-index", "total_market-index"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27297.8623046875, 13219.574157714844, 30173.691650390625),
        array("Monday", 27619.58447265625, 13557.738830566406, 30468.77734375),
        array("Saturday", 27820.83154296875, 13493.751281738281, 30940.971923828125),
        array("Sunday", 24791.223876953125, 13585.541015625, 29305.086059570312),
        array("Thursday", 28562.748901367188, 14279.127197265625, 32361.38720703125),
        array("Tuesday", 26968.280639648438, 13199.471435546875, 29676.578125),
        array("Wednesday", 28985.5751953125, 14271.368591308594, 32753.337890625)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // multi-valued
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.of(PivotColumnSpec.toSpecs("market"), "index", "rows")
                )
            )
        )
    );
    columnNames = new String[]{"dayOfWeek", "spot", "total_market", "upfront"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", list(13219.574157714844D, 117L), list(30173.691650390625D, 26L), list(27297.8623046875D, 26L)),
        array("Monday", list(13557.738830566406D, 117L), list(30468.77734375D, 26L), list(27619.58447265625D, 26L)),
        array("Saturday", list(13493.751281738281D, 117L), list(30940.971923828125D, 26L), list(27820.83154296875D, 26L)),
        array("Sunday", list(13585.541015625D, 117L), list(29305.086059570312D, 26L), list(24791.223876953125D, 26L)),
        array("Thursday", list(14279.127197265625D, 126L), list(32361.38720703125D, 28L), list(28562.748901367188D, 28L)),
        array("Tuesday", list(13199.471435546875D, 117L), list(29676.578125D, 26L), list(26968.280639648438D, 26L)),
        array("Wednesday", list(14271.368591308594D, 126L), list(32753.337890625D, 28L), list(28985.5751953125D, 28L))
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // multi-value (appendValueColumn = true)
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.of(PivotColumnSpec.toSpecs("market"), "index", "rows").withAppendValueColumn(true)
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "spot-index", "spot-rows", "total_market-index", "total_market-rows", "upfront-index", "upfront-rows"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 13219.574157714844, 117L, 30173.691650390625, 26L, 27297.8623046875, 26L),
        array("Monday", 13557.738830566406, 117L, 30468.77734375, 26L, 27619.58447265625, 26L),
        array("Saturday", 13493.751281738281, 117L, 30940.971923828125, 26L, 27820.83154296875, 26L),
        array("Sunday", 13585.541015625, 117L, 29305.086059570312, 26L, 24791.223876953125, 26L),
        array("Thursday", 14279.127197265625, 126L, 32361.38720703125, 28L, 28562.748901367188, 28L),
        array("Tuesday", 13199.471435546875, 117L, 29676.578125, 26L, 26968.280639648438, 26L),
        array("Wednesday", 14271.368591308594, 126L, 32753.337890625, 28L, 28985.5751953125, 28L)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // custom comparator
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc.withComparator("dayofweek")),
                    Arrays.<String>asList(),
                    PivotSpec.of(PivotColumnSpec.toSpecs("market"), "index")
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "upfront", "spot", "total_market"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Monday", 27619.58447265625, 13557.738830566406, 30468.77734375),
        array("Tuesday", 26968.280639648438, 13199.471435546875, 29676.578125),
        array("Wednesday", 28985.5751953125, 14271.368591308594, 32753.337890625),
        array("Thursday", 28562.748901367188, 14279.127197265625, 32361.38720703125),
        array("Friday", 27297.8623046875, 13219.574157714844, 30173.691650390625),
        array("Saturday", 27820.83154296875, 13493.751281738281, 30940.971923828125),
        array("Sunday", 24791.223876953125, 13585.541015625, 29305.086059570312)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // filtered
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"), Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.of(
                        Arrays.asList(new PivotColumnSpec("market", Arrays.asList("upfront", "spot", "dummy"))),
                        "index"
                    )
                )
            )
        )
    );
    columnNames = new String[]{"dayOfWeek", "upfront", "spot"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27297.8623046875, 13219.574157714844),
        array("Monday", 27619.58447265625, 13557.738830566406),
        array("Saturday", 27820.83154296875, 13493.751281738281),
        array("Sunday", 24791.223876953125, 13585.541015625),
        array("Thursday", 28562.748901367188, 14279.127197265625),
        array("Tuesday", 26968.280639648438, 13199.471435546875),
        array("Wednesday", 28985.5751953125, 14271.368591308594)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // expression, filtered
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"), Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.of(
                        Arrays.asList(
                            PivotColumnSpec.ofExpression(
                                "substring(market, 0, 4)",
                                null,
                                null,
                                Arrays.asList("upfr", "spot", "dumm")
                            )
                        ),
                        "index"
                    )
                )
            )
        )
    );
    columnNames = new String[]{"dayOfWeek", "upfr", "spot"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Friday", 27297.8623046875, 13219.574157714844),
        array("Monday", 27619.58447265625, 13557.738830566406),
        array("Saturday", 27820.83154296875, 13493.751281738281),
        array("Sunday", 24791.223876953125, 13585.541015625),
        array("Thursday", 28562.748901367188, 14279.127197265625),
        array("Tuesday", 26968.280639648438, 13199.471435546875),
        array("Wednesday", 28985.5751953125, 14271.368591308594)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // row expression
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("market"), Arrays.asList(marketDesc),
                    Arrays.<String>asList(),
                    new PivotSpec(
                        Arrays.asList(
                            PivotColumnSpec.ofColumn(
                                "dayOfWeek", null, "dayOfWeek", Arrays.asList("Monday", "Wednesday", "Friday")
                            )
                        ),
                        Arrays.asList("index"),
                        null,
                        Arrays.<String>asList("sum = Monday + Wednesday + Friday"),
                        null,
                        false,
                        false
                    )
                )
            )
        )
    );
    columnNames = new String[]{"market", "Monday", "Wednesday", "Friday", "sum"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("spot", 13557.738830566406, 14271.368591308594, 13219.574157714844, 41048.681579589844),
        array("total_market", 30468.77734375, 32753.337890625, 30173.691650390625, 93395.80688476562),
        array("upfront", 27619.58447265625, 28985.5751953125, 27297.8623046875, 83903.02197265625)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // partition expression
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("market"), Arrays.asList(marketDesc),
                    Arrays.<String>asList(),
                    new PivotSpec(
                        Arrays.asList(
                            PivotColumnSpec.ofColumn(
                                "dayOfWeek", null, "dayOfWeek", Arrays.asList("Monday", "Wednesday", "Friday")
                            )
                        ),
                        Arrays.asList("index"),
                        null,
                        Arrays.<String>asList(
                            "sum = Monday + Wednesday + Friday"
                        ),
                        PartitionExpression.from(
                            "Monday = $sum(Monday)",
                            "Wednesday = $delta(Wednesday)",
                            "#Friday = $sum(Friday)",
                            "Friday = Friday / #Friday * 100"
                        ),
                        false,
                        false
                    )
                )
            )
        )
    );
    columnNames = new String[]{"market", "Monday", "Wednesday", "Friday", "sum"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("spot", 13557.738830566406, 0.0, 18.700471347156927, 41048.681579589844),
        array("total_market", 44026.516174316406, 18481.969299316406, 42.683845138595395, 93395.80688476562),
        array("upfront", 71646.10064697266, -3767.7626953125, 38.61568351424768, 83903.02197265625)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  @Test
  public void testPivotTable()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek", "dayofweek");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            DefaultDimensionSpec.of("market"),
            DefaultDimensionSpec.of("quality"),
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(QueryRunnerTestHelper.addRowsIndexConstant)
        .setGranularity(QueryGranularities.ALL)
        .setHavingSpec(
            AndHavingSpec.of(
                new ExpressionHavingSpec("!(dayOfWeek == 'Monday' && market == 'spot')"),
                new ExpressionHavingSpec("!(dayOfWeek == 'Tuesday' && market == 'total_market')"),
                new ExpressionHavingSpec("!(dayOfWeek == 'Wednesday' && quality == 'premium')")
            )
        );

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                )
            )
        )
    );
    String[] columnNames = new String[]{
        "dayOfWeek",
        "spot-automotive", "spot-business", "spot-entertainment", "spot-health", "spot-mezzanine",
        "spot-news", "spot-premium", "spot-technology", "spot-travel",
        "total_market-mezzanine", "total_market-premium", "upfront-mezzanine", "upfront-premium"};

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null, null, null, null, null, null, null,
            15301.728393554688, 15167.04833984375, 15479.327270507812, 12140.257507324219
        ),
        array(
            "Tuesday",
            1664.368782043457, 1404.3215408325195, 1653.3230514526367, 1522.367774963379, 1369.873420715332,
            1425.5140914916992, 1560.511329650879, 1068.2061462402344, 1530.9851303100586,
            null, null, 15147.467102050781, 11820.81298828125
        ),
        array(
            "Wednesday",
            1801.9095306396484, 1559.0761184692383, 1783.8484954833984, 1556.1792068481445, 1477.5527877807617,
            1566.9974746704102, null, 1268.3166580200195, 1623.1850204467773,
            15749.735595703125, null, 14765.832275390625, null
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // with row evaluation
    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                             .withRowExpressions("test = round(\"upfront-mezzanine\", 1)")
                )
            )
        )
    );
    columnNames = new String[]{
        "dayOfWeek",
        "spot-automotive", "spot-business", "spot-entertainment", "spot-health", "spot-mezzanine",
        "spot-news", "spot-premium", "spot-technology", "spot-travel",
        "total_market-mezzanine", "total_market-premium", "upfront-mezzanine", "upfront-premium", "test"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null, null, null, null, null, null, null,
            15301.728393554688, 15167.04833984375, 15479.327270507812, 12140.257507324219, 15479.3
        ),
        array(
            "Tuesday",
            1664.368782043457, 1404.3215408325195, 1653.3230514526367, 1522.367774963379, 1369.873420715332,
            1425.5140914916992, 1560.511329650879, 1068.2061462402344, 1530.9851303100586,
            null, null, 15147.467102050781, 11820.81298828125, 15147.5
        ),
        array(
            "Wednesday",
            1801.9095306396484, 1559.0761184692383, 1783.8484954833984, 1556.1792068481445, 1477.5527877807617,
            1566.9974746704102, null, 1268.3166580200195, 1623.1850204467773,
            15749.735595703125, null, 14765.832275390625, null, 14765.8
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    for (Object x : results) {
      System.out.println(x);
    }
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // multi-value-expanded
    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(
                        PivotColumnSpec.toSpecs("market", "quality"), "index", "rows"
                    ).withAppendValueColumn(true)
                )
            )
        )
    );

    columnNames = new String[]{
        "dayOfWeek",
        "spot-automotive-index", "spot-automotive-rows", "spot-business-index", "spot-business-rows",
        "spot-entertainment-index", "spot-entertainment-rows", "spot-health-index", "spot-health-rows",
        "spot-mezzanine-index", "spot-mezzanine-rows", "spot-news-index", "spot-news-rows",
        "spot-premium-index", "spot-premium-rows", "spot-technology-index", "spot-technology-rows",
        "spot-travel-index", "spot-travel-rows",
        "total_market-mezzanine-index", "total_market-mezzanine-rows", "total_market-premium-index", "total_market-premium-rows",
        "upfront-mezzanine-index", "upfront-mezzanine-rows", "upfront-premium-index", "upfront-premium-rows"};

    // market-quality-<metrics>
    // c(market,index) = sum(c(market,index))
    //
    // spot-index.sum = !sum(spot-<quality>-index)
    // spot-rows.sum = !sum(spot-<quality>-rows)
    // total_market-index.sum = !sum(total_market-<quality>-index)
    // total_market-rows.sum = !sum(total_market-<quality>-rows)
    // upfront-index.sum = !sum(upfront-<quality>-index)
    // upfront-rows.sum = !sum(upfront-<quality>-rows)
    //
    // index.sum = spot-index.sum + total_market-index.sum + upfront-index.sum
    // rows.sum = spot-rows.sum + total_market-rows.sum + upfront-rows.sum
    //
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Monday",
              null, null, null, null, null, null, null, null,
              null, null, null, null, null, null, null, null,
              null, null,
              15301.728393554688, 13L, 15167.04833984375, 13L, 15479.327270507812, 13L, 12140.257507324219, 13L),
        array("Tuesday",
              1664.368782043457, 13L, 1404.3215408325195, 13L, 1653.3230514526367, 13L, 1522.367774963379, 13L,
              1369.873420715332, 13L, 1425.5140914916992, 13L, 1560.511329650879, 13L, 1068.2061462402344, 13L,
              1530.9851303100586, 13L,
              null, null, null, null, 15147.467102050781, 13L, 11820.81298828125, 13L),
        array("Wednesday",
              1801.9095306396484, 14L, 1559.0761184692383, 14L, 1783.8484954833984, 14L, 1556.1792068481445, 14L,
              1477.5527877807617, 14L, 1566.9974746704102, 14L, null, null, 1268.3166580200195, 14L,
              1623.1850204467773, 14L,
              15749.735595703125, 14L, null, null, 14765.832275390625, 14L, null, null)
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                             .withPartitionExpressions(PartitionExpression.of("_ = $sum(_)"))
                )
            )
        )
    );

    columnNames = new String[]{
        "dayOfWeek",
        "spot-automotive", "spot-business", "spot-entertainment", "spot-health", "spot-mezzanine",
        "spot-news", "spot-premium", "spot-technology", "spot-travel",
        "total_market-mezzanine", "total_market-premium", "upfront-mezzanine", "upfront-premium"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            15301.728393554688, 15167.04833984375, 15479.327270507812, 12140.257507324219
        ),
        array(
            "Tuesday",
            1664.368782043457, 1404.3215408325195, 1653.3230514526367, 1522.367774963379, 1369.873420715332,
            1425.5140914916992, 1560.511329650879, 1068.2061462402344, 1530.9851303100586,
            15301.728393554688, 15167.04833984375, 30626.794372558594, 23961.07049560547
        ),
        array(
            "Wednesday",
            3466.2783126831055, 2963.397659301758, 3437.171546936035, 3078.5469818115234, 2847.4262084960938,
            2992.5115661621094, 1560.511329650879, 2336.522804260254, 3154.170150756836,
            31051.463989257812, 15167.04833984375, 45392.62664794922, 23961.07049560547
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                             .withPartitionExpressions(
                                 PartitionExpression.from(
                                     "#_ = $sum(_)",
                                     "_ = case(#_ == 0, 0.0, _ / #_ * 100)"
                                 )
                             )
                )
            )
        )
    );

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            16.813941650603674, 19.44656562237923, 15.17663768899694, 16.009429362202997
        ),
        array(
            "Tuesday",
            15.75634222579802, 15.882278300416639, 15.990390808152055, 17.304017518163935, 16.148675284780392,
            15.990723077592929, 20.09860503302568, 15.024990146457418, 15.946557910143886,
            0.0, 0.0, 14.851266860403003, 15.588175986014235
        ),
        array(
            "Wednesday",
            17.058420904667074, 17.632486638624368, 17.25278950187719, 17.68833569624825, 17.418047408740776,
            17.57781478997598, 0.0, 17.839670138962944, 16.906900932597882,
            17.306223748562424, 0.0, 14.477094689189887, 0.0
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                             .withPartitionExpressions(
                                 PartitionExpression.from(
                                     new String[]{"^spot-.*", "#_ = $sum(_)"},
                                     new String[]{"^spot-.*", "_ = case(#_ == 0, 0.0, _ / #_ * 100)"}
                                 )
                             )
                )
            )
        )
    );

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            15301.728393554688, 15167.04833984375, 15479.327270507812, 12140.257507324219
        ),
        array(
            "Tuesday",
            15.75634222579802, 15.882278300416639, 15.990390808152055, 17.304017518163935, 16.148675284780392,
            15.990723077592929, 20.09860503302568, 15.024990146457418, 15.946557910143886,
            null, null, 15147.467102050781, 11820.81298828125
        ),
        array(
            "Wednesday",
            17.058420904667074, 17.632486638624368, 17.25278950187719, 17.68833569624825, 17.418047408740776,
            17.57781478997598, 0.0, 17.839670138962944, 16.906900932597882,
            15749.735595703125, null, 14765.832275390625, null
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 24,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs(), "index", "rows")
                             .withRowExpressions("index_part = $sum(index)", "rows_part = $sum(rows)")
                             .withPartitionExpressions(PartitionExpression.of("_ = $sum(_)"))
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "rows_part", "index_part", "rows", "index"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Monday", 13L, 15301.728393554688, 13.0, 15301.728393554688),
        array("Monday", 26L, 30468.776733398438, 26.0, 30468.776733398438),
        array("Monday", 39L, 45948.10400390625, 39.0, 45948.10400390625),
        array("Monday", 52L, 58088.36151123047, 52.0, 58088.36151123047),
        array("Tuesday", 13L, 1664.368782043457, 65.0, 59752.730293273926),
        array("Tuesday", 26L, 3068.6903228759766, 78.0, 61157.051834106445),
        array("Tuesday", 39L, 4722.013374328613, 91.0, 62810.37488555908),
        array("Tuesday", 52L, 6244.381149291992, 104.0, 64332.74266052246),
        array("Tuesday", 65L, 7614.254570007324, 117.0, 65702.6160812378),
        array("Tuesday", 78L, 9039.768661499023, 130.0, 67128.13017272949),
        array("Tuesday", 91L, 10600.279991149902, 143.0, 68688.64150238037),
        array("Tuesday", 104L, 11668.486137390137, 156.0, 69756.8476486206),
        array("Tuesday", 117L, 13199.471267700195, 169.0, 71287.83277893066),
        array("Tuesday", 130L, 28346.938369750977, 182.0, 86435.29988098145),
        array("Tuesday", 143L, 40167.75135803223, 195.0, 98256.1128692627),
        array("Wednesday", 14L, 1801.9095306396484, 209.0, 100058.02239990234),
        array("Wednesday", 28L, 3360.9856491088867, 223.0, 101617.09851837158),
        array("Wednesday", 42L, 5144.834144592285, 237.0, 103400.94701385498),
        array("Wednesday", 56L, 6701.01335144043, 251.0, 104957.12622070312),
        array("Wednesday", 70L, 8178.566139221191, 265.0, 106434.67900848389),
        array("Wednesday", 84L, 9745.563613891602, 279.0, 108001.6764831543),
        array("Wednesday", 98L, 11013.880271911621, 293.0, 109269.99314117432),
        array("Wednesday", 112L, 12637.065292358398, 307.0, 110893.1781616211),
        array("Wednesday", 126L, 28386.800888061523, 321.0, 126642.91375732422)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // someone can understand this, maybe
    builder.setLimitSpec(
        new LimitSpec(
            null, 24,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs(), "index", "rows")
                             .withRowExpressions(
                                 "#_ = $sum(_)",
                                 "concat(_, '.percent') = round(_ / #_ * 100, 3)"
                             )
                )
            )
        )
    );

    columnNames = new String[]{"dayOfWeek", "index.percent", "rows.percent", "index", "rows"};

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Monday", 26.342, 25.0, 15301.728393554688, 13L),
        array("Monday", 26.11, 25.0, 15167.04833984375, 13L),
        array("Monday", 26.648, 25.0, 15479.327270507812, 13L),
        array("Monday", 20.9, 25.0, 12140.257507324219, 13L),
        array("Tuesday", 4.144, 9.091, 1664.368782043457, 13L),
        array("Tuesday", 3.496, 9.091, 1404.3215408325195, 13L),
        array("Tuesday", 4.116, 9.091, 1653.3230514526367, 13L),
        array("Tuesday", 3.79, 9.091, 1522.367774963379, 13L),
        array("Tuesday", 3.41, 9.091, 1369.873420715332, 13L),
        array("Tuesday", 3.549, 9.091, 1425.5140914916992, 13L),
        array("Tuesday", 3.885, 9.091, 1560.511329650879, 13L),
        array("Tuesday", 2.659, 9.091, 1068.2061462402344, 13L),
        array("Tuesday", 3.811, 9.091, 1530.9851303100586, 13L),
        array("Tuesday", 37.711, 9.091, 15147.467102050781, 13L),
        array("Tuesday", 29.429, 9.091, 11820.81298828125, 13L),
        array("Wednesday", 4.176, 10.0, 1801.9095306396484, 14L),
        array("Wednesday", 3.613, 10.0, 1559.0761184692383, 14L),
        array("Wednesday", 4.134, 10.0, 1783.8484954833984, 14L),
        array("Wednesday", 3.606, 10.0, 1556.1792068481445, 14L),
        array("Wednesday", 3.424, 10.0, 1477.5527877807617, 14L),
        array("Wednesday", 3.631, 10.0, 1566.9974746704102, 14L),
        array("Wednesday", 2.939, 10.0, 1268.3166580200195, 14L),
        array("Wednesday", 3.761, 10.0, 1623.1850204467773, 14L),
        array("Wednesday", 36.498, 10.0, 15749.735595703125, 14L)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  @Test
  public void test834()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek", "dayofweek");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            DefaultDimensionSpec.of("market"),
            DefaultDimensionSpec.of("quality"),
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(QueryRunnerTestHelper.addRowsIndexConstant)
        .setGranularity(QueryGranularities.ALL)
        .setHavingSpec(
            AndHavingSpec.of(
                new ExpressionHavingSpec("!(dayOfWeek == 'Monday' && market == 'spot')"),
                new ExpressionHavingSpec("!(dayOfWeek == 'Tuesday' && market == 'total_market')"),
                new ExpressionHavingSpec("!(dayOfWeek == 'Wednesday' && quality == 'premium')")
            )
        );

    builder.setLimitSpec(
        new LimitSpec(
            null, 24,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    "sum_post=$sum(addRowsIndexConstant)"
                ),
                new WindowingSpec(
                    Arrays.asList("dayOfWeek", "market"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs(), "sum_post")
                             .withPartitionExpressions(
                                 PartitionExpression.of("#_ = $sum(_)"),
                                 PartitionExpression.of("concat(_, '.percent') = _ / #_ * 100"))
                             .withAppendValueColumn(true)
                )
            )
        )
    );

    String[] columnNames = new String[]{"dayOfWeek", "market", "sum_post", "sum_post.percent"};

    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("Monday", "total_market", 15315.728393554688, 1.0109193500130786),
        array("Monday", "total_market", 30496.776733398438, 2.0129491017740437),
        array("Monday", "upfront", 45990.10400390625, 3.0355909201307147),
        array("Monday", "upfront", 58144.36151123047, 3.837836414662108),
        array("Tuesday", "spot", 1678.368782043457, 0.11078124622134419),
        array("Tuesday", "spot", 3096.6903228759766, 0.20439799452900861),
        array("Tuesday", "spot", 4764.013374328613, 0.31445016391493497),
        array("Tuesday", "spot", 6300.381149291992, 0.41585859011166926),
        array("Tuesday", "spot", 7684.254570007324, 0.5072015796856195),
        array("Tuesday", "spot", 9123.768661499023, 0.602217148799385),
        array("Tuesday", "spot", 10698.279991149902, 0.7061432520220519),
        array("Tuesday", "spot", 11780.486137390137, 0.7775746006216872),
        array("Tuesday", "spot", 13325.471267700195, 0.8795518180010571),
        array("Tuesday", "upfront", 28486.938369750977, 1.8802891041558518),
        array("Tuesday", "upfront", 40321.75135803223, 2.661449565232874),
        array("Wednesday", "spot", 1816.9095306396484, 0.11992567082344968),
        array("Wednesday", "spot", 3390.9856491088867, 0.22382304779859133),
        array("Wednesday", "spot", 5189.834144592285, 0.3425565944571594),
        array("Wednesday", "spot", 6761.01335144043, 0.4462627598922561),
        array("Wednesday", "spot", 8253.566139221191, 0.5447791644217661),
        array("Wednesday", "spot", 9835.563613891602, 0.6491993929424785),
        array("Wednesday", "spot", 11118.880271911621, 0.7339051025535545),
        array("Wednesday", "spot", 12757.065292358398, 0.8420340072661859),
        array("Wednesday", "total_market", 28521.800888061523, 1.8825902153694163)
    );

    Iterable<Row> results = runQuery(factory, runner, builder.build());
    validate(columnNames, expectedResults, results);
  }

  @Test
  public void testPivotWithGroupingSet()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek", "dayofweek");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setGroupingSets(new GroupingSetSpec.Indices.Builder().add(0, 2).add(0, 1, 2).build())
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            DefaultDimensionSpec.of("market"),
            DefaultDimensionSpec.of("quality"),
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null)
            )
        )
        .setDimFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "premium"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(QueryRunnerTestHelper.addRowsIndexConstant)
        .setGranularity(QueryGranularities.ALL)
        .setHavingSpec(
            AndHavingSpec.of(
                new ExpressionHavingSpec("!(dayOfWeek == 'Monday' && market == 'spot')"),
                new ExpressionHavingSpec("!(dayOfWeek == 'Tuesday' && market == 'total_market')"),
                new ExpressionHavingSpec("!(dayOfWeek == 'Wednesday' && quality == 'premium')")
            )
        );

    String[] columnNames = new String[]{
        "market", "quality", "dayOfWeek", "rows", "index", "addRowsIndexConstant"
    };
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("spot", null, "Friday", 26L, 2880.166572570801, 2907.166572570801),
        array("spot", null, "Saturday", 26L, 2912.418525695801, 2939.418525695801),
        array("spot", null, "Sunday", 26L, 2925.456039428711, 2952.456039428711),
        array("spot", null, "Thursday", 28L, 3121.1823120117188, 3150.1823120117188),
        array("spot", null, "Tuesday", 26L, 2930.384750366211, 2957.384750366211),
        array("spot", null, "Wednesday", 28L, 3111.8562088012695, 3140.8562088012695),
        array("spot", "mezzanine", "Friday", 13L, 1358.3970184326172, 1372.3970184326172),
        array("spot", "mezzanine", "Saturday", 13L, 1398.4330215454102, 1412.4330215454102),
        array("spot", "mezzanine", "Sunday", 13L, 1392.425064086914, 1406.425064086914),
        array("spot", "mezzanine", "Thursday", 14L, 1486.202865600586, 1501.202865600586),
        array("spot", "mezzanine", "Tuesday", 13L, 1369.873420715332, 1383.873420715332),
        array("spot", "mezzanine", "Wednesday", 14L, 1477.5527877807617, 1492.5527877807617),
        array("spot", "premium", "Friday", 13L, 1521.7695541381836, 1535.7695541381836),
        array("spot", "premium", "Saturday", 13L, 1513.9855041503906, 1527.9855041503906),
        array("spot", "premium", "Sunday", 13L, 1533.0309753417969, 1547.0309753417969),
        array("spot", "premium", "Thursday", 14L, 1634.9794464111328, 1649.9794464111328),
        array("spot", "premium", "Tuesday", 13L, 1560.511329650879, 1574.511329650879),
        array("total_market", null, "Friday", 26L, 30173.691955566406, 30200.691955566406),
        array("total_market", null, "Monday", 26L, 30468.776733398438, 30495.776733398438),
        array("total_market", null, "Saturday", 26L, 30940.971740722656, 30967.971740722656),
        array("total_market", null, "Sunday", 26L, 29305.0859375, 29332.0859375),
        array("total_market", null, "Thursday", 28L, 32361.38690185547, 32390.38690185547),
        array("total_market", null, "Wednesday", 28L, 32753.337280273438, 32782.33728027344),
        array("total_market", "mezzanine", "Friday", 13L, 14696.267150878906, 14710.267150878906),
        array("total_market", "mezzanine", "Monday", 13L, 15301.728393554688, 15315.728393554688),
        array("total_market", "mezzanine", "Saturday", 13L, 14784.656677246094, 14798.656677246094),
        array("total_market", "mezzanine", "Sunday", 13L, 14311.892395019531, 14325.892395019531),
        array("total_market", "mezzanine", "Thursday", 14L, 16161.914001464844, 16176.914001464844),
        array("total_market", "mezzanine", "Wednesday", 14L, 15749.735595703125, 15764.735595703125),
        array("total_market", "premium", "Friday", 13L, 15477.4248046875, 15491.4248046875),
        array("total_market", "premium", "Monday", 13L, 15167.04833984375, 15181.04833984375),
        array("total_market", "premium", "Saturday", 13L, 16156.315063476562, 16170.315063476562),
        array("total_market", "premium", "Sunday", 13L, 14993.193542480469, 15007.193542480469),
        array("total_market", "premium", "Thursday", 14L, 16199.472900390625, 16214.472900390625),
        array("upfront", null, "Friday", 26L, 27297.862365722656, 27324.862365722656),
        array("upfront", null, "Monday", 26L, 27619.58477783203, 27646.58477783203),
        array("upfront", null, "Saturday", 26L, 27820.831176757812, 27847.831176757812),
        array("upfront", null, "Sunday", 26L, 24791.22381591797, 24818.22381591797),
        array("upfront", null, "Thursday", 28L, 28562.748779296875, 28591.748779296875),
        array("upfront", null, "Tuesday", 26L, 26968.28009033203, 26995.28009033203),
        array("upfront", null, "Wednesday", 28L, 28985.57501220703, 29014.57501220703),
        array("upfront", "mezzanine", "Friday", 13L, 14354.38134765625, 14368.38134765625),
        array("upfront", "mezzanine", "Monday", 13L, 15479.327270507812, 15493.327270507812),
        array("upfront", "mezzanine", "Saturday", 13L, 13736.503540039062, 13750.503540039062),
        array("upfront", "mezzanine", "Sunday", 13L, 12682.70849609375, 12696.70849609375),
        array("upfront", "mezzanine", "Thursday", 14L, 15828.224243164062, 15843.224243164062),
        array("upfront", "mezzanine", "Tuesday", 13L, 15147.467102050781, 15161.467102050781),
        array("upfront", "mezzanine", "Wednesday", 14L, 14765.832275390625, 14780.832275390625),
        array("upfront", "premium", "Friday", 13L, 12943.481018066406, 12957.481018066406),
        array("upfront", "premium", "Monday", 13L, 12140.257507324219, 12154.257507324219),
        array("upfront", "premium", "Saturday", 13L, 14084.32763671875, 14098.32763671875),
        array("upfront", "premium", "Sunday", 13L, 12108.515319824219, 12122.515319824219),
        array("upfront", "premium", "Thursday", 14L, 12734.524536132812, 12749.524536132812),
        array("upfront", "premium", "Tuesday", 13L, 11820.81298828125, 11834.81298828125)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                )
            )
        )
    );
    columnNames = new String[]{
        "dayOfWeek",
        "spot", "spot-mezzanine", "spot-premium",
        "total_market", "total_market-mezzanine", "total_market-premium",
        "upfront", "upfront-mezzanine", "upfront-premium"
    };

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Monday",
              null, null, null,
              30468.776733398438, 15301.728393554688, 15167.04833984375,
              27619.58477783203, 15479.327270507812, 12140.257507324219),
        array("Tuesday",
              2930.384750366211, 1369.873420715332, 1560.511329650879,
              null, null, null,
              26968.28009033203, 15147.467102050781, 11820.81298828125
        ),
        array(
            "Wednesday",
            3111.8562088012695, 1477.5527877807617, null,
            32753.337280273438, 15749.735595703125, null,
            28985.57501220703, 14765.832275390625, null
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // pivot + partition expression
    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(
                        Arrays.<PivotColumnSpec>asList(
                            PivotColumnSpec.ofExpression("market"),
                            PivotColumnSpec.ofExpression("quality")
                        ), "index"
                    ).withPartitionExpressions(PartitionExpression.of("_ = $sum(_)")
                    ).withAppendValueColumn(true)
                )
            )
        )
    );
    columnNames = new String[]{
        "dayOfWeek",
        "spot-index", "spot-mezzanine-index", "spot-premium-index",
        "total_market-index", "total_market-mezzanine-index", "total_market-premium-index",
        "upfront-index", "upfront-mezzanine-index", "upfront-premium-index"
    };

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("Monday",
              0.0, 0.0, 0.0,
              30468.776733398438, 15301.728393554688, 15167.04833984375,
              27619.58477783203, 15479.327270507812, 12140.257507324219),
        array("Tuesday",
              2930.384750366211, 1369.873420715332, 1560.511329650879,
              30468.776733398438, 15301.728393554688, 15167.04833984375,
              54587.86486816406, 30626.794372558594, 23961.07049560547
        ),
        array(
            "Wednesday",
            6042.2409591674805, 2847.4262084960938, 1560.511329650879,
            63222.114013671875, 31051.463989257812, 15167.04833984375,
            83573.4398803711, 45392.62664794922, 23961.07049560547
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                             .withRowExpressions(
                                 "concat($3, '.percent') = round($3 / $1 * 100, 3)"
                             )
                )
            )
        )
    );
    columnNames = new String[]{
        "dayOfWeek",
        "spot", "spot-mezzanine", "spot-premium",
        "total_market", "total_market-mezzanine", "total_market-premium",
        "upfront", "upfront-mezzanine", "upfront-premium",
        "spot-mezzanine.percent", "spot-premium.percent",
        "total_market-mezzanine.percent", "total_market-premium.percent",
        "upfront-mezzanine.percent", "upfront-premium.percent"
    };
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null,
            30468.776733398438, 15301.728393554688, 15167.04833984375,
            27619.58477783203, 15479.327270507812, 12140.257507324219,
            null, null, 50.221, 49.779, 56.045, 43.955
        ),
        array(
            "Tuesday",
            2930.384750366211, 1369.873420715332, 1560.511329650879,
            null, null, null,
            26968.28009033203, 15147.467102050781, 11820.81298828125,
            46.747, 53.253, null, null, 56.168, 43.832
        ),
        array(
            "Wednesday",
            3111.8562088012695, 1477.5527877807617, null,
            32753.337280273438, 15749.735595703125, null,
            28985.57501220703, 14765.832275390625, null,
            47.481, null, 48.086, null, 50.942, null)
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // multi-valued (no appending)
    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index", "rows")
                             .withRowExpressions( "concat($3, '.percent') = round(100f * $3 / $1, 3)")
                )
            )
        )
    );
    columnNames = new String[]{
        "dayOfWeek",
        "spot", "spot-mezzanine", "spot-premium",
        "total_market", "total_market-mezzanine", "total_market-premium",
        "upfront", "upfront-mezzanine", "upfront-premium",
        "spot-mezzanine.percent", "spot-premium.percent",
        "total_market-mezzanine.percent", "total_market-premium.percent",
        "upfront-mezzanine.percent", "upfront-premium.percent"
    };

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null,
            list(30468.776733398438, 26L), list(15301.728393554688, 13L), list(15167.04833984375, 13L),
            list(27619.58477783203, 26L), list(15479.327270507812, 13L), list(12140.257507324219, 13L),
            null, null, list(50.221, 50.0),
            list(49.779, 50.0), list(56.045, 50.0), list(43.955, 50.0)
        ),
        array(
            "Tuesday",
            list(2930.384750366211, 26L), list(1369.873420715332, 13L), list(1560.511329650879, 13L),
            null, null, null,
            list(26968.28009033203, 26L), list(15147.467102050781, 13L), list(11820.81298828125, 13L),
            list(46.747, 50.0), list(53.253, 50.0), null,
            null, list(56.168, 50.0), list(43.832, 50.0)
        ),
        array(
            "Wednesday",
            list(3111.8562088012695, 28L), list(1477.5527877807617, 14L), null,
            list(32753.337280273438, 28L), list(15749.735595703125, 14L), null,
            list(28985.57501220703, 28L), list(14765.832275390625, 14L), null,
            list(47.481, 50.0), null, list(48.086, 50.0), null, list(50.942, 50.0), null
        )
    );
    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // multi-valued (appending)
    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index", "rows")
                             .withRowExpressions( "concat($3, '.percent') = round(100f * $3 / $1, 3)")
                             .withAppendValueColumn(true)
                )
            )
        )
    );
    columnNames = new String[]{
        "dayOfWeek",
        "spot-index", "spot-rows",
        "spot-mezzanine-index", "spot-mezzanine-rows", "spot-premium-index", "spot-premium-rows",
        "total_market-index", "total_market-rows",
        "total_market-mezzanine-index", "total_market-mezzanine-rows", "total_market-premium-index", "total_market-premium-rows",
        "upfront-index", "upfront-rows",
        "upfront-mezzanine-index", "upfront-mezzanine-rows", "upfront-premium-index", "upfront-premium-rows",
        "spot-mezzanine-index.percent", "spot-mezzanine-rows.percent", "spot-premium-index.percent", "spot-premium-rows.percent",
        "total_market-mezzanine-index.percent", "total_market-mezzanine-rows.percent", "total_market-premium-index.percent", "total_market-premium-rows.percent",
        "upfront-mezzanine-index.percent", "upfront-mezzanine-rows.percent", "upfront-premium-index.percent", "upfront-premium-rows.percent",
    };
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null,
            null, null, null, null,
            30468.776733398438, 26L,
            15301.728393554688, 13L, 15167.04833984375, 13L,
            27619.58477783203, 26L,
            15479.327270507812, 13L, 12140.257507324219, 13L,
            null, null, null, null,
            50.221, 50.0, 49.779, 50.0,
            56.045, 50.0, 43.955, 50.0),
        array(
            "Tuesday",
            2930.384750366211, 26L,
            1369.873420715332, 13L, 1560.511329650879, 13L,
            null, null,
            null, null, null, null,
            26968.28009033203, 26L,
            15147.467102050781, 13L, 11820.81298828125, 13L,
            46.747, 50.0, 53.253, 50.0,
            null, null, null, null,
              56.168, 50.0, 43.832, 50.0),
        array(
            "Wednesday",
            3111.8562088012695, 28L,
            1477.5527877807617, 14L, null, null,
            32753.337280273438, 28L,
            15749.735595703125, 14L, null, null,
            28985.57501220703, 28L,
            14765.832275390625, 14L, null, null,
            47.481, 50.0, null, null,
            48.086, 50.0, null, null,
            50.942, 50.0, null, null)
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, builder.build());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  @Test
  public void testBySegmentResults()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toBrokerRunner(runner, segmentCount, exec);

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }


  @Test
  public void testBySegmentResultsUnOptimizedDimextraction()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(
                        new MapLookupExtractor(
                            ImmutableMap.<Object, String>of(
                                "mezzanine",
                                "mezzanine0"
                            ),
                            false
                        ), false, null, false,
                        false
                    ),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toBrokerRunner(runner, segmentCount, exec);

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResultsOptimizedDimextraction()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(
                        new MapLookupExtractor(
                            ImmutableMap.<Object, String>of(
                                "mezzanine",
                                "mezzanine0"
                            ),
                            false
                        ), false, null, true,
                        false
                    ),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toBrokerRunner(runner, segmentCount, exec);

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }

  // Extraction Filters testing

  @Test
  public void testGroupByWithExtractionDimFilter()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("automotive", "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put("business", "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put("mezzanine", "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put("news", "automotiveAndBusinessAndNewsAndMezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    List<DimFilter> dimFilters = Lists.<DimFilter>newArrayList(
        new ExtractionDimFilter("quality", "automotiveAndBusinessAndNewsAndMezzanine", lookupExtractionFn, null),
        new SelectorDimFilter("quality", "entertainment", null),
        new SelectorDimFilter("quality", "health", null),
        new SelectorDimFilter("quality", "premium", null),
        new SelectorDimFilter("quality", "technology", null),
        new SelectorDimFilter("quality", "travel", null)
    );

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new DefaultDimensionSpec(
                    "quality",
                    "alias"
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(Druids.newOrDimFilterBuilder().fields(dimFilters).build())
        .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

  }

  @Test
  public void testGroupByWithExtractionDimFilterCaseMappingValueIsNullOrEmpty()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("automotive", "automotive0");
    extractionMap.put("business", "business0");
    extractionMap.put("entertainment", "entertainment0");
    extractionMap.put("health", "health0");
    extractionMap.put("mezzanine", null);
    extractionMap.put("news", "");
    extractionMap.put("premium", "premium0");
    extractionMap.put("technology", "technology0");
    extractionMap.put("travel", "travel0");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new DefaultDimensionSpec(
                    "quality",
                    "alias"
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(new ExtractionDimFilter("quality", "", lookupExtractionFn, null))
        .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithExtractionDimFilterWhenSearchValueNotInTheMap()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new DefaultDimensionSpec(
                    "quality",
                    "alias"
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(
            new ExtractionDimFilter(
                "quality",
                "NOT_THERE",
                lookupExtractionFn,
                null
            )
        ).build();
    List<Row> expectedResults = Arrays.asList();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithExtractionDimFilterKeyisNull()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("", "NULLorEMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new DefaultDimensionSpec(
                    "null_column",
                    "alias"
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(
            new ExtractionDimFilter(
                "null_column",
                "NULLorEMPTY",
                lookupExtractionFn,
                null
            )
        ).build();

    final String[] columns = new String[]{"__time", "alias", "rows", "idx"};
    final List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", null, 13L, 6619L),
        array("2011-04-02", null, 13L, 5827L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithAggregatorFilterAndExtractionFunction()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("automotive", "automotive0");
    extractionMap.put("business", "business0");
    extractionMap.put("entertainment", "entertainment0");
    extractionMap.put("health", "health0");
    extractionMap.put("mezzanine", "mezzanineANDnews");
    extractionMap.put("news", "mezzanineANDnews");
    extractionMap.put("premium", "premium0");
    extractionMap.put("technology", "technology0");
    extractionMap.put("travel", "travel0");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "missing", true, false);
    DimFilter filter = new ExtractionDimFilter("quality", "mezzanineANDnews", lookupExtractionFn, null);
    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new DefaultDimensionSpec(
                    "quality",
                    "alias"
                )
            )
        )
        .setAggregatorSpecs(
            new FilteredAggregatorFactory(QueryRunnerTestHelper.rowsCount, filter),
            new FilteredAggregatorFactory(new LongSumAggregatorFactory("idx", "index"), filter),
            new FilteredAggregatorFactory(new LongSumAggregatorFactory("idx2", "index"), new MathExprFilter("1 == 0")),
            new GenericSumAggregatorFactory("idx3", "index", null, "in(quality, 'automotive', 'business')", "long")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "alias", "rows", "idx", "idx2", "idx3"},
        new Object[]{"2011-04-01", "automotive", 0L, 0L, 0L, 135L},
        new Object[]{"2011-04-01", "business", 0L, 0L, 0L, 118L},
        new Object[]{"2011-04-01", "entertainment", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-01", "health", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-01", "mezzanine", 3L, 2870L, 0L, 0L},
        new Object[]{"2011-04-01", "news", 1L, 121L, 0L, 0L},
        new Object[]{"2011-04-01", "premium", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-01", "technology", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-01", "travel", 0L, 0L, 0L, 0L},

        new Object[]{"2011-04-02", "automotive", 0L, 0L, 0L, 147L},
        new Object[]{"2011-04-02", "business", 0L, 0L, 0L, 112L},
        new Object[]{"2011-04-02", "entertainment", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-02", "health", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-02", "mezzanine", 3L, 2447L, 0L, 0L},
        new Object[]{"2011-04-02", "news", 1L, 114L, 0L, 0L},
        new Object[]{"2011-04-02", "premium", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-02", "technology", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-02", "travel", 0L, 0L, 0L, 0L}
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withDimFilter(new RegexDimFilter("quality", "^[a-m].*$", null));

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "alias", "rows", "idx", "idx2", "idx3"},
        new Object[]{"2011-04-01", "automotive", 0L, 0L, 0L, 135L},
        new Object[]{"2011-04-01", "business", 0L, 0L, 0L, 118L},
        new Object[]{"2011-04-01", "entertainment", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-01", "health", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-01", "mezzanine", 3L, 2870L, 0L, 0L},

        new Object[]{"2011-04-02", "automotive", 0L, 0L, 0L, 147L},
        new Object[]{"2011-04-02", "business", 0L, 0L, 0L, 112L},
        new Object[]{"2011-04-02", "entertainment", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-02", "health", 0L, 0L, 0L, 0L},
        new Object[]{"2011-04-02", "mezzanine", 3L, 2447L, 0L, 0L}
    );

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithExtractionDimFilterOptimazitionManyToOne()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("mezzanine", "newsANDmezzanine");
    extractionMap.put("news", "newsANDmezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(
            "quality",
            "alias"
        )))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(new ExtractionDimFilter(
            "quality",
            "newsANDmezzanine",
            lookupExtractionFn,
            null
        ))
        .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithExtractionDimFilterNullDims()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(
            "null_column",
            "alias"
        )))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(new ExtractionDimFilter(
            "null_column",
            "EMPTY",
            lookupExtractionFn,
            null
        )).build();

    final String[] columns = new String[]{"__time", "alias", "rows", "idx"};
    final List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", null, 13L, 6619L),
        array("2011-04-02", null, 13L, 5827L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testBySegmentResultsWithAllFiltersWithExtractionFns()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }

    String extractionJsFn = "function(str) { return 'super-' + str; }";
    String jsFn = "function(x) { return(x === 'super-mezzanine') }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getDefault());

    List<DimFilter> superFilterList = new ArrayList<>();
    superFilterList.add(new SelectorDimFilter("quality", "super-mezzanine", extractionFn));
    superFilterList.add(new InDimFilter(
        "quality",
        Arrays.asList("not-super-mezzanine", "FOOBAR", "super-mezzanine"),
        extractionFn
    ));
    superFilterList.add(new BoundDimFilter(
        "quality",
        "super-mezzanine",
        "super-mezzanine",
        false,
        false,
        true,
        extractionFn
    ));
    superFilterList.add(new RegexDimFilter("quality", "super-mezzanine", extractionFn));
    superFilterList.add(new SearchQueryDimFilter(
        "quality",
        new ContainsSearchQuerySpec("super-mezzanine", true),
        extractionFn
    ));
    superFilterList.add(new JavaScriptDimFilter("quality", jsFn, extractionFn, JavaScriptConfig.getDefault()));
    DimFilter superFilter = new AndDimFilter(superFilterList);

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(superFilter)
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toBrokerRunner(runner, segmentCount, exec);

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }

  @Test
  public void testGroupByWithAllFiltersOnNullDimsWithExtractionFns()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");
    extractionMap.put(null, "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn extractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    String jsFn = "function(x) { return(x === 'EMPTY') }";

    List<DimFilter> superFilterList = new ArrayList<>();
    superFilterList.add(new SelectorDimFilter("null_column", "EMPTY", extractionFn));
    superFilterList.add(new InDimFilter("null_column", Arrays.asList("NOT-EMPTY", "FOOBAR", "EMPTY"), extractionFn));
    superFilterList.add(new BoundDimFilter("null_column", "EMPTY", "EMPTY", false, false, true, extractionFn));
    superFilterList.add(new RegexDimFilter("null_column", "EMPTY", extractionFn));
    superFilterList.add(new SearchQueryDimFilter(
        "null_column",
        new ContainsSearchQuerySpec("EMPTY", true),
        extractionFn
    ));
    superFilterList.add(new JavaScriptDimFilter("null_column", jsFn, extractionFn, JavaScriptConfig.getDefault()));
    DimFilter superFilter = new AndDimFilter(superFilterList);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(
            "null_column",
            "alias"
        )))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(superFilter).build();

    final String[] columns = new String[]{"__time", "alias", "rows", "idx"};
    final List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", null, 13L, 6619L),
        array("2011-04-02", null, 13L, 5827L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByCardinalityAggWithExtractionFn()
  {
    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getDefault());

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("market", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new CardinalityAggregatorFactory(
                    "numVals",
                    null,
                    ImmutableList.<DimensionSpec>of(new ExtractionDimensionSpec(
                        QueryRunnerTestHelper.qualityDimension,
                        QueryRunnerTestHelper.qualityDimension,
                        helloFn
                    )),
                    null,
                    false,
                    false
                )
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "spot", "rows", 9L, "numVals", 1.0002442201269182d),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "total_market", "rows", 2L, "numVals", 1.0002442201269182d),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "upfront", "rows", 2L, "numVals", 1.0002442201269182d),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "spot", "rows", 9L, "numVals", 1.0002442201269182d),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "total_market", "rows", 2L, "numVals", 1.0002442201269182d),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "upfront", "rows", 2L, "numVals", 1.0002442201269182d)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
