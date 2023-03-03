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
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Sequence;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularities;
import io.druid.js.JavaScriptConfig;
import io.druid.math.expr.Parser;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.PostAggregationsPostProcessor;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryException;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.RowToArray;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AverageAggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
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
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.groupby.orderby.PartitionExpression;
import io.druid.query.groupby.orderby.PivotColumnSpec;
import io.druid.query.groupby.orderby.PivotSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.timeline.DataSegment;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.NaN;

/**
 */
@RunWith(Parameterized.class)
public class GroupByQueryRunnerGenericTest extends GroupByQueryRunnerTestHelper
{
  static {
    Parser.register(ModuleBuiltinFunctions.class);
  }

  @Parameterized.Parameters(name = "{0}:{1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return cartesian(Arrays.asList(TestIndex.DS_NAMES), Arrays.asList(false, true));
  }

  private final String dataSource;

  public GroupByQueryRunnerGenericTest(String dataSource, boolean sortParallel)
  {
    this.dataSource = dataSource;
    TestIndex.segmentWalker.getQueryConfig().getGroupBy().setUseParallelSort(sortParallel);
  }

  @SuppressWarnings("unchecked")
  private Sequence<Result> runSegmentQuery(Query query)
  {
    return query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap());
  }

  private String getSegmentId(Interval interval)
  {
    return new DataSegment(dataSource, interval, "0", null, null, null, null, null, 0).getIdentifier();
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new GenericSumAggregatorFactory("idx2", "indexDecimal", ValueDesc.DECIMAL)
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx", "idx2"),
        array("2011-04-01T00:00:00.000Z", "automotive", 1, 135, 135),
        array("2011-04-01T00:00:00.000Z", "business", 1, 118, 118),
        array("2011-04-01T00:00:00.000Z", "entertainment", 1, 158, 158),
        array("2011-04-01T00:00:00.000Z", "health", 1, 120, 120),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 3, 2870, 2870),
        array("2011-04-01T00:00:00.000Z", "news", 1, 121, 121),
        array("2011-04-01T00:00:00.000Z", "premium", 3, 2900, 2900),
        array("2011-04-01T00:00:00.000Z", "technology", 1, 78, 78),
        array("2011-04-01T00:00:00.000Z", "travel", 1, 119, 119),
        array("2011-04-02T00:00:00.000Z", "automotive", 1, 147, 147),
        array("2011-04-02T00:00:00.000Z", "business", 1, 112, 112),
        array("2011-04-02T00:00:00.000Z", "entertainment", 1, 166, 166),
        array("2011-04-02T00:00:00.000Z", "health", 1, 113, 113),
        array("2011-04-02T00:00:00.000Z", "mezzanine", 3, 2447, 2447),
        array("2011-04-02T00:00:00.000Z", "news", 1, 114, 114),
        array("2011-04-02T00:00:00.000Z", "premium", 3, 2505, 2505),
        array("2011-04-02T00:00:00.000Z", "technology", 1, 97, 97),
        array("2011-04-02T00:00:00.000Z", "travel", 1, 126, 126)
    );

    query = query.withOutputColumns(Arrays.asList("alias", "rows"));

    String[] columnNames = array("__time", "alias", "rows");
    Object[][] objects = new Object[][] {
        array("2011-04-01", "automotive", 1),
        array("2011-04-01", "business", 1),
        array("2011-04-01", "entertainment", 1),
        array("2011-04-01", "health", 1),
        array("2011-04-01", "mezzanine", 3),
        array("2011-04-01", "news", 1),
        array("2011-04-01", "premium", 3),
        array("2011-04-01", "technology", 1),
        array("2011-04-01", "travel", 1),
        array("2011-04-02", "automotive", 1),
        array("2011-04-02", "business", 1),
        array("2011-04-02", "entertainment", 1),
        array("2011-04-02", "health", 1),
        array("2011-04-02", "mezzanine", 3),
        array("2011-04-02", "news", 1),
        array("2011-04-02", "premium", 3),
        array("2011-04-02", "technology", 1),
        array("2011-04-02", "travel", 1)
    };

    List<Row> results = runQuery(query, true);
    List<Row>expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // to array
    query = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(Query.POST_PROCESSING, new RowToArray(Arrays.asList(columnNames)))
    );
    List<Object[]> array = runRawQuery(query);
    Assert.assertEquals(objects.length, array.size());
    for (int i = 0; i < objects.length; i++) {
      for (int j = 0; j < objects[i].length; j++) {
        if (columnNames[j].equals(Row.TIME_COLUMN_NAME)) {
          Assert.assertEquals(new DateTime(objects[i][j]).getMillis(), array.get(i)[j]);
        } else {
          Assert.assertEquals(objects[i][j], array.get(i)[j]);
        }
      }
    }

    // add post-processing
    query = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(
            Query.POST_PROCESSING,
            new LimitingPostProcessor(
                LimitSpecs.of(10, OrderByColumnSpec.desc("alias")),
                Suppliers.ofInstance(new GroupByQueryConfig())
            )
        )
    );

    validate(
        query,
        false,
        columnNames,
        array("2011-04-01T00:00:00.000Z", "travel", 1),
        array("2011-04-01T00:00:00.000Z", "technology", 1),
        array("2011-04-01T00:00:00.000Z", "premium", 3),
        array("2011-04-01T00:00:00.000Z", "news", 1),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 3),
        array("2011-04-01T00:00:00.000Z", "health", 1),
        array("2011-04-01T00:00:00.000Z", "entertainment", 1),
        array("2011-04-01T00:00:00.000Z", "business", 1),
        array("2011-04-01T00:00:00.000Z", "automotive", 1),
        array("2011-04-02T00:00:00.000Z", "travel", 1)
    );

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
    query = query.withOutputColumns(Arrays.asList("alias", "rows", "daily"));

    validate(
        query,
        array("__time", "alias", "rows", "daily"),
        array("2011-04-01T00:00:00.000Z", "automotive", 1, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "business", 1, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "entertainment", 1, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "health", 1, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 3, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "news", 1, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "premium", 3, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "technology", 1, "Apr 01"),
        array("2011-04-01T00:00:00.000Z", "travel", 1, "Apr 01"),
        array("2011-04-02T00:00:00.000Z", "automotive", 1, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "business", 1, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "entertainment", 1, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "health", 1, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "mezzanine", 3, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "news", 1, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "premium", 3, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "technology", 1, "Apr 02"),
        array("2011-04-02T00:00:00.000Z", "travel", 1, "Apr 02")
    );
  }

  @Test
  public void testCountWithField()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setAggregatorSpecs(
            CountAggregatorFactory.of("rows"),
            CountAggregatorFactory.of("rows_nc1", "partial_null_column"),
            CountAggregatorFactory.predicate("rows_nc2", "partial_null_column == 'value'"),
            CountAggregatorFactory.predicate("rows_nc3", "partial_null_column >= 'value'")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "rows", "rows_nc1", "rows_nc2", "rows_nc3"),
        array("2011-04-01", 13, 2, 2, 2),
        array("2011-04-02", 13, 2, 2, 2)
    );
  }

  @Test
  public void testGroupByRelay()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(fullOnInterval)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                RelayAggregatorFactory.min("index_min", "index"),
                RelayAggregatorFactory.max("index_max", "index"),
                RelayAggregatorFactory.timeMin("index_timemin", "index"),
                RelayAggregatorFactory.timeMax("index_timemax", "index"),
                RelayAggregatorFactory.first("index_first", "index"),
                RelayAggregatorFactory.last("index_last", "index")
            )
        )
        .build();

    validate(
        query,
        array("__time", "rows", "index_min", "index_max", "index_timemin", "index_timemax", "index_first", "index_last"),
        array("1970-01-01", 93, 71.31593322753906, 277.2735290527344, 100.0, 106.793701171875, 100.0, 106.793701171875),
        array("1970-01-01", 93, 92.5374984741211, 135.1832733154297, 100.0, 94.4697494506836, 100.0, 94.4697494506836),
        array("1970-01-01", 93, 84.71052551269531, 193.78756713867188, 100.0, 135.10919189453125, 100.0, 135.10919189453125),
        array("1970-01-01", 93, 85.06978607177734, 189.38595581054688, 100.0, 99.59690856933594, 100.0, 99.59690856933594),
        array("1970-01-01", 279, 91.27055358886719, 1870.06103515625, 100.0, 92.78276062011719, 100.0, 962.731201171875),
        array("1970-01-01", 93, 96.0313720703125, 142.97296142578125, 100.0, 97.8597640991211, 100.0, 97.8597640991211),
        array("1970-01-01", 279, 99.2845230102539, 1862.7379150390625, 100.0, 120.50816345214844, 100.0, 780.27197265625),
        array("1970-01-01", 93, 59.02102279663086, 119.85015106201172, 100.0, 89.64623260498047, 100.0, 89.64623260498047),
        array("1970-01-01", 93, 100.0, 158.73936462402344, 100.0, 120.29034423828125, 100.0, 120.29034423828125)
    );
  }

  @Test
  public void testGroupByOnExprDim()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(ExpressionDimensionSpec.of("replace(partial_null_column, 'v', 'x')", "x"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "x", "rows", "idx"),
        array("2011-04-01T00:00:00.000Z", null, 11, 3938),
        array("2011-04-01T00:00:00.000Z", "xalue", 2, 2681),
        array("2011-04-02T00:00:00.000Z", null, 11, 3634),
        array("2011-04-02T00:00:00.000Z", "xalue", 2, 2193)
    );
  }

  @Test
  public void testGroupByLocalLimit()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(fullOnInterval)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            new CountAggregatorFactory("rows"),
            new LongSumAggregatorFactory("idx", "index")
        )
        .setLimitSpec(LimitSpecs.of(5, OrderByColumnSpec.desc("idx")))
        .setGranularity(Granularities.ALL)
        .build();

    List<Row> results;
    List<Row> expectedResults;
    String[] columnNames = {"__time", "alias", "rows", "idx"};

    expectedResults = createExpectedRows(
        columnNames,
        array("1970-01-01T00:00:00.000Z", "mezzanine", 279, 217586),
        array("1970-01-01T00:00:00.000Z", "premium", 279, 210722),
        array("1970-01-01T00:00:00.000Z", "automotive", 93, 12226),
        array("1970-01-01T00:00:00.000Z", "entertainment", 93, 12038),
        array("1970-01-01T00:00:00.000Z", "travel", 93, 11138)
    );
    results = runQuery(query, false);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    LimitSpec limitSpec = new LimitSpec(
        OrderByColumnSpec.descending("idx"), 5, OrderedLimitSpec.of(2), null, null, null
    );
    expectedResults = createExpectedRows(
        columnNames,
        array("1970-01-01T00:00:00.000Z", "mezzanine", 279, 217586),
        array("1970-01-01T00:00:00.000Z", "premium", 279, 210722)
    );
    results = runQuery(query.withLimitSpec(limitSpec), false);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByGroupingSet()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setGroupingSets(
            new GroupingSetSpec.Names.Builder().add().add("quality").add("alias").add("quality", "alias").build()
        )
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .build();

    validate(
        query,
        array("__time", "quality", "alias", "rows", "idx"),
        array("2011-04-01T00:00:00.000Z", null, null, 52, 24892),
        array("2011-04-01T00:00:00.000Z", null, "a", 2, 282),
        array("2011-04-01T00:00:00.000Z", null, "b", 2, 230),
        array("2011-04-01T00:00:00.000Z", null, "e", 2, 324),
        array("2011-04-01T00:00:00.000Z", null, "h", 2, 233),
        array("2011-04-01T00:00:00.000Z", null, "m", 6, 5317),
        array("2011-04-01T00:00:00.000Z", null, "n", 2, 235),
        array("2011-04-01T00:00:00.000Z", null, "p", 6, 5405),
        array("2011-04-01T00:00:00.000Z", null, "preferred", 26, 12446),
        array("2011-04-01T00:00:00.000Z", null, "t", 4, 420),
        array("2011-04-01T00:00:00.000Z", "automotive", null, 4, 564),
        array("2011-04-01T00:00:00.000Z", "automotive", "a", 2, 282),
        array("2011-04-01T00:00:00.000Z", "automotive", "preferred", 2, 282),
        array("2011-04-01T00:00:00.000Z", "business", null, 4, 460),
        array("2011-04-01T00:00:00.000Z", "business", "b", 2, 230),
        array("2011-04-01T00:00:00.000Z", "business", "preferred", 2, 230),
        array("2011-04-01T00:00:00.000Z", "entertainment", null, 4, 648),
        array("2011-04-01T00:00:00.000Z", "entertainment", "e", 2, 324),
        array("2011-04-01T00:00:00.000Z", "entertainment", "preferred", 2, 324),
        array("2011-04-01T00:00:00.000Z", "health", null, 4, 466),
        array("2011-04-01T00:00:00.000Z", "health", "h", 2, 233),
        array("2011-04-01T00:00:00.000Z", "health", "preferred", 2, 233),
        array("2011-04-01T00:00:00.000Z", "mezzanine", null, 12, 10634),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "m", 6, 5317),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "preferred", 6, 5317),
        array("2011-04-01T00:00:00.000Z", "news", null, 4, 470),
        array("2011-04-01T00:00:00.000Z", "news", "n", 2, 235),
        array("2011-04-01T00:00:00.000Z", "news", "preferred", 2, 235),
        array("2011-04-01T00:00:00.000Z", "premium", null, 12, 10810),
        array("2011-04-01T00:00:00.000Z", "premium", "p", 6, 5405),
        array("2011-04-01T00:00:00.000Z", "premium", "preferred", 6, 5405),
        array("2011-04-01T00:00:00.000Z", "technology", null, 4, 350),
        array("2011-04-01T00:00:00.000Z", "technology", "preferred", 2, 175),
        array("2011-04-01T00:00:00.000Z", "technology", "t", 2, 175),
        array("2011-04-01T00:00:00.000Z", "travel", null, 4, 490),
        array("2011-04-01T00:00:00.000Z", "travel", "preferred", 2, 245),
        array("2011-04-01T00:00:00.000Z", "travel", "t", 2, 245)
    );
  }

  @Test
  public void testGroupByGroupingSetRollup()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setGroupingSets(new GroupingSetSpec.Rollup())
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        )
        .setAggregatorSpecs(
            rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.ALL)
        .build();

    validate(
        query,
        array("__time", "quality", "alias", "rows", "idx"),
        array("2011-04-01T00:00:00.000Z", null, null, 52, 24892),
        array("2011-04-01T00:00:00.000Z", "automotive", null, 4, 564),
        array("2011-04-01T00:00:00.000Z", "automotive", "a", 2, 282),
        array("2011-04-01T00:00:00.000Z", "automotive", "preferred", 2, 282),
        array("2011-04-01T00:00:00.000Z", "business", null, 4, 460),
        array("2011-04-01T00:00:00.000Z", "business", "b", 2, 230),
        array("2011-04-01T00:00:00.000Z", "business", "preferred", 2, 230),
        array("2011-04-01T00:00:00.000Z", "entertainment", null, 4, 648),
        array("2011-04-01T00:00:00.000Z", "entertainment", "e", 2, 324),
        array("2011-04-01T00:00:00.000Z", "entertainment", "preferred", 2, 324),
        array("2011-04-01T00:00:00.000Z", "health", null, 4, 466),
        array("2011-04-01T00:00:00.000Z", "health", "h", 2, 233),
        array("2011-04-01T00:00:00.000Z", "health", "preferred", 2, 233),
        array("2011-04-01T00:00:00.000Z", "mezzanine", null, 12, 10634),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "m", 6, 5317),
        array("2011-04-01T00:00:00.000Z", "mezzanine", "preferred", 6, 5317),
        array("2011-04-01T00:00:00.000Z", "news", null, 4, 470),
        array("2011-04-01T00:00:00.000Z", "news", "n", 2, 235),
        array("2011-04-01T00:00:00.000Z", "news", "preferred", 2, 235),
        array("2011-04-01T00:00:00.000Z", "premium", null, 12, 10810),
        array("2011-04-01T00:00:00.000Z", "premium", "p", 6, 5405),
        array("2011-04-01T00:00:00.000Z", "premium", "preferred", 6, 5405),
        array("2011-04-01T00:00:00.000Z", "technology", null, 4, 350),
        array("2011-04-01T00:00:00.000Z", "technology", "preferred", 2, 175),
        array("2011-04-01T00:00:00.000Z", "technology", "t", 2, 175),
        array("2011-04-01T00:00:00.000Z", "travel", null, 4, 490),
        array("2011-04-01T00:00:00.000Z", "travel", "preferred", 2, 245),
        array("2011-04-01T00:00:00.000Z", "travel", "t", 2, 245)
    );
  }

  @Test
  public void testGroupByOnMetric()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval(new Interval("2011-01-12/2011-01-13"))
        .setDimensions(DefaultDimensionSpec.toSpec("index"))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(rowsCount))
        .build();

    validate(
        query,
        array("__time", "index", "rows"),
        array("2011-01-12", 100.0d, 9),
        array("2011-01-12", 800.0d, 2),
        array("2011-01-12", 1000.0d, 2)
    );
  }

  @Test
  public void testGroupByOnTime()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval(new Interval("2011-01-12/2011-01-14"))
        .setDimensions(new DefaultDimensionSpec(Column.TIME_COLUMN_NAME, "time"))
        .setAggregatorSpecs(rowsCount)
        .build();

    validate(
        query,
        array("__time", "time", "rows"),
        array("2011-01-12", 1294790400000L, 13),
        array("2011-01-12", 1294876800000L, 13)
    );
  }

  @Test
  public void testGroupByOnTimeVC()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval(new Interval("2011-01-12/2011-01-31"))
        .setVirtualColumns(new ExprVirtualColumn("bucketStart(__time, 'WEEK')", "_week"))
        .setDimensions(DefaultDimensionSpec.toSpec("_week"))
        .setAggregatorSpecs(rowsCount)
        .setPostAggregatorSpecs(new MathPostAggregator("week = time_format(_week, out.format='ww xxxx')"))
        .build();

    validate(
        query,
        array("__time", "week", "rows"),
        array("2011-01-12", "02 2011", 65),
        array("2011-01-12", "03 2011", 78),
        array("2011-01-12", "04 2011", 91)
    );
  }

  @Test
  public void testMultiValueDimension()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(new DefaultDimensionSpec("placementish", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "a", 2, 282),
        array("2011-04-01", "b", 2, 230),
        array("2011-04-01", "e", 2, 324),
        array("2011-04-01", "h", 2, 233),
        array("2011-04-01", "m", 6, 5317),
        array("2011-04-01", "n", 2, 235),
        array("2011-04-01", "p", 6, 5405),
        array("2011-04-01", "preferred", 26, 12446),
        array("2011-04-01", "t", 4, 420)
    );
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValue1()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("placementish", "alias"),
            new DefaultDimensionSpec("quality", "quality")
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .build();

    validate(
        query,
        array("__time", "quality", "alias", "rows", "idx"),
        array("2011-04-01", "automotive", "a", 2, 282),
        array("2011-04-01", "business", "b", 2, 230),
        array("2011-04-01", "entertainment", "e", 2, 324),
        array("2011-04-01", "health", "h", 2, 233),
        array("2011-04-01", "mezzanine", "m", 6, 5317),
        array("2011-04-01", "news", "n", 2, 235),
        array("2011-04-01", "premium", "p", 6, 5405),
        array("2011-04-01", "automotive", "preferred", 2, 282),
        array("2011-04-01", "business", "preferred", 2, 230),
        array("2011-04-01", "entertainment", "preferred", 2, 324),
        array("2011-04-01", "health", "preferred", 2, 233),
        array("2011-04-01", "mezzanine", "preferred", 6, 5317),
        array("2011-04-01", "news", "preferred", 2, 235),
        array("2011-04-01", "premium", "preferred", 6, 5405),
        array("2011-04-01", "technology", "preferred", 2, 175),
        array("2011-04-01", "travel", "preferred", 2, 245),
        array("2011-04-01", "technology", "t", 2, 175),
        array("2011-04-01", "travel", "t", 2, 245)
    );
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValueDifferentOrder()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .build();

    validate(
        query,
        array("__time", "quality", "alias", "rows", "idx"),
        array("2011-04-01", "automotive", "a", 2, 282),
        array("2011-04-01", "automotive", "preferred", 2, 282),
        array("2011-04-01", "business", "b", 2, 230),
        array("2011-04-01", "business", "preferred", 2, 230),
        array("2011-04-01", "entertainment", "e", 2, 324),
        array("2011-04-01", "entertainment", "preferred", 2, 324),
        array("2011-04-01", "health", "h", 2, 233),
        array("2011-04-01", "health", "preferred", 2, 233),
        array("2011-04-01", "mezzanine", "m", 6, 5317),
        array("2011-04-01", "mezzanine", "preferred", 6, 5317),
        array("2011-04-01", "news", "n", 2, 235),
        array("2011-04-01", "news", "preferred", 2, 235),
        array("2011-04-01", "premium", "p", 6, 5405),
        array("2011-04-01", "premium", "preferred", 6, 5405),
        array("2011-04-01", "technology", "preferred", 2, 175),
        array("2011-04-01", "technology", "t", 2, 175),
        array("2011-04-01", "travel", "preferred", 2, 245),
        array("2011-04-01", "travel", "t", 2, 245)
    );
  }

  @Test(expected = QueryException.class)
  public void testGroupByMaxRowsLimitContextOverrid()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .setContext(ImmutableMap.<String, Object>of("maxResults", 1))
        .build();

    runQuery(query);
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
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive0", 1, 135),
        array("2011-04-01", "business0", 1, 118),
        array("2011-04-01", "entertainment0", 1, 158),
        array("2011-04-01", "health0", 1, 120),
        array("2011-04-01", "mezzanine0", 3, 2870),
        array("2011-04-01", "news0", 1, 121),
        array("2011-04-01", "premium0", 3, 2900),
        array("2011-04-01", "technology0", 1, 78),
        array("2011-04-01", "travel0", 1, 119),
        array("2011-04-02", "automotive0", 1, 147),
        array("2011-04-02", "business0", 1, 112),
        array("2011-04-02", "entertainment0", 1, 166),
        array("2011-04-02", "health0", 1, 113),
        array("2011-04-02", "mezzanine0", 3, 2447),
        array("2011-04-02", "news0", 1, 114),
        array("2011-04-02", "premium0", 3, 2505),
        array("2011-04-02", "technology0", 1, 97),
        array("2011-04-02", "travel0", 1, 126)
    );
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
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, false, false),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive0", 1, 135),
        array("2011-04-01", "business0", 1, 118),
        array("2011-04-01", "entertainment0", 1, 158),
        array("2011-04-01", "health0", 1, 120),
        array("2011-04-01", "mezzanine0", 3, 2870),
        array("2011-04-01", "news0", 1, 121),
        array("2011-04-01", "premium0", 3, 2900),
        array("2011-04-01", "technology0", 1, 78),
        array("2011-04-01", "travel0", 1, 119),
        array("2011-04-02", "automotive0", 1, 147),
        array("2011-04-02", "business0", 1, 112),
        array("2011-04-02", "entertainment0", 1, 166),
        array("2011-04-02", "health0", 1, 113),
        array("2011-04-02", "mezzanine0", 3, 2447),
        array("2011-04-02", "news0", 1, 114),
        array("2011-04-02", "premium0", 3, 2505),
        array("2011-04-02", "technology0", 1, 97),
        array("2011-04-02", "travel0", 1, 126)
    );
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
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, true, false),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive0", 1, 135),
        array("2011-04-01", "business0", 1, 118),
        array("2011-04-01", "entertainment0", 1, 158),
        array("2011-04-01", "health0", 1, 120),
        array("2011-04-01", "mezzanine0", 3, 2870),
        array("2011-04-01", "news0", 1, 121),
        array("2011-04-01", "premium0", 3, 2900),
        array("2011-04-01", "technology0", 1, 78),
        array("2011-04-01", "travel0", 1, 119),
        array("2011-04-02", "automotive0", 1, 147),
        array("2011-04-02", "business0", 1, 112),
        array("2011-04-02", "entertainment0", 1, 166),
        array("2011-04-02", "health0", 1, 113),
        array("2011-04-02", "mezzanine0", 3, 2447),
        array("2011-04-02", "news0", 1, 114),
        array("2011-04-02", "premium0", 3, 2505),
        array("2011-04-02", "technology0", 1, 97),
        array("2011-04-02", "travel0", 1, 126)
    );
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
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), false, "MISSING", true, false),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive0", 1, 135),
        array("2011-04-01", "business0", 1, 118),
        array("2011-04-01", "entertainment0", 1, 158),
        array("2011-04-01", "health0", 1, 120),
        array("2011-04-01", "mezzanine0", 3, 2870),
        array("2011-04-01", "news0", 1, 121),
        array("2011-04-01", "premium0", 3, 2900),
        array("2011-04-01", "technology0", 1, 78),
        array("2011-04-01", "travel0", 1, 119),
        array("2011-04-02", "automotive0", 1, 147),
        array("2011-04-02", "business0", 1, 112),
        array("2011-04-02", "entertainment0", 1, 166),
        array("2011-04-02", "health0", 1, 113),
        array("2011-04-02", "mezzanine0", 3, 2447),
        array("2011-04-02", "news0", 1, 114),
        array("2011-04-02", "premium0", 3, 2505),
        array("2011-04-02", "technology0", 1, 97),
        array("2011-04-02", "travel0", 1, 126)
    );
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
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, true, false),
                null
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive0", 1, 135),
        array("2011-04-01", "business0", 1, 118),
        array("2011-04-01", "entertainment0", 1, 158),
        array("2011-04-01", "health0", 1, 120),
        array("2011-04-01", "mezzanine0", 3, 2870),
        array("2011-04-01", "news0", 1, 121),
        array("2011-04-01", "premium0", 3, 2900),
        array("2011-04-01", "technology0", 1, 78),
        array("2011-04-01", "travel0", 1, 119),
        array("2011-04-02", "automotive0", 1, 147),
        array("2011-04-02", "business0", 1, 112),
        array("2011-04-02", "entertainment0", 1, 166),
        array("2011-04-02", "health0", 1, 113),
        array("2011-04-02", "mezzanine0", 3, 2447),
        array("2011-04-02", "news0", 1, 114),
        array("2011-04-02", "premium0", 3, 2505),
        array("2011-04-02", "technology0", 1, 97),
        array("2011-04-02", "travel0", 1, 126)
    );
  }

  @Test
  public void testGroupByWithUniques()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                qualityUniques
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    validate(
        query,
        array("__time", "rows", "uniques"),
        array("2011-04-01", 26, UNIQUES_9)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithUniquesAndPostAggWithSameName()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                rowsCount,
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
        .build();

    validate(
        query,
        array("__time", "rows", "quality_uniques"),
        array("2011-04-01", 26, UNIQUES_9)
    );
  }

  @Test
  public void testGroupByWithCardinality()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                qualityCardinality
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    validate(
        query, array("__time", "rows", "cardinality"), array("2011-04-01", 26, UNIQUES_9)
    );

    query = query
        .withVirtualColumns(
            Arrays.<VirtualColumn>asList(new ExprVirtualColumn("partial_null_column + ''", "PN"))
        ).withAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                CountAggregatorFactory.predicate("not_null", "!isnull(PN)"),
                CountAggregatorFactory.predicate("null", "isnull(PN)"))
        );

    validate(query, array("__time", "null", "not_null"), array("2011-04-01", 22, 4));

    query = query
        .withVirtualColumns(Arrays.<VirtualColumn>asList(new ExprVirtualColumn("market + 'AA'", "marketAA")))
        .withFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    BoundDimFilter.lt("marketAA", "spotAA"),
                    BoundDimFilter.gt("marketAA", "total_marketAA")
                )
            )
        )
        .withAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(rowsCount)
        );

    validate(query, array("__time", "rows"), array("2011-04-01", 4));
  }

  @Test
  public void testLimitOnCardinality()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(january)
        .setAggregatorSpecs(qualityCardinality)
        .setDimensions(DefaultDimensionSpec.of("market"))
        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("cardinality")))
        .build();

    validate(
        query,
        array("__time", "market", "cardinality"),
        array("2011-01-01", "spot", UNIQUES_9),
        array("2011-01-01", "total_market", UNIQUES_2),
        array("2011-01-01", "upfront", UNIQUES_2)
    );
  }

  @Test
  public void testGroupByWithNullProducingDimExtractionFn()
  {
    final ExtractionFn nullExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public KeyBuilder getCacheKey(KeyBuilder builder)
      {
        return builder.append((byte) 0xFF);
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals("mezzanine") ? null : super.apply(dimValue);
      }
    };
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setDimensions(new ExtractionDimensionSpec("quality", "alias", nullExtractionFn, null))
        .setQuerySegmentSpec(firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", null, 3, 2870),
        array("2011-04-01", "a", 1, 135),
        array("2011-04-01", "b", 1, 118),
        array("2011-04-01", "e", 1, 158),
        array("2011-04-01", "h", 1, 120),
        array("2011-04-01", "n", 1, 121),
        array("2011-04-01", "p", 3, 2900),
        array("2011-04-01", "t", 2, 197),

        array("2011-04-02", null, 3, 2447),
        array("2011-04-02", "a", 1, 147),
        array("2011-04-02", "b", 1, 112),
        array("2011-04-02", "e", 1, 166),
        array("2011-04-02", "h", 1, 113),
        array("2011-04-02", "n", 1, 114),
        array("2011-04-02", "p", 3, 2505),
        array("2011-04-02", "t", 2, 223)
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
      public KeyBuilder getCacheKey(KeyBuilder builder)
      {
        return builder.append((byte) 0xFF);
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals("mezzanine") ? "" : super.apply(dimValue);
      }
    };

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setDimensions(new ExtractionDimensionSpec("quality", "alias", emptyStringExtractionFn, null))
        .setQuerySegmentSpec(firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", null, 3, 2870),
        array("2011-04-01", "a", 1, 135),
        array("2011-04-01", "b", 1, 118),
        array("2011-04-01", "e", 1, 158),
        array("2011-04-01", "h", 1, 120),
        array("2011-04-01", "n", 1, 121),
        array("2011-04-01", "p", 3, 2900),
        array("2011-04-01", "t", 2, 197),
        array("2011-04-02", null, 3, 2447),
        array("2011-04-02", "a", 1, 147),
        array("2011-04-02", "b", 1, 112),
        array("2011-04-02", "e", 1, 166),
        array("2011-04-02", "h", 1, 113),
        array("2011-04-02", "n", 1, 114),
        array("2011-04-02", "p", 3, 2505),
        array("2011-04-02", "t", 2, 223)
    );
  }

  @Test
  public void testGroupByWithTimeZone()
  {
    DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");

    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(dataSource)
        .setInterval("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(new PeriodGranularity(new Period("P1D"), null, tz))
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array(new DateTime("2011-03-31", tz), "automotive", 1, 135),
        array(new DateTime("2011-03-31", tz), "business", 1, 118),
        array(new DateTime("2011-03-31", tz), "entertainment", 1, 158),
        array(new DateTime("2011-03-31", tz), "health", 1, 120),
        array(new DateTime("2011-03-31", tz), "mezzanine", 3, 2870),
        array(new DateTime("2011-03-31", tz), "news", 1, 121),
        array(new DateTime("2011-03-31", tz), "premium", 3, 2900),
        array(new DateTime("2011-03-31", tz), "technology", 1, 78),
        array(new DateTime("2011-03-31", tz), "travel", 1, 119),
        array(new DateTime("2011-04-01", tz), "automotive", 1, 147),
        array(new DateTime("2011-04-01", tz), "business", 1, 112),
        array(new DateTime("2011-04-01", tz), "entertainment", 1, 166),
        array(new DateTime("2011-04-01", tz), "health", 1, 113),
        array(new DateTime("2011-04-01", tz), "mezzanine", 3, 2447),
        array(new DateTime("2011-04-01", tz), "news", 1, 114),
        array(new DateTime("2011-04-01", tz), "premium", 3, 2505),
        array(new DateTime("2011-04-01", tz), "technology", 1, 97),
        array(new DateTime("2011-04-01", tz), "travel", 1, 126)
    );
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
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(limit);

    final GroupByQuery fullQuery = builder.build();

    final String[] columnNames = array("__time", "alias", "rows", "idx");

    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "automotive", 2, 269),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "entertainment", 2, 319),
        array("2011-04-01", "health", 2, 216),
        array("2011-04-01", "mezzanine", 6, 4420),
        array("2011-04-01", "news", 2, 221),
        array("2011-04-01", "premium", 6, 4416),
        array("2011-04-01", "technology", 2, 177),
        array("2011-04-01", "travel", 2, 243)
    );

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), runQuery(fullQuery), String.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderBy()
  {
    final int limit = 14;
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval(firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryGranularities.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", Direction.DESCENDING);

    GroupByQuery query = builder.build();

    String[] columnNames = array("__time", "alias", "rows", "idx");

    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "premium", 3, 2900),
        array("2011-04-01", "mezzanine", 3, 2870),
        array("2011-04-01", "entertainment", 1, 158),
        array("2011-04-01", "automotive", 1, 135),
        array("2011-04-01", "news", 1, 121),
        array("2011-04-01", "health", 1, 120),
        array("2011-04-01", "travel", 1, 119),
        array("2011-04-01", "business", 1, 118),
        array("2011-04-01", "technology", 1, 78),

        array("2011-04-02", "premium", 3, 2505),
        array("2011-04-02", "mezzanine", 3, 2447),
        array("2011-04-02", "entertainment", 1, 166),
        array("2011-04-02", "automotive", 1, 147),
        array("2011-04-02", "travel", 1, 126)
    );

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), runQuery(query), String.format("limit: %d", limit)
    );

    builder.setAggregatorSpecs(
        Arrays.asList(
            rowsCount,
            new LongSumAggregatorFactory("idx", null, "index * 2 + indexMin / 10", null)
        )
    );
    query = builder.build();

    expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "premium", 3, 6090),
        array("2011-04-01", "mezzanine", 3, 6030),
        array("2011-04-01", "entertainment", 1, 333),
        array("2011-04-01", "automotive", 1, 285),
        array("2011-04-01", "news", 1, 255),
        array("2011-04-01", "health", 1, 252),
        array("2011-04-01", "travel", 1, 251),
        array("2011-04-01", "business", 1, 248),
        array("2011-04-01", "technology", 1, 165),

        array("2011-04-02", "premium", 3, 5262),
        array("2011-04-02", "mezzanine", 3, 5141),
        array("2011-04-02", "entertainment", 1, 348),
        array("2011-04-02", "automotive", 1, 309),
        array("2011-04-02", "travel", 1, 265)
    );

    TestHelper.validate(
        columnNames, expectedResults.subList(0, limit), runQuery(query)
    );
  }

  @Ignore
  public void testMergeResultsWithNegativeLimit()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
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

    String[] columnNames = array("__time", "alias", "rows", "idx");

    List<Row> allResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "automotive", 2, 269),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "entertainment", 2, 319),
        array("2011-04-01", "health", 2, 216),
        array("2011-04-01", "mezzanine", 6, 4420),
        array("2011-04-01", "news", 2, 221),
        array("2011-04-01", "premium", 6, 4416),
        array("2011-04-01", "technology", 2, 177),
        array("2011-04-01", "travel", 2, 243)
    );

    List<List<Row>> expectedResults = Arrays.asList(
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
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimitSpec(orderBySpec);

    TestHelper.assertExpectedObjects(expectedResults, runQuery(builder.build()), "merged");
  }

  @Test
  public void testGroupByOrderLimit() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .addOrderByColumn("rows")
        .addOrderByColumn("alias", Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    String[] columnNames = {"__time", "alias", "rows", "idx"};
    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "travel", 2, 243),
        array("2011-04-01", "technology", 2, 177),
        array("2011-04-01", "news", 2, 221),
        array("2011-04-01", "health", 2, 216),
        array("2011-04-01", "entertainment", 2, 319),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "automotive", 2, 269),
        array("2011-04-01", "premium", 6, 4416),
        array("2011-04-01", "mezzanine", 6, 4420)
    );

    GroupByQuery query = builder.build();

    TestHelper.validate(columnNames, expectedResults, runQuery(query));

    query = builder.limit(5).build();
    TestHelper.validate(columnNames, expectedResults.subList(0, 5), runQuery(query));

    builder.setAggregatorSpecs(
        Arrays.asList(
            rowsCount,
            CountAggregatorFactory.predicate("rows1", "index > 110"),
            CountAggregatorFactory.predicate("rows2", "index > 130"),
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("idx2", "index", null, "index > 110"),
            new DoubleSumAggregatorFactory("idx3", "index", null, "index > 130")
        )
    );

    columnNames = array("__time", "alias", "rows", "rows1", "rows2", "idx", "idx2", "idx3");
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "travel", 2, 2, 0, 243, 243, 0.0D),
        array("2011-04-01", "technology", 2, 0, 0, 177, 0, 0.0D),
        array("2011-04-01", "news", 2, 1, 0, 221, 114, 0.0D),
        array("2011-04-01", "health", 2, 1, 0, 216, 113, 0.0D),
        array("2011-04-01", "entertainment", 2, 2, 2, 319, 319, 319.9440155029297D),
        array("2011-04-01", "business", 2, 1, 0, 217, 112, 0.0D),
        array("2011-04-01", "automotive", 2, 2, 1, 269, 269, 147.42593383789062D),
        array("2011-04-01", "premium", 6, 6, 5, 4416, 4416, 4296.476791381836D),
        array("2011-04-01", "mezzanine", 6, 5, 4, 4420, 4313, 4205.673645019531D)
    );

    query = builder.limit(100).build();
    TestHelper.validate(columnNames, expectedResults, runQuery(query));

    builder.limit(Integer.MAX_VALUE)
           .setAggregatorSpecs(
               rowsCount,
               new DoubleSumAggregatorFactory("idx", null, "index / 2 + indexMin", null)
           )
           .setPostAggregatorSpecs(
               new MathPostAggregator(
                   "MMM-yyyy", "time_format(__time, out.format='MMM yyyy', out.timezone='UTC', out.locale='en')"
               )
           );

    columnNames = array("__time", "alias", "rows", "idx", "MMM-yyyy");
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "travel", 2, 365.4876403808594D, "Apr 2011"),
        array("2011-04-01", "technology", 2, 267.3737564086914D, "Apr 2011"),
        array("2011-04-01", "news", 2, 333.3147277832031D, "Apr 2011"),
        array("2011-04-01", "health", 2, 325.467529296875D, "Apr 2011"),
        array("2011-04-01", "entertainment", 2, 479.916015625D, "Apr 2011"),
        array("2011-04-01", "business", 2, 328.08372497558594D, "Apr 2011"),
        array("2011-04-01", "automotive", 2, 405.5966796875D, "Apr 2011"),
        array("2011-04-01", "premium", 6, 6627.927734375D, "Apr 2011"),
        array("2011-04-01", "mezzanine", 6, 6635.480163574219D, "Apr 2011")
    );

    query = builder.build();
    TestHelper.validate(columnNames, expectedResults, runQuery(query));
    query = builder.limit(5).build();
    TestHelper.validate(columnNames, expectedResults.subList(0, 5), runQuery(query));
  }

  @Test
  public void testGroupByWithOrderLimit2() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .addOrderByColumn("rows", "desc")
        .addOrderByColumn("alias", "desc")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    String[] columnNames = {"__time", "alias", "rows", "idx"};
    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "premium", 6, 4416),
        array("2011-04-01", "mezzanine", 6, 4420),
        array("2011-04-01", "travel", 2, 243),
        array("2011-04-01", "technology", 2, 177),
        array("2011-04-01", "news", 2, 221),
        array("2011-04-01", "health", 2, 216),
        array("2011-04-01", "entertainment", 2, 319),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "automotive", 2, 269)
    );

    GroupByQuery query = builder.build();
    TestHelper.validate(columnNames, expectedResults, runQuery(query));

    query = builder.limit(5).build();
    TestHelper.validate(columnNames, expectedResults.subList(0, 5), runQuery(query));
  }

  @Test
  public void testGroupByWithOrderLimit3() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            rowsCount,
            new DoubleSumAggregatorFactory("idx", "index")
        )
        .addOrderByColumn("idx", "desc")
        .addOrderByColumn("alias", "desc")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    String[] columnNames = {"__time", "alias", "rows", "idx"};
    List<Row> expectedResults =createExpectedRows(
        columnNames,
        array("2011-04-01", "mezzanine", 6, 4423.653350830078D),
        array("2011-04-01", "premium", 6, 4418.618499755859D),
        array("2011-04-01", "entertainment", 2, 319.9440155029297D),
        array("2011-04-01", "automotive", 2, 270.39778900146484D),
        array("2011-04-01", "travel", 2, 243.65843200683594D),
        array("2011-04-01", "news", 2, 222.2098159790039D),
        array("2011-04-01", "business", 2, 218.7224884033203D),
        array("2011-04-01", "health", 2, 216.97835540771484D),
        array("2011-04-01", "technology", 2, 178.24917602539062D)
    );

    GroupByQuery query = builder.build();
    TestHelper.validate(columnNames, expectedResults, runQuery(query));

    query = builder.limit(5).build();
    TestHelper.validate(columnNames, expectedResults.subList(0, 5), runQuery(query));
  }

  @Test
  public void testGroupByWithSameCaseOrdering()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec(marketDimension, "marketalias"))
        .setInterval(fullOnInterval)
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
        .setAggregatorSpecs(rowsCount)
        .build();

    validate(
        query,
        array("__time", "marketalias", "rows"),
        array("1970-01-01", "upfront", 186),
        array("1970-01-01", "total_market", 186),
        array("1970-01-01", "spot", 837)
    );
  }

  @Test
  public void testGroupByWithOrderLimit4()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(DefaultDimensionSpec.of(marketDimension))
        .setInterval(fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        marketDimension,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(rowsCount)
        .build();

    List<Row> expectedResults = createExpectedRows(
        array("__time", "market", "rows"),
        array("1970-01-01T00:00:00.000Z", "upfront", 186),
        array("1970-01-01T00:00:00.000Z", "total_market", 186),
        array("1970-01-01T00:00:00.000Z", "spot", 837)
    );

    List<Row> results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");

    query = query.withOverriddenContext(Query.GBY_PRE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(TestIndex.segmentWalker);
    DimensionSpec dimensionSpec = query.getDimensions().get(0);
    Assert.assertTrue(dimensionSpec instanceof DimensionSpecWithOrdering);
    Assert.assertTrue(query.getLimitSpec().getColumns().isEmpty());

    results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testRemoveOrdering()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(DefaultDimensionSpec.of(marketDimension))
        .setInterval(fullOnInterval)
        .setLimitSpec(new LimitSpec(Lists.newArrayList(OrderByColumnSpec.asc(marketDimension)), 3))
        .setAggregatorSpecs(rowsCount)
        .build();

    List<Row> expectedResults = createExpectedRows(
        array("__time", "market", "rows"),
        array("1970-01-01T00:00:00.000Z", "spot", 837),
        array("1970-01-01T00:00:00.000Z", "total_market", 186),
        array("1970-01-01T00:00:00.000Z", "upfront", 186)
    );

    List<Row> results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-remove");

    query = query.withOverriddenContext(Query.GBY_REMOVE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(TestIndex.segmentWalker);
    Assert.assertTrue(query.getLimitSpec().getColumns().isEmpty());

    results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-remove");
  }

  @Test
  public void testConvertTimeseries()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.MONTH)
        .setInterval(fullOnInterval)
        .setDimFilter(new InDimFilter("market", Arrays.asList("spot", "total_market"), null))
        .setLimitSpec(new LimitSpec(Lists.newArrayList(OrderByColumnSpec.desc("rows")), 3))
        .setAggregatorSpecs(rowsCount)
        .setContext(ImmutableMap.<String, Object>of(GroupByQuery.SORT_ON_TIME, false))
        .build();

    List<Row> expectedResults = createExpectedRows(
        array("__time", "rows"),
        array("2011-03-01T00:00:00.000Z", 341),
        array("2011-02-01T00:00:00.000Z", 308),
        array("2011-01-01T00:00:00.000Z", 209)
    );

    List<Row> results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "convert-timeseries");

    query = query.withOverriddenContext(Query.GBY_CONVERT_TIMESERIES, true);
    Query timeseries = query.rewriteQuery(TestIndex.segmentWalker);
    Assert.assertTrue(timeseries instanceof TimeseriesQuery);

    results = runRowQuery(timeseries);
    TestHelper.assertExpectedObjects(expectedResults, results, "convert-timeseries");
  }

  @Test
  public void testGroupByWithOrderOnHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(DefaultDimensionSpec.of(marketDimension))
        .setInterval(fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(OrderByColumnSpec.desc(uniqueMetric), OrderByColumnSpec.desc(marketDimension)), 3
            )
        )
        .setAggregatorSpecs(qualityUniques)
        .setPostAggregatorSpecs(
            new HyperUniqueFinalizingPostAggregator(
                hyperUniqueFinalizingPostAggMetric,
                uniqueMetric
            )
        )
        .build();

    validate(
        query,
        array("__time", "market", "uniques", "hyperUniqueFinalizingPostAggMetric"),
        array("1970-01-01", "spot", UNIQUES_9, UNIQUES_9),
        array("1970-01-01", "upfront", UNIQUES_2, UNIQUES_2),
        array("1970-01-01", "total_market", UNIQUES_2, UNIQUES_2)
    );
  }

  @Test
  public void testGroupByWithHavingOnHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(DefaultDimensionSpec.of(marketDimension))
        .setInterval(fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        uniqueMetric,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(uniqueMetric, 8))
        .setAggregatorSpecs(
            qualityUniques
        )
        .setPostAggregatorSpecs(
            new HyperUniqueFinalizingPostAggregator(
                hyperUniqueFinalizingPostAggMetric,
                uniqueMetric
            ),
            new MathPostAggregator(
                "auto_finalized", uniqueMetric + " + 100"
            )
        )
        .build();

    validate(
        query,
        array("__time", "market", "uniques", "hyperUniqueFinalizingPostAggMetric", "auto_finalized"),
        array("1970-01-01", "spot", UNIQUES_9, UNIQUES_9, UNIQUES_9 + 100)
    );
  }

  @Test
  public void testGroupByWithHavingOnFinalizedHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(DefaultDimensionSpec.of(marketDimension))
        .setInterval(fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        hyperUniqueFinalizingPostAggMetric,
                        Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setHavingSpec(
            new GreaterThanHavingSpec(hyperUniqueFinalizingPostAggMetric, 8)
        )
        .setAggregatorSpecs(qualityUniques)
        .setPostAggregatorSpecs(
            new HyperUniqueFinalizingPostAggregator(
                hyperUniqueFinalizingPostAggMetric,
                uniqueMetric
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
       createExpectedRow(
           "1970-01-01T00:00:00.000Z",
           "market",
           "spot",
           uniqueMetric,
           UNIQUES_9,
           hyperUniqueFinalizingPostAggMetric,
           UNIQUES_9
       )
    );

    List<Row> results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");

    query = query.withHavingSpec(
        new ExpressionHavingSpec(
            hyperUniqueFinalizingPostAggMetric + "> 8"
        )
    );
    TestHelper.assertExpectedObjects(expectedResults, runQuery(query), "order-limit");
  }

  @Test
  public void testGroupByWithLimitOnFinalizedHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(DefaultDimensionSpec.of(marketDimension))
        .setInterval(fullOnInterval)
        .setLimitSpec(
            LimitSpecs.of(
                3,
                OrderByColumnSpec.desc(hyperUniqueFinalizingPostAggMetric),
                OrderByColumnSpec.desc(marketDimension)
            )
        )
        .setAggregatorSpecs(qualityUniques)
        .setPostAggregatorSpecs(
            new HyperUniqueFinalizingPostAggregator(
                hyperUniqueFinalizingPostAggMetric,
                uniqueMetric
            )
        )
        .build();

    validate(
        query,
        array("__time", "market", uniqueMetric, hyperUniqueFinalizingPostAggMetric),
        array("1970-01-01T00:00:00.000Z", "spot", UNIQUES_9, UNIQUES_9),
        array("1970-01-01T00:00:00.000Z", "upfront", UNIQUES_2, UNIQUES_2),
        array("1970-01-01T00:00:00.000Z", "total_market", UNIQUES_2, UNIQUES_2)
    );
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
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false),
                null
            )
        )
        .setAggregatorSpecs(rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new LimitSpec(
                Lists.<OrderByColumnSpec>newArrayList(
                    new OrderByColumnSpec("alias", Direction.DESCENDING, StringComparators.ALPHANUMERIC_NAME)
                ), null
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    List<Row> expectedResults = createExpectedRows(
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "travel555", 1, 119),
        array("2011-04-01", "travel123", 1, 78),
        array("2011-04-01", "travel47", 1, 158),
        array("2011-04-01", "health999", 3, 2900),
        array("2011-04-01", "health105", 1, 135),
        array("2011-04-01", "health55", 1, 120),
        array("2011-04-01", "health20", 1, 118),
        array("2011-04-01", "health09", 3, 2870),
        array("2011-04-01", "health0000", 1, 121),

        array("2011-04-02", "travel555", 1, 126),
        array("2011-04-02", "travel123", 1, 97),
        array("2011-04-02", "travel47", 1, 166),
        array("2011-04-02", "health999", 3, 2505),
        array("2011-04-02", "health105", 1, 147),
        array("2011-04-02", "health55", 1, 113),
        array("2011-04-02", "health20", 1, 112),
        array("2011-04-02", "health09", 3, 2447),
        array("2011-04-02", "health0000", 1, 114)
    );

    List<Row> results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withOverriddenContext(Query.GBY_PRE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(TestIndex.segmentWalker);
    DimensionSpec dimensionSpec = query.getDimensions().get(0);
    Assert.assertTrue(dimensionSpec instanceof DimensionSpecWithOrdering);
    Assert.assertTrue(query.getLimitSpec().getColumns().isEmpty());

    results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testPostAggMergedHavingSpec()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("index", "index")
        )
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000)
                )
            )
        );

    validate(
        builder.build(),
        array("__time", "alias", "rows", "index", "addRowsIndexConstant"),
        array("2011-04-01", "mezzanine", 6, 4420, 4427.0D),
        array("2011-04-01", "premium", 6, 4416, 4423.0D)
    );
  }

  @Test
  public void testGroupByWithOrderLimitHavingSpec()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-01-25/2011-01-28")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new DoubleSumAggregatorFactory("index", "index")
        )
        .setGranularity(QueryGranularities.ALL)
        .setHavingSpec(new GreaterThanHavingSpec("index", 310))
        .setLimitSpec(LimitSpecs.of(5, OrderByColumnSpec.asc("index")));

    validate(
        builder.build(),
        array("__time", "alias", "rows", "index"),
        array("2011-01-25", "business", 3, 312.3816375732422D),
        array("2011-01-25", "news", 3, 312.78340911865234D),
        array("2011-01-25", "technology", 3, 324.64124298095703D),
        array("2011-01-25", "travel", 3, 393.3632049560547D),
        array("2011-01-25", "health", 3, 511.29969787597656D)
    );
  }

  @Test
  public void testPostAggHavingSpec()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("index", "index")
        )
        .setPostAggregatorSpecs(QueryRunnerTestHelper.addRowsIndexConstant)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000)
                )
            )
        );

    validate(
        builder.build(),
        array("__time", "alias", "rows", "index", "addRowsIndexConstant"),
        array("2011-04-01", "mezzanine", 6, 4420, 4427.0D),
        array("2011-04-01", "premium", 6, 4416, 4423.0D)
    );

    builder.setHavingSpec(new ExpressionHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric + "> 1000"));
    validate(
        builder.build(),
        array("__time", "alias", "rows", "index", "addRowsIndexConstant"),
        array("2011-04-01", "mezzanine", 6, 4420, 4427.0D),
        array("2011-04-01", "premium", 6, 4416, 4423.0D)
    );
  }

  @Test
  public void testHavingSpec()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec("rows", 2),
                    new EqualToHavingSpec("idx", 217)
                )
            )
        );

    validate(
        builder.build(),
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "mezzanine", 6, 4420),
        array("2011-04-01", "premium", 6, 4416)
    );

    builder.setHavingSpec(new ExpressionHavingSpec("rows > 2 || idx == 217"));

    validate(
        builder.build(),
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "mezzanine", 6, 4420),
        array("2011-04-01", "premium", 6, 4416)
    );
  }

  @Test
  public void testMergedHavingSpec()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec("rows", 2),
                    new EqualToHavingSpec("idx", 217)
                )
            )
        );

    validate(
        builder.build(),
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "business", 2, 217),
        array("2011-04-01", "mezzanine", 6, 4420),
        array("2011-04-01", "premium", 6, 4416)
    );
  }

  @Test
  public void testMergedPostAggHavingSpec()
  {
    List<Row> expectedResults = createExpectedRows(
        array("__time", "alias", "rows", "idx", "rows_times_10"),
        array("2011-04-01", "business", 2, 217, 20.0),
        array("2011-04-01", "mezzanine", 6, 4420, 60.0),
        array("2011-04-01", "premium", 6, 4416, 60.0)
    );

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "rows_times_10",
                "*",
                Arrays.<PostAggregator>asList(
                    new FieldAccessPostAggregator("rows", "rows"),
                    new ConstantPostAggregator("const", 10)
                )
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec("rows_times_10", 20),
                    new EqualToHavingSpec("idx", 217)
                )
            )
        );

    GroupByQuery query = builder.build();

    TestHelper.assertExpectedObjects(expectedResults, runQuery(query), "merged");

    query = query
        .withPostAggregatorSpecs(Arrays.<PostAggregator>asList(new MathPostAggregator("rows_times_10", "rows * 10.0")))
        .withHavingSpec(new ExpressionHavingSpec("rows_times_10 > 20 || idx == 217"));

    TestHelper.assertExpectedObjects(expectedResults, runQuery(query), "merged");

    query = query.withPostAggregatorSpecs(
        Arrays.<PostAggregator>asList(new MathPostAggregator("rows_times_10 = rows * 10.0"))
    );

    TestHelper.assertExpectedObjects(expectedResults, runQuery(query), "merged");
  }

  @Test
  public void testGroupByWithRegEx() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimFilter(new RegexDimFilter("quality", "auto.*", null))
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    validate(
        builder.build(),
        array("__time", "quality", "rows"),
        array("2011-04-01", "automotive", 2)
    );
  }

  @Test
  public void testGroupByWithNonexistentDimension() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("billy")
        .addDimension("quality")
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    validate(
        builder.build(),
        array("__time", "billy", "quality", "rows"),
        array("2011-04-01", null, "automotive", 2),
        array("2011-04-01", null, "business", 2),
        array("2011-04-01", null, "entertainment", 2),
        array("2011-04-01", null, "health", 2),
        array("2011-04-01", null, "mezzanine", 6),
        array("2011-04-01", null, "news", 2),
        array("2011-04-01", null, "premium", 6),
        array("2011-04-01", null, "technology", 2),
        array("2011-04-01", null, "travel", 2)
    );
  }

  // A subquery identical to the query should yield identical results
  @Test
  public void testIdenticalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getDefault()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive", 1L, 135L),
        array("2011-04-01", "business", 1L, 118L),
        array("2011-04-01", "entertainment", 1L, 158L),
        array("2011-04-01", "health", 1L, 120L),
        array("2011-04-01", "mezzanine", 3L, 2870L),
        array("2011-04-01", "news", 1L, 121L),
        array("2011-04-01", "premium", 3L, 2900L),
        array("2011-04-01", "technology", 1L, 78L),
        array("2011-04-01", "travel", 1L, 119L),
        array("2011-04-02", "automotive", 1L, 147L),
        array("2011-04-02", "business", 1L, 112L),
        array("2011-04-02", "entertainment", 1L, 166L),
        array("2011-04-02", "health", 1L, 113L),
        array("2011-04-02", "mezzanine", 3L, 2447L),
        array("2011-04-02", "news", 1L, 114L),
        array("2011-04-02", "premium", 3L, 2505L),
        array("2011-04-02", "technology", 1L, 97L),
        array("2011-04-02", "travel", 1L, 126L)
    );
  }

  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQuery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(
            new JavaScriptDimFilter(
                "quality",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(Granularities.DAY)
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
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive", 1L, 135L),
        array("2011-04-01", "business", 1L, 118L),
        array("2011-04-01", "entertainment", 1L, 158L),
        array("2011-04-01", "health", 1L, 120L),
        array("2011-04-01", "mezzanine", 3L, 2870L),
        array("2011-04-01", "news", 1L, 121L),
        array("2011-04-01", "premium", 3L, 2900L),
        array("2011-04-01", "technology", 1L, 78L),
        array("2011-04-01", "travel", 1L, 119L),
        array("2011-04-02", "automotive", 1L, 147L),
        array("2011-04-02", "business", 1L, 112L),
        array("2011-04-02", "entertainment", 1L, 166L),
        array("2011-04-02", "health", 1L, 113L),
        array("2011-04-02", "mezzanine", 3L, 2447L),
        array("2011-04-02", "news", 1L, 114L),
        array("2011-04-02", "premium", 3L, 2505L),
        array("2011-04-02", "technology", 1L, 97L),
        array("2011-04-02", "travel", 1L, 126L)
    );
  }

  @Test
  public void testSubqueryWithExtractionFnInOuterQuery()
  {
    //https://github.com/druid-io/druid/issues/2556

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(
            new JavaScriptDimFilter(
                "quality",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
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
        .setDimensions(
            new ExtractionDimensionSpec(
                "alias",
                "alias",
                new RegexDimExtractionFn("(a).*", true, "a")
            )
        )
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "a", 13, 6619),
        array("2011-04-02", "a", 13, 5827)
    );

    query = query.withFilter(
        DimFilters.or(
            new InDimFilter("alias", Arrays.asList("a", "b"), null),
            new MathExprFilter("idx > 100 && idx < 200"),
            new InDimFilter("alias", Arrays.asList("b", "c"), null)
        )
    );
    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "a", 6, 771),
        array("2011-04-02", "a", 6, 778)
    );
  }

  @Test
  public void testDifferentGroupingSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new DoubleMaxAggregatorFactory("idx", "idx"),
            new DoubleMaxAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "rows", "idx", "indexMaxPlusTen"),
        array("2011-04-01", 9, 2900.0, 2930.0),
        array("2011-04-02", 9, 2505.0, 2535.0)
    );

    subquery = subquery.withAggregatorSpecs(
        Arrays.asList(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", null, "-index + 100", null),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
    );
    query = query.withDataSource(QueryDataSource.of(subquery));

    validate(
        query,
        array("__time", "rows", "idx", "indexMaxPlusTen"),
        array("2011-04-01", 9, 21.0, 2930.0),
        array("2011-04-02", 9, 2.0, 2535.0)
    );
  }

  @Test
  public void testDifferentGroupingSubqueryMultipleAggregatorsOnSameField()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "post_agg",
                "+",
                Lists.<PostAggregator>newArrayList(
                    new FieldAccessPostAggregator("idx", "idx"),
                    new FieldAccessPostAggregator("idx", "idx")
                )
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            new DoubleMaxAggregatorFactory("idx1", "idx"),
            new DoubleMaxAggregatorFactory("idx2", "idx"),
            new DoubleMaxAggregatorFactory("idx3", "post_agg"),
            new DoubleMaxAggregatorFactory("idx4", "post_agg")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "idx1", "idx2", "idx3", "idx4"),
        array("2011-04-01", 2900.0, 2900.0, 5800.0, 5800.0),
        array("2011-04-02", 2505.0, 2505.0, 5010.0, 5010.0)
    );
  }


  @Test
  public void testDifferentGroupingSubqueryWithFilter()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
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
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "idx"),
        array("2011-04-01", 2900.0),
        array("2011-04-02", 2505.0)
    );
  }

  @Test
  public void testDifferentIntervalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.secondOnly)
        .setAggregatorSpecs(
            new DoubleMaxAggregatorFactory("idx", "idx")
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "idx"),
        array("2011-04-02", 2505.0)
    );
  }

  @Test
  public void testEmptySubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setGranularity(Granularities.DAY)
        .build();

    Assert.assertFalse(runQuery(query).iterator().hasNext());
  }

  @Test
  public void testSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
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
        .addOrderByColumn("index", Direction.DESCENDING)
        .build();

    List<Row> expectedResults = createExpectedRows(
        array("__time", "rows", "index"),
        array("2011-01-12T00:00:00.000Z", 1, 120628),
        array("2011-01-12T00:00:00.000Z", 2, 2483),
        array("2011-01-12T00:00:00.000Z", 3, 1278),
        array("2011-01-12T00:00:00.000Z", 4, 808),
        array("2011-01-12T00:00:00.000Z", 6, 627),
        array("2011-01-12T00:00:00.000Z", 5, 208)
    );

    TestHelper.assertExpectedObjects(expectedResults, runQuery(query), "");

    // outer queries are resolved from schema of inner query.. (FluentQueryRunnerBuilder.applySubQueryResolver)
    GroupByQuery outerResolving = query.withAggregatorSpecs(
        Arrays.<AggregatorFactory>asList(new GenericSumAggregatorFactory("index", "index", null))
    );
    TestHelper.assertExpectedObjects(expectedResults, runQuery(outerResolving), "");
  }

  @Test
  public void testSubqueryWithPostAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(
            new JavaScriptDimFilter(
                "quality",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx_subagg", "index")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "idx_subpostagg",
                "+",
                Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000)
                )
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx_subpostagg")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "idx_post", "+", Arrays.asList(
                new FieldAccessPostAggregator("the_idx_agg", "idx"),
                new ConstantPostAggregator("ten_thousand", 10000)
            )
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx_post", "idx"),
        array("2011-04-01", "automotive", 1L, 11135.0, 1135L),
        array("2011-04-01", "business", 1L, 11118.0, 1118L),
        array("2011-04-01", "entertainment", 1L, 11158.0, 1158L),
        array("2011-04-01", "health", 1L, 11120.0, 1120L),
        array("2011-04-01", "mezzanine", 3L, 13870.0, 3870L),
        array("2011-04-01", "news", 1L, 11121.0, 1121L),
        array("2011-04-01", "premium", 3L, 13900.0, 3900L),
        array("2011-04-01", "technology", 1L, 11078.0, 1078L),
        array("2011-04-01", "travel", 1L, 11119.0, 1119L),
        array("2011-04-02", "automotive", 1L, 11147.0, 1147L),
        array("2011-04-02", "business", 1L, 11112.0, 1112L),
        array("2011-04-02", "entertainment", 1L, 11166.0, 1166L),
        array("2011-04-02", "health", 1L, 11113.0, 1113L),
        array("2011-04-02", "mezzanine", 3L, 13447.0, 3447L),
        array("2011-04-02", "news", 1L, 11114.0, 1114L),
        array("2011-04-02", "premium", 3L, 13505.0, 3505L),
        array("2011-04-02", "technology", 1L, 11097.0, 1097L),
        array("2011-04-02", "travel", 1L, 11126.0, 1126L)
    );
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(
            new JavaScriptDimFilter(
                "quality",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx_subagg", "index")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "idx_subpostagg",
                "+",
                Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000)
                )
            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public Predicate<Row> toEvaluator(TypeResolver resolver)
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
        .setGranularity(Granularities.DAY)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx_subpostagg")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "idx_post", "+", Arrays.asList(
                new FieldAccessPostAggregator("the_idx_agg", "idx"),
                new ConstantPostAggregator("ten_thousand", 10000)
            )
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx_post", "idx"),
        array("2011-04-01", "automotive", 1, 11135.0, 1135),
        array("2011-04-01", "business", 1, 11118.0, 1118),
        array("2011-04-01", "entertainment", 1, 11158.0, 1158),
        array("2011-04-01", "health", 1, 11120.0, 1120),
        array("2011-04-01", "news", 1, 11121.0, 1121),
        array("2011-04-01", "technology", 1, 11078.0, 1078),
        array("2011-04-01", "travel", 1, 11119.0, 1119),
        array("2011-04-02", "automotive", 1, 11147.0, 1147),
        array("2011-04-02", "business", 1, 11112.0, 1112),
        array("2011-04-02", "entertainment", 1, 11166.0, 1166),
        array("2011-04-02", "health", 1, 11113.0, 1113),
        array("2011-04-02", "mezzanine", 3, 13447.0, 3447),
        array("2011-04-02", "news", 1, 11114.0, 1114),
        array("2011-04-02", "premium", 3, 13505.0, 3505),
        array("2011-04-02", "technology", 1, 11097.0, 1097),
        array("2011-04-02", "travel", 1, 11126.0, 1126)
    );
  }

  @Test
  public void testSubqueryWithMultiColumnAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(
            new JavaScriptDimFilter(
                "market",
                "function(dim){ return true; }",
                null,
                JavaScriptConfig.getDefault()
            )
        )
        .setAggregatorSpecs(
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
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "idx_subpostagg",
                "+",
                Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000)
                )
            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public Predicate<Row> toEvaluator(TypeResolver resolver)
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
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx_subpostagg"),
            new DoubleSumAggregatorFactory("js_outer_agg", "js_agg")
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "idx_post", "+", Arrays.asList(
                new FieldAccessPostAggregator("the_idx_agg", "idx"),
                new ConstantPostAggregator("ten_thousand", 10000)
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
        .setGranularity(Granularities.DAY)
        .build();

    String[] columnNames = {"__time", "alias", "rows", "idx_post", "idx", "js_outer_agg"};
    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "travel", 1, 11119.0, 1119, 123.92274475097656),
        array("2011-04-01", "technology", 1, 11078.0, 1078, 82.62254333496094),
        array("2011-04-01", "news", 1, 11121.0, 1121, 125.58358001708984),
        array("2011-04-01", "health", 1, 11120.0, 1120, 124.13470458984375),
        array("2011-04-01", "entertainment", 1, 11158.0, 1158, 162.74722290039062)
    );

    // Subqueries are handled by the ToolChest
    TestHelper.validate(columnNames, expectedResults, runQuery(query));

    query = query.withHavingSpec(new ExpressionHavingSpec("idx_subpostagg == null || idx_subpostagg < 3800.0"));
    TestHelper.validate(columnNames, expectedResults, runQuery(query));
  }

  @Test
  public void testSubqueryWithHyperUniques()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new HyperUniquesAggregatorFactory("quality_uniques", "quality_uniques")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx"),
            new HyperUniquesAggregatorFactory("uniq", "quality_uniques")
        )
        .setGranularity(Granularities.ALL)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx", "uniq"),
        array("2011-04-01", "automotive", 2L, 282L, 1.0002442201269182D),
        array("2011-04-01", "business", 2L, 230L, 1.0002442201269182D),
        array("2011-04-01", "entertainment", 2L, 324L, 1.0002442201269182D),
        array("2011-04-01", "health", 2L, 233L, 1.0002442201269182D),
        array("2011-04-01", "mezzanine", 6L, 5317L, 1.0002442201269182D),
        array("2011-04-01", "news", 2L, 235L, 1.0002442201269182D),
        array("2011-04-01", "premium", 6L, 5405L, 1.0002442201269182D),
        array("2011-04-01", "technology", 2L, 175L, 1.0002442201269182D),
        array("2011-04-01", "travel", 2L, 245L, 1.0002442201269182D)
    );
  }

  @Test
  public void testSubqueryWithHyperUniquesPostAggregator()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions()
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new HyperUniquesAggregatorFactory("quality_uniques_inner", "quality_uniques")
        )
        .setPostAggregatorSpecs(
            new FieldAccessPostAggregator("quality_uniques_inner_post", "quality_uniques_inner")
        )
        .setGranularity(Granularities.DAY)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions()
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx"),
            new HyperUniquesAggregatorFactory("quality_uniques_outer", "quality_uniques_inner_post")
        )
        .setPostAggregatorSpecs(
            new HyperUniqueFinalizingPostAggregator("quality_uniques_outer_post", "quality_uniques_outer")
        )
        .setGranularity(Granularities.ALL)
        .build();

    validate(
        query,
        array("__time", "rows", "idx", "quality_uniques_outer", "quality_uniques_outer_post"),
        array("2011-04-01", 26L, 12446L, 9.019833517963864, 9.019833517963864)
    );
  }

  @Test
  public void testGroupByWithTimeColumn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
            QueryRunnerTestHelper.__timeLongSum
        )
        .setGranularity(Granularities.ALL)
        .build();

    validate(
        query,
        array("__time", "rows", "ntimestamps", "sumtime"),
        array("2011-04-01", 26, 13.0, 33843139200000L)
    );
  }

  @Test
  public void testGroupByTimeExtraction()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
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
            QueryRunnerTestHelper.rowsCount,
            QueryRunnerTestHelper.indexDoubleSum
        )
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(Granularities.ALL)
        .setDimFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter("market", "spot", null),
                    new MathExprFilter("market == 'upfront'")
                )
            )
        )
        .build();

    validate(
        query,
        array("__time", "dayOfWeek", "market", "index", "rows", "addRowsIndexConstant"),
        array("1970-01-01", "Friday", "spot", 13219.574020385742D, 117, 13337.574020385742D),
        array("1970-01-01", "Monday", "spot", 13557.73889541626D, 117, 13675.73889541626D),
        array("1970-01-01", "Saturday", "spot", 13493.751190185547D, 117, 13611.751190185547D),
        array("1970-01-01", "Sunday", "spot", 13585.540908813477D, 117, 13703.540908813477D),
        array("1970-01-01", "Thursday", "spot", 14279.127326965332D, 126, 14406.127326965332D),
        array("1970-01-01", "Tuesday", "spot", 13199.471267700195D, 117, 13317.471267700195D),
        array("1970-01-01", "Wednesday", "spot", 14271.368713378906D, 126, 14398.368713378906D),
        array("1970-01-01", "Friday", "upfront", 27297.862365722656D, 26, 27324.862365722656D),
        array("1970-01-01", "Monday", "upfront", 27619.58477783203D, 26, 27646.58477783203D),
        array("1970-01-01", "Saturday", "upfront", 27820.831176757812D, 26, 27847.831176757812D),
        array("1970-01-01", "Sunday", "upfront", 24791.22381591797D, 26, 24818.22381591797D),
        array("1970-01-01", "Thursday", "upfront", 28562.748779296875D, 28, 28591.748779296875D),
        array("1970-01-01", "Tuesday", "upfront", 26968.28009033203D, 26, 26995.28009033203D),
        array("1970-01-01", "Wednesday", "upfront", 28985.57501220703D, 28, 29014.57501220703D)
    );
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
        .setDataSource(dataSource)
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
            QueryRunnerTestHelper.rowsCount,
            QueryRunnerTestHelper.indexDoubleSum
        )
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
        )
        .setLimitSpec(
            new LimitSpec(
                Arrays.asList(
                    OrderByColumnSpec.asc("dayOfWeek", StringComparators.DAY_OF_WEEK_NAME),
                    OrderByColumnSpec.desc("market"),
                    OrderByColumnSpec.asc("rows")
                ),
                30
            )
        );

    String[] columnNames = {"__time", "dayOfWeek", "market", "rows", "index", "addRowsIndexConstant"};
    List<Row> results;

    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("1970-01-01", "Monday", "upfront", 26, 27619.58477783203D, 27646.58477783203D),
        array("1970-01-01", "Monday", "total_market", 26, 30468.776733398438D, 30495.776733398438D),
        array("1970-01-01", "Monday", "spot", 117, 13557.73889541626D, 13675.73889541626D),
        array("1970-01-01", "Tuesday", "upfront", 26, 26968.28009033203D, 26995.28009033203D),
        array("1970-01-01", "Tuesday", "total_market", 26, 29676.578247070312D, 29703.578247070312D),
        array("1970-01-01", "Tuesday", "spot", 117, 13199.471267700195D, 13317.471267700195D),
        array("1970-01-01", "Wednesday", "upfront", 28, 28985.57501220703D, 29014.57501220703D),
        array("1970-01-01", "Wednesday", "total_market", 28, 32753.337280273438D, 32782.33728027344D),
        array("1970-01-01", "Wednesday", "spot", 126, 14271.368713378906D, 14398.368713378906D),
        array("1970-01-01", "Thursday", "upfront", 28, 28562.748779296875D, 28591.748779296875D),
        array("1970-01-01", "Thursday", "total_market", 28, 32361.38690185547D, 32390.38690185547D),
        array("1970-01-01", "Thursday", "spot", 126, 14279.127326965332D, 14406.127326965332D),
        array("1970-01-01", "Friday", "upfront", 26, 27297.862365722656D, 27324.862365722656D),
        array("1970-01-01", "Friday", "total_market", 26, 30173.691955566406D, 30200.691955566406D),
        array("1970-01-01", "Friday", "spot", 117, 13219.574020385742D, 13337.574020385742D),
        array("1970-01-01", "Saturday", "upfront", 26, 27820.831176757812D, 27847.831176757812D),
        array("1970-01-01", "Saturday", "total_market", 26, 30940.971740722656D, 30967.971740722656D),
        array("1970-01-01", "Saturday", "spot", 117, 13493.751190185547D, 13611.751190185547D),
        array("1970-01-01", "Sunday", "upfront", 26, 24791.22381591797D, 24818.22381591797D),
        array("1970-01-01", "Sunday", "total_market", 26, 29305.0859375D, 29332.0859375D),
        array("1970-01-01", "Sunday", "spot", 117, 13585.540908813477D, 13703.540908813477D)
    );

    results = runQuery(builder.build(), true);
    TestHelper.validate(columnNames, expectedResults, results);

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
                    "sum_post = $sum(addRowsIndexConstant)",
                    "avg_post = round($avg(addRowsIndexConstant), 1)"
                )
            )
        )
    );

    columnNames = array(
        "__time", "dayOfWeek", "rows", "delta_week", "sum_week", "delta_all", "sum_all", "sum_post", "avg_post"
    );
    expectedResults = createExpectedRows(
        columnNames,
        array("1970-01-01", "Friday", 26, 0L, 26L, 0L, 26L, 27324.862365722656, 27324.9),
        array("1970-01-01", "Friday", 26, 0L, 52L, 0L, 52L, 57525.55432128906, 28762.8),
        array("1970-01-01", "Friday", 117, 91L, 169L, 91L, 169L, 70863.1283416748, 23621.0),
        array("1970-01-01", "Monday", 26, 0L, 26L, -91L, 195L, 98509.71311950684, 24627.4),
        array("1970-01-01", "Monday", 26, 0L, 52L, 0L, 221L, 129005.48985290527, 25801.1),
        array("1970-01-01", "Monday", 117, 91L, 169L, 91L, 338L, 142681.22874832153, 23780.2),
        array("1970-01-01", "Saturday", 26, 0L, 26L, -91L, 364L, 170529.05992507935, 24361.3),
        array("1970-01-01", "Saturday", 26, 0L, 52L, 0L, 390L, 201497.031665802, 25187.1),
        array("1970-01-01", "Saturday", 117, 91L, 169L, 91L, 507L, 215108.78285598755, 23901.0),
        array("1970-01-01", "Sunday", 26, 0L, 26L, -91L, 533L, 239927.00667190552, 23992.7),
        array("1970-01-01", "Sunday", 26, 0L, 52L, 0L, 559L, 269259.0926094055, 24478.1),
        array("1970-01-01", "Sunday", 117, 91L, 169L, 91L, 676L, 282962.633518219, 23580.2),
        array("1970-01-01", "Thursday", 28, 0L, 28L, -89L, 704L, 311554.38229751587, 23965.7),
        array("1970-01-01", "Thursday", 28, 0L, 56L, 0L, 732L, 343944.76919937134, 24567.5),
        array("1970-01-01", "Thursday", 126, 98L, 182L, 98L, 858L, 358350.89652633667, 23890.1),
        array("1970-01-01", "Tuesday", 26, 0L, 26L, -100L, 884L, 385346.1766166687, 24084.1),
        array("1970-01-01", "Tuesday", 26, 0L, 52L, 0L, 910L, 415049.754863739, 24414.7),
        array("1970-01-01", "Tuesday", 117, 91L, 169L, 91L, 1027L, 428367.2261314392, 23798.2),
        array("1970-01-01", "Wednesday", 28, 0L, 28L, -89L, 1055L, 457381.80114364624, 24072.7),
        array("1970-01-01", "Wednesday", 28, 0L, 56L, 0L, 1083L, 490164.1384239197, 24508.2),
        array("1970-01-01", "Wednesday", 126, 98L, 182L, 98L, 1209L, 504562.5071372986, 24026.8)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    GroupByQuery query = builder.build();
    query = query.withOverriddenContext(Query.GBY_PRE_ORDERING, true);
    query = (GroupByQuery) query.rewriteQuery(TestIndex.segmentWalker);
    DimensionSpec dimensionSpec = query.getDimensions().get(1);
    Assert.assertTrue(dimensionSpec instanceof DimensionSpecWithOrdering);  // 0 is basic ordering
    WindowingSpec windowingSpec = query.getLimitSpec().getWindowingSpecs().get(0);
    Assert.assertTrue(windowingSpec.isSkipSorting());

    results = runQuery(query);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("__time", "dayOfWeek", "rows", "delta_week", "sum_week", "delta_all", "sum_all");
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-01-01", "Friday", 4, 0L, 4L, 0L, 4L),
        array("2011-02-01", "Friday", 8, 4L, 12L, 4L, 12L),
        array("2011-03-01", "Friday", 8, 0L, 20L, 0L, 20L),
        array("2011-04-01", "Friday", 6, -2L, 26L, -2L, 26L),
        array("2011-01-01", "Friday", 4, -2L, 30L, -2L, 30L),
        array("2011-02-01", "Friday", 8, 4L, 38L, 4L, 38L),
        array("2011-03-01", "Friday", 8, 0L, 46L, 0L, 46L),
        array("2011-04-01", "Friday", 6, -2L, 52L, -2L, 52L),
        array("2011-01-01", "Friday", 18, 12L, 70L, 12L, 70L),
        array("2011-02-01", "Friday", 36, 18L, 106L, 18L, 106L),
        array("2011-03-01", "Friday", 36, 0L, 142L, 0L, 142L),
        array("2011-04-01", "Friday", 27, -9L, 169L, -9L, 169L),
        array("2011-01-01", "Monday", 6, 0L, 6L, -21L, 175L),
        array("2011-02-01", "Monday", 8, 2L, 14L, 2L, 183L),
        array("2011-03-01", "Monday", 8, 0L, 22L, 0L, 191L),
        array("2011-04-01", "Monday", 4, -4L, 26L, -4L, 195L),
        array("2011-01-01", "Monday", 6, 2L, 32L, 2L, 201L),
        array("2011-02-01", "Monday", 8, 2L, 40L, 2L, 209L),
        array("2011-03-01", "Monday", 8, 0L, 48L, 0L, 217L),
        array("2011-04-01", "Monday", 4, -4L, 52L, -4L, 221L),
        array("2011-01-01", "Monday", 27, 23L, 79L, 23L, 248L),
        array("2011-02-01", "Monday", 36, 9L, 115L, 9L, 284L),
        array("2011-03-01", "Monday", 36, 0L, 151L, 0L, 320L),
        array("2011-04-01", "Monday", 18, -18L, 169L, -18L, 338L),
        array("2011-01-01", "Saturday", 6, 0L, 6L, -12L, 344L),
        array("2011-02-01", "Saturday", 8, 2L, 14L, 2L, 352L),
        array("2011-03-01", "Saturday", 8, 0L, 22L, 0L, 360L),
        array("2011-04-01", "Saturday", 4, -4L, 26L, -4L, 364L),
        array("2011-01-01", "Saturday", 6, 2L, 32L, 2L, 370L),
        array("2011-02-01", "Saturday", 8, 2L, 40L, 2L, 378L),
        array("2011-03-01", "Saturday", 8, 0L, 48L, 0L, 386L),
        array("2011-04-01", "Saturday", 4, -4L, 52L, -4L, 390L),
        array("2011-01-01", "Saturday", 27, 23L, 79L, 23L, 417L),
        array("2011-02-01", "Saturday", 36, 9L, 115L, 9L, 453L),
        array("2011-03-01", "Saturday", 36, 0L, 151L, 0L, 489L),
        array("2011-04-01", "Saturday", 18, -18L, 169L, -18L, 507L),
        array("2011-01-01", "Sunday", 6, 0L, 6L, -12L, 513L),
        array("2011-02-01", "Sunday", 8, 2L, 14L, 2L, 521L),
        array("2011-03-01", "Sunday", 8, 0L, 22L, 0L, 529L),
        array("2011-04-01", "Sunday", 4, -4L, 26L, -4L, 533L),
        array("2011-01-01", "Sunday", 6, 2L, 32L, 2L, 539L),
        array("2011-02-01", "Sunday", 8, 2L, 40L, 2L, 547L),
        array("2011-03-01", "Sunday", 8, 0L, 48L, 0L, 555L),
        array("2011-04-01", "Sunday", 4, -4L, 52L, -4L, 559L),
        array("2011-01-01", "Sunday", 27, 23L, 79L, 23L, 586L),
        array("2011-02-01", "Sunday", 36, 9L, 115L, 9L, 622L),
        array("2011-03-01", "Sunday", 36, 0L, 151L, 0L, 658L),
        array("2011-04-01", "Sunday", 18, -18L, 169L, -18L, 676L),
        array("2011-01-01", "Thursday", 6, 0L, 6L, -12L, 682L),
        array("2011-02-01", "Thursday", 8, 2L, 14L, 2L, 690L),
        array("2011-03-01", "Thursday", 10, 2L, 24L, 2L, 700L),
        array("2011-04-01", "Thursday", 4, -6L, 28L, -6L, 704L),
        array("2011-01-01", "Thursday", 6, 2L, 34L, 2L, 710L),
        array("2011-02-01", "Thursday", 8, 2L, 42L, 2L, 718L),
        array("2011-03-01", "Thursday", 10, 2L, 52L, 2L, 728L),
        array("2011-04-01", "Thursday", 4, -6L, 56L, -6L, 732L),
        array("2011-01-01", "Thursday", 27, 23L, 83L, 23L, 759L),
        array("2011-02-01", "Thursday", 36, 9L, 119L, 9L, 795L),
        array("2011-03-01", "Thursday", 45, 9L, 164L, 9L, 840L),
        array("2011-04-01", "Thursday", 18, -27L, 182L, -27L, 858L),
        array("2011-01-01", "Tuesday", 4, 0L, 4L, -14L, 862L),
        array("2011-02-01", "Tuesday", 8, 4L, 12L, 4L, 870L),
        array("2011-03-01", "Tuesday", 10, 2L, 22L, 2L, 880L),
        array("2011-04-01", "Tuesday", 4, -6L, 26L, -6L, 884L),
        array("2011-01-01", "Tuesday", 4, 0L, 30L, 0L, 888L),
        array("2011-02-01", "Tuesday", 8, 4L, 38L, 4L, 896L),
        array("2011-03-01", "Tuesday", 10, 2L, 48L, 2L, 906L),
        array("2011-04-01", "Tuesday", 4, -6L, 52L, -6L, 910L),
        array("2011-01-01", "Tuesday", 18, 14L, 70L, 14L, 928L),
        array("2011-02-01", "Tuesday", 36, 18L, 106L, 18L, 964L),
        array("2011-03-01", "Tuesday", 45, 9L, 151L, 9L, 1009L),
        array("2011-04-01", "Tuesday", 18, -27L, 169L, -27L, 1027L),
        array("2011-01-01", "Wednesday", 6, 0L, 6L, -12L, 1033L),
        array("2011-02-01", "Wednesday", 8, 2L, 14L, 2L, 1041L),
        array("2011-03-01", "Wednesday", 10, 2L, 24L, 2L, 1051L),
        array("2011-04-01", "Wednesday", 4, -6L, 28L, -6L, 1055L),
        array("2011-01-01", "Wednesday", 6, 2L, 34L, 2L, 1061L),
        array("2011-02-01", "Wednesday", 8, 2L, 42L, 2L, 1069L),
        array("2011-03-01", "Wednesday", 10, 2L, 52L, 2L, 1079L),
        array("2011-04-01", "Wednesday", 4, -6L, 56L, -6L, 1083L),
        array("2011-01-01", "Wednesday", 27, 23L, 83L, 23L, 1110L),
        array("2011-02-01", "Wednesday", 36, 9L, 119L, 9L, 1146L),
        array("2011-03-01", "Wednesday", 45, 9L, 164L, 9L, 1191L),
        array("2011-04-01", "Wednesday", 18, -27L, 182L, -27L, 1209L)
    );
    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("__time", "dayOfWeek", "rows", "delta_week", "sum_week", "delta_all", "sum_all");
    expectedResults = createExpectedRows(
        columnNames,
        array("1970-01-01T00:00:00.000Z", "Wednesday", 126, 98L, 182L, 98L, 1209L),
        array("1970-01-01T00:00:00.000Z", "Wednesday", 28, 0L, 56L, 0L, 1083L),
        array("1970-01-01T00:00:00.000Z", "Wednesday", 28, 0L, 28L, -89L, 1055L),
        array("1970-01-01T00:00:00.000Z", "Tuesday", 117, 91L, 169L, 91L, 1027L),
        array("1970-01-01T00:00:00.000Z", "Tuesday", 26, 0L, 52L, 0L, 910L),
        array("1970-01-01T00:00:00.000Z", "Tuesday", 26, 0L, 26L, -100L, 884L),
        array("1970-01-01T00:00:00.000Z", "Thursday", 126, 98L, 182L, 98L, 858L),
        array("1970-01-01T00:00:00.000Z", "Thursday", 28, 0L, 56L, 0L, 732L),
        array("1970-01-01T00:00:00.000Z", "Thursday", 28, 0L, 28L, -89L, 704L),
        array("1970-01-01T00:00:00.000Z", "Sunday", 117, 91L, 169L, 91L, 676L),
        array("1970-01-01T00:00:00.000Z", "Sunday", 26, 0L, 52L, 0L, 559L),
        array("1970-01-01T00:00:00.000Z", "Sunday", 26, 0L, 26L, -91L, 533L),
        array("1970-01-01T00:00:00.000Z", "Saturday", 117, 91L, 169L, 91L, 507L),
        array("1970-01-01T00:00:00.000Z", "Saturday", 26, 0L, 52L, 0L, 390L),
        array("1970-01-01T00:00:00.000Z", "Saturday", 26, 0L, 26L, -91L, 364L),
        array("1970-01-01T00:00:00.000Z", "Monday", 117, 91L, 169L, 91L, 338L),
        array("1970-01-01T00:00:00.000Z", "Monday", 26, 0L, 52L, 0L, 221L),
        array("1970-01-01T00:00:00.000Z", "Monday", 26, 0L, 26L, -91L, 195L),
        array("1970-01-01T00:00:00.000Z", "Friday", 117, 91L, 169L, 91L, 169L),
        array("1970-01-01T00:00:00.000Z", "Friday", 26, 0L, 52L, 0L, 52L),
        array("1970-01-01T00:00:00.000Z", "Friday", 26, 0L, 26L, 0L, 26L)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "min_all = $min(index)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "min_week = $min(index)")
            )
        )
    );

    columnNames = array("dayOfWeek", "market", "index", "min_week", "min_all");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", "upfront", 27297.862365722656, 27297.862365722656, 27297.862365722656),
        array("Friday", "total_market", 30173.691955566406, 27297.862365722656, 27297.862365722656),
        array("Friday", "spot", 13219.574020385742, 13219.574020385742, 13219.574020385742),
        array("Monday", "upfront", 27619.58477783203, 27619.58477783203, 13219.574020385742),
        array("Monday", "total_market", 30468.776733398438, 27619.58477783203, 13219.574020385742),
        array("Monday", "spot", 13557.73889541626, 13557.73889541626, 13219.574020385742),
        array("Saturday", "upfront", 27820.831176757812, 27820.831176757812, 13219.574020385742),
        array("Saturday", "total_market", 30940.971740722656, 27820.831176757812, 13219.574020385742),
        array("Saturday", "spot", 13493.751190185547, 13493.751190185547, 13219.574020385742),
        array("Sunday", "upfront", 24791.22381591797, 24791.22381591797, 13219.574020385742),
        array("Sunday", "total_market", 29305.0859375, 24791.22381591797, 13219.574020385742),
        array("Sunday", "spot", 13585.540908813477, 13585.540908813477, 13219.574020385742),
        array("Thursday", "upfront", 28562.748779296875, 28562.748779296875, 13219.574020385742),
        array("Thursday", "total_market", 32361.38690185547, 28562.748779296875, 13219.574020385742),
        array("Thursday", "spot", 14279.127326965332, 14279.127326965332, 13219.574020385742),
        array("Tuesday", "upfront", 26968.28009033203, 26968.28009033203, 13219.574020385742),
        array("Tuesday", "total_market", 29676.578247070312, 26968.28009033203, 13219.574020385742),
        array("Tuesday", "spot", 13199.471267700195, 13199.471267700195, 13199.471267700195),
        array("Wednesday", "upfront", 28985.57501220703, 28985.57501220703, 13199.471267700195),
        array("Wednesday", "total_market", 32753.337280273438, 28985.57501220703, 13199.471267700195),
        array("Wednesday", "spot", 14271.368713378906, 14271.368713378906, 13199.471267700195)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    // don't know what the hell is irr
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "irr_all = $irr(index)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "irr_week = $irr(index)")
            )
        )
    );

    columnNames = array("dayOfWeek", "market", "index", "irr_all", "irr_week");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", "upfront", 27297.862365722656, null, null),
        array("Friday", "total_market", 30173.691955566406, null, null),
        array("Friday", "spot", 13219.574020385742, null, NaN),
        array("Monday", "upfront", 27619.58477783203, null, null),
        array("Monday", "total_market", 30468.776733398438, null, null),
        array("Monday", "spot", 13557.73889541626, null, NaN),
        array("Saturday", "upfront", 27820.831176757812, null, null),
        array("Saturday", "total_market", 30940.971740722656, null, null),
        array("Saturday", "spot", 13493.751190185547, null, NaN),
        array("Sunday", "upfront", 24791.22381591797, null, null),
        array("Sunday", "total_market", 29305.0859375, null, null),
        array("Sunday", "spot", 13585.540908813477, null, NaN),
        array("Thursday", "upfront", 28562.748779296875, null, null),
        array("Thursday", "total_market", 32361.38690185547, null, null),
        array("Thursday", "spot", 14279.127326965332, null, NaN),
        array("Tuesday", "upfront", 26968.28009033203, null, null),
        array("Tuesday", "total_market", 29676.578247070312, null, null),
        array("Tuesday", "spot", 13199.471267700195, null, NaN),
        array("Wednesday", "upfront", 28985.57501220703, null, null),
        array("Wednesday", "total_market", 32753.337280273438, null, null),
        array("Wednesday", "spot", 14271.368713378906, NaN, NaN)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    // don't know what the hell is npv
    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "npv_all = $npv(index, 0.1)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "npv_week = $npv(index, 0.1)")
            )
        )
    );

    columnNames = array("dayOfWeek", "market", "index", "npv_all", "npv_week");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", "upfront", 27297.862365722656, null, null),
        array("Friday", "total_market", 30173.691955566406, null, null),
        array("Friday", "spot", 13219.574020385742, null, 59685.23563789121),
        array("Monday", "upfront", 27619.58477783203, null, null),
        array("Monday", "total_market", 30468.776733398438, null, null),
        array("Monday", "spot", 13557.73889541626, null, 60475.65055096265),
        array("Saturday", "upfront", 27820.831176757812, null, null),
        array("Saturday", "total_market", 30940.971740722656, null, null),
        array("Saturday", "spot", 13493.751190185547, null, 61000.770720403765),
        array("Sunday", "upfront", 24791.22381591797, null, null),
        array("Sunday", "total_market", 29305.0859375, null, null),
        array("Sunday", "spot", 13585.540908813477, null, 56963.57344652458),
        array("Thursday", "upfront", 28562.748779296875, null, null),
        array("Thursday", "total_market", 32361.38690185547, null, null),
        array("Thursday", "spot", 14279.127326965332, null, 63439.202811386596),
        array("Tuesday", "upfront", 26968.28009033203, null, null),
        array("Tuesday", "total_market", 29676.578247070312, null, null),
        array("Tuesday", "spot", 13199.471267700195, null, 58959.67411628796),
        array("Wednesday", "upfront", 28985.57501220703, null, null),
        array("Wednesday", "total_market", 32753.337280273438, null, null),
        array("Wednesday", "spot", 14271.368713378906, 209577.55611065833, 64141.68706720525)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "market", "index", "min_week", "min_all", "min_all[upfront]", "min_week[spot]");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday",
              list("upfront", "total_market", "spot"),
              list(27297.862365722656D, 30173.691955566406D, 13219.574020385742D),
              list(27297.862365722656D, 27297.862365722656D, 13219.574020385742D),
              list(27297.862365722656D, 27297.862365722656D, 13219.574020385742D),
              27297.862365722656, 13219.574020385742),
        array("Monday",
              list("upfront", "total_market", "spot"),
              list(27619.58477783203D, 30468.776733398438D, 13557.73889541626D),
              list(27619.58477783203D, 27619.58477783203D, 13557.73889541626D),
              list(13219.574020385742D, 13219.574020385742D, 13219.574020385742D),
              13219.574020385742, 13557.73889541626),
        array("Saturday",
              list("upfront", "total_market", "spot"),
              list(27820.831176757812D, 30940.971740722656D, 13493.751190185547D),
              list(27820.831176757812D, 27820.831176757812D, 13493.751190185547D),
              list(13219.574020385742D, 13219.574020385742D, 13219.574020385742D),
              13219.574020385742, 13493.751190185547),
        array("Sunday",
              list("upfront", "total_market", "spot"),
              list(24791.22381591797D, 29305.0859375D, 13585.540908813477D),
              list(24791.22381591797D, 24791.22381591797D, 13585.540908813477D),
              list(13219.574020385742D, 13219.574020385742D, 13219.574020385742D),
              13219.574020385742, 13585.540908813477),
        array("Thursday",
              list("upfront", "total_market", "spot"),
              list(28562.748779296875D, 32361.38690185547D, 14279.127326965332D),
              list(28562.748779296875D, 28562.748779296875D, 14279.127326965332D),
              list(13219.574020385742D, 13219.574020385742D, 13219.574020385742D),
              13219.574020385742, 14279.127326965332),
        array("Tuesday",
              list("upfront", "total_market", "spot"),
              list(26968.28009033203D, 29676.578247070312D, 13199.471267700195D),
              list(26968.28009033203D, 26968.28009033203D, 13199.471267700195D),
              list(13219.574020385742D, 13219.574020385742D, 13199.471267700195D),
              13219.574020385742, 13199.471267700195),
        array("Wednesday",
              list("upfront", "total_market", "spot"),
              list(28985.57501220703D, 32753.337280273438D, 14271.368713378906D),
              list(28985.57501220703D, 28985.57501220703D, 14271.368713378906D),
              list(13199.471267700195D, 13199.471267700195D, 13199.471267700195D),
              13199.471267700195, 14271.368713378906)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    // order by on partition sum.. NMC requirement
    // changed identifier spec to accept index. use '_' for minus instead of '-'
    builder.setLimitSpec(
        new LimitSpec(
            OrderByColumnSpec.descending("sum_week_last"),
            null,
            Arrays.asList(
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(indexDsc), Arrays.asList("sum_week = $sum(index)"),
                    FlattenSpec.array(Arrays.asList("market", "index", "sum_week"), null)
                               .withExpression(
                                   "sum_week_first=\"sum_week\"[0]", "sum_week_last=\"sum_week\"[-1]"
                               )
                )
            )
        )
    );

    columnNames = array("dayOfWeek", "market", "index", "sum_week", "sum_week_first", "sum_week_last");
    expectedResults = createExpectedRows(
        columnNames,
        array("Wednesday",
              list("total_market", "upfront", "spot"),
              list(32753.337280273438D, 28985.57501220703D, 14271.368713378906D),
              list(32753.337280273438D, 61738.91229248047D, 76010.28100585938D),
              32753.337280273438, 76010.28100585938),
        array("Thursday",
              list("total_market", "upfront", "spot"),
              list(32361.38690185547D, 28562.748779296875D, 14279.127326965332D),
              list(32361.38690185547D, 60924.135681152344D, 75203.26300811768D),
              32361.38690185547, 75203.26300811768),
        array("Saturday",
              list("total_market", "upfront", "spot"),
              list(30940.971740722656D, 27820.831176757812D, 13493.751190185547D),
              list(30940.971740722656D, 58761.80291748047D, 72255.55410766602D),
              30940.971740722656, 72255.55410766602),
        array("Monday",
              list("total_market", "upfront", "spot"),
              list(30468.776733398438D, 27619.58477783203D, 13557.73889541626D),
              list(30468.776733398438D, 58088.36151123047D, 71646.10040664673D),
              30468.776733398438, 71646.10040664673),
        array("Friday",
              list("total_market", "upfront", "spot"),
              list(30173.691955566406D, 27297.862365722656D, 13219.574020385742D),
              list(30173.691955566406D, 57471.55432128906D, 70691.1283416748D),
              30173.691955566406, 70691.1283416748),
        array("Tuesday",
              list("total_market", "upfront", "spot"),
              list(29676.578247070312D, 26968.28009033203D, 13199.471267700195D),
              list(29676.578247070312D, 56644.858337402344D, 69844.32960510254D),
              29676.578247070312, 69844.32960510254),
        array("Sunday",
              list("total_market", "upfront", "spot"),
              list(29305.0859375D, 24791.22381591797D, 13585.540908813477D),
              list(29305.0859375D, 54096.30975341797D, 67681.85066223145D),
              29305.0859375, 67681.85066223145)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("rows", "columns");
    expectedResults = createExpectedRows(
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
                    27297.862365722656, 30173.691955566406, 13219.574020385742, 27619.58477783203, 30468.776733398438,
                    13557.73889541626, 27820.831176757812, 30940.971740722656, 13493.751190185547, 24791.22381591797,
                    29305.0859375, 13585.540908813477, 28562.748779296875, 32361.38690185547, 14279.127326965332,
                    26968.28009033203, 29676.578247070312, 13199.471267700195, 28985.57501220703, 32753.337280273438,
                    14271.368713378906
                ),
                "min_all",
                Arrays.asList(
                    27297.862365722656, 27297.862365722656, 13219.574020385742, 13219.574020385742, 13219.574020385742,
                    13219.574020385742, 13219.574020385742, 13219.574020385742, 13219.574020385742, 13219.574020385742,
                    13219.574020385742, 13219.574020385742, 13219.574020385742, 13219.574020385742, 13219.574020385742,
                    13219.574020385742, 13219.574020385742, 13199.471267700195, 13199.471267700195, 13199.471267700195,
                    13199.471267700195
                )
            )
        )
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("rows", "columns");
    expectedResults = createExpectedRows(
        columnNames,
        array(
            Arrays.asList("Friday", "Monday", "Saturday", "Sunday", "Thursday", "Tuesday", "Wednesday"),
            ImmutableMap.builder().put(
                "upfront-index",
                Arrays.asList(27297.862365722656, 27619.58477783203, 27820.831176757812, 24791.22381591797,
                              28562.748779296875, 26968.28009033203, 28985.57501220703)).put(
                "upfront-min_all",
                Arrays.asList(27297.862365722656, 13219.574020385742, 13219.574020385742, 13219.574020385742,
                              13219.574020385742, 13219.574020385742, 13199.471267700195)).put(
                "spot-index",
                Arrays.asList(13219.574020385742, 13557.73889541626, 13493.751190185547, 13585.540908813477,
                              14279.127326965332, 13199.471267700195, 14271.368713378906)).put(
                "spot-min_all",
                Arrays.asList(13219.574020385742, 13219.574020385742, 13219.574020385742, 13219.574020385742,
                              13219.574020385742, 13199.471267700195, 13199.471267700195)).put(
                "total_market-index",
                Arrays.asList(30173.691955566406, 30468.776733398438, 30940.971740722656, 29305.0859375,
                              32361.38690185547, 29676.578247070312, 32753.337280273438)).put(
                "total_market-min_all",
                Arrays.asList(27297.862365722656, 13219.574020385742, 13219.574020385742, 13219.574020385742,
                              13219.574020385742, 13219.574020385742, 13199.471267700195)).build())
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("rows", "columns");
    expectedResults = createExpectedRows(columnNames,
        array(
            Arrays.asList("upfront", "total_market", "spot"),
            ImmutableMap.builder().put(
                "Friday-index", list(27297.862365722656, 30173.691955566406, 13219.574020385742)).put(
                "Friday-min_all", list(27297.862365722656, 27297.862365722656, 13219.574020385742)).put(
                "Wednesday-index", list(28985.57501220703, 32753.337280273438, 14271.368713378906)).put(
                "Wednesday-min_all", list(13199.471267700195, 13199.471267700195, 13199.471267700195)).put(
                "Thursday-index", list(28562.748779296875, 32361.38690185547, 14279.127326965332)).put(
                "Thursday-min_all", list(13219.574020385742, 13219.574020385742, 13219.574020385742)).put(
                "Sunday-index", list(24791.22381591797, 29305.0859375, 13585.540908813477)).put(
                "Sunday-min_all", list(13219.574020385742, 13219.574020385742, 13219.574020385742)).put(
                "Monday-index", list(27619.58477783203, 30468.776733398438, 13557.73889541626)).put(
                "Monday-min_all", list(13219.574020385742, 13219.574020385742, 13219.574020385742)).put(
                "Tuesday-index", list(26968.28009033203, 29676.578247070312, 13199.471267700195)).put(
                "Tuesday-min_all", list(13219.574020385742, 13219.574020385742, 13199.471267700195)).put(
                "Saturday-index", list(27820.831176757812, 30940.971740722656, 13493.751190185547)).put(
                "Saturday-min_all", list(13219.574020385742, 13219.574020385742, 13219.574020385742)).build()
        )
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusMarket, "sum_all = $sum(addRowsIndexConstant)"),
                new WindowingSpec(dayOfWeek, Arrays.asList(marketDsc), "sum_week = $sum(addRowsIndexConstant)")
            )
        )
    );

    columnNames = array("dayOfWeek", "addRowsIndexConstant", "sum_week", "sum_all");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27324.862365722656, 27324.862365722656, 27324.862365722656),
        array("Friday", 30200.691955566406, 57525.55432128906, 57525.55432128906),
        array("Friday", 13337.574020385742, 70863.1283416748, 70863.1283416748),
        array("Monday", 27646.58477783203, 27646.58477783203, 98509.71311950684),
        array("Monday", 30495.776733398438, 58142.36151123047, 129005.48985290527),
        array("Monday", 13675.73889541626, 71818.10040664673, 142681.22874832153),
        array("Saturday", 27847.831176757812, 27847.831176757812, 170529.05992507935),
        array("Saturday", 30967.971740722656, 58815.80291748047, 201497.031665802),
        array("Saturday", 13611.751190185547, 72427.55410766602, 215108.78285598755),
        array("Sunday", 24818.22381591797, 24818.22381591797, 239927.00667190552),
        array("Sunday", 29332.0859375, 54150.30975341797, 269259.0926094055),
        array("Sunday", 13703.540908813477, 67853.85066223145, 282962.633518219),
        array("Thursday", 28591.748779296875, 28591.748779296875, 311554.38229751587),
        array("Thursday", 32390.38690185547, 60982.135681152344, 343944.76919937134),
        array("Thursday", 14406.127326965332, 75388.26300811768, 358350.89652633667),
        array("Tuesday", 26995.28009033203, 26995.28009033203, 385346.1766166687),
        array("Tuesday", 29703.578247070312, 56698.858337402344, 415049.754863739),
        array("Tuesday", 13317.471267700195, 70016.32960510254, 428367.2261314392),
        array("Wednesday", 29014.57501220703, 29014.57501220703, 457381.80114364624),
        array("Wednesday", 32782.33728027344, 61796.91229248047, 490164.1384239197),
        array("Wednesday", 14398.368713378906, 76195.28100585938, 504562.5071372986)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "sum_week", "sum_week_ratio_permil", "sum_all", "sum_all_ratio_percent");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27297.862365722656, 386.15683475559473, 27297.862365722656, 5.423425266327249),
        array("Friday", 57471.55432128906, 812.9952890765735, 57471.55432128906, 11.418208342663636),
        array("Friday", 70691.1283416748, 1000.0, 70691.1283416748, 14.044618088295207),
        array("Monday", 27619.58477783203, 385.5001824393741, 98310.71311950684, 19.531961819563094),
        array("Monday", 58088.36151123047, 810.7679438452943, 128779.48985290527, 25.585371106932485),
        array("Monday", 71646.10040664673, 1000.0, 142337.22874832153, 28.278966037354493),
        array("Saturday", 27820.831176757812, 385.03380840873155, 170158.05992507935, 33.806292562515495),
        array("Saturday", 58761.80291748047, 813.2496337917653, 201099.031665802, 39.953515581489434),
        array("Saturday", 72255.55410766602, 1000.0, 214592.78285598755, 42.63439770192532),
        array("Sunday", 24791.22381591797, 366.2905723373228, 239384.00667190552, 47.55981449189542),
        array("Sunday", 54096.30975341797, 799.2735013022535, 268689.0926094055, 53.382026552899106),
        array("Sunday", 67681.85066223145, 1000.0, 282274.633518219, 56.08114507120844),
        array("Thursday", 28562.748779296875, 379.80730671505205, 310837.38229751587, 61.755872686507395),
        array("Thursday", 60924.135681152344, 810.126226498656, 343198.76919937134, 68.18529785634408),
        array("Thursday", 75203.26300811768, 1000.0, 357477.89652633667, 71.0222152269661),
        array("Tuesday", 26968.28009033203, 386.1198216492272, 384446.1766166687, 76.38016046354818),
        array("Tuesday", 56644.858337402344, 811.0158499289842, 414122.754863739, 82.27617906482145)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "index", "mean_all", "count_all", "mean_week", "count_week");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27297.862365722656, 28735.77716064453, 21L, 28735.77716064453, 3L),
        array("Friday", 30173.691955566406, 23563.709447224934, 21L, 23563.709447224934, 3L),
        array("Friday", 13219.574020385742, 23670.950251261394, 21L, 21696.632987976074, 3L),
        array("Monday", 27619.58477783203, 23769.31184387207, 21L, 29044.180755615234, 3L),
        array("Monday", 30468.776733398438, 23882.033468882244, 21L, 23882.033468882244, 3L),
        array("Monday", 13557.73889541626, 23949.115601857502, 21L, 22013.25781440735, 3L),
        array("Saturday", 27820.831176757812, 24106.513937632244, 21L, 29380.901458740234, 3L),
        array("Saturday", 30940.971740722656, 24085.18470255534, 21L, 24085.18470255534, 3L),
        array("Saturday", 13493.751190185547, 23075.31558227539, 21L, 22217.3614654541, 3L),
        array("Sunday", 24791.22381591797, 22530.020314534504, 21L, 27048.154876708984, 3L),
        array("Sunday", 29305.0859375, 22560.61688741048, 21L, 22560.61688741048, 3L),
        array("Sunday", 13585.540908813477, 23817.79187520345, 21L, 21445.31342315674, 3L),
        array("Thursday", 28562.748779296875, 24836.55886332194, 21L, 30462.067840576172, 3L),
        array("Thursday", 32361.38690185547, 25067.754336039226, 21L, 25067.754336039226, 3L),
        array("Thursday", 14279.127326965332, 24536.264773050945, 21L, 23320.2571144104, 3L),
        array("Tuesday", 26968.28009033203, 23641.328554789226, 21L, 28322.429168701172, 3L),
        array("Tuesday", 29676.578247070312, 23281.443201700848, 21L, 23281.443201700848, 3L),
        array("Tuesday", 13199.471267700195, 23953.874842325848, 21L, 21438.024757385254, 3L)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
                    dayOfWeek, Arrays.asList(rowsAsc, OrderByColumnSpec.asc("row_num_all")), // topN sorting makes different result (#3785)
                    "row_num_week = $row_num(rows)",
                    "rank_week = $rank(rows)",
                    "dense_rank_week = $dense_rank(rows)"
                )
            )
        )
    );

    columnNames = array(
        "dayOfWeek", "rows", "row_num_all", "rank_all", "dense_rank_all", "row_num_week", "rank_week", "dense_rank_week"
    );

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 26, 1L, 1L, 1L, 1L, 1L, 1L),
        array("Friday", 26, 2L, 1L, 1L, 2L, 1L, 1L),
        array("Friday", 117, 3L, 3L, 2L, 3L, 3L, 2L),
        array("Monday", 26, 4L, 4L, 3L, 1L, 1L, 1L),
        array("Monday", 26, 5L, 4L, 3L, 2L, 1L, 1L),
        array("Monday", 117, 6L, 6L, 4L, 3L, 3L, 2L),
        array("Saturday", 26, 7L, 7L, 5L, 1L, 1L, 1L),
        array("Saturday", 26, 8L, 7L, 5L, 2L, 1L, 1L),
        array("Saturday", 117, 9L, 9L, 6L, 3L, 3L, 2L),
        array("Sunday", 26, 10L, 10L, 7L, 1L, 1L, 1L),
        array("Sunday", 26, 11L, 10L, 7L, 2L, 1L, 1L),
        array("Sunday", 117, 12L, 12L, 8L, 3L, 3L, 2L),
        array("Thursday", 28, 13L, 13L, 9L, 1L, 1L, 1L),
        array("Thursday", 28, 14L, 13L, 9L, 2L, 1L, 1L),
        array("Thursday", 126, 15L, 15L, 10L, 3L, 3L, 2L),
        array("Tuesday", 26, 16L, 16L, 11L, 1L, 1L, 1L),
        array("Tuesday", 26, 17L, 16L, 11L, 2L, 1L, 1L)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "rows", "lead_all", "lag_all", "lead_week", "lag_week");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 26, 117, null, 26, null),
        array("Friday", 26, 26, null, 117, 26),
        array("Friday", 117, 26, 26, null, 26),
        array("Monday", 26, 117, 26, 26, null),
        array("Monday", 26, 26, 117, 117, 26),
        array("Monday", 117, 26, 26, null, 26),
        array("Saturday", 26, 117, 26, 26, null),
        array("Saturday", 26, 26, 117, 117, 26),
        array("Saturday", 117, 26, 26, null, 26),
        array("Sunday", 26, 117, 26, 26, null),
        array("Sunday", 26, 28, 117, 117, 26),
        array("Sunday", 117, 28, 26, null, 26),
        array("Thursday", 28, 126, 26, 28, null),
        array("Thursday", 28, 26, 117, 126, 28),
        array("Thursday", 126, 26, 28, null, 28),
        array("Tuesday", 26, 117, 28, 26, null),
        array("Tuesday", 26, 28, 126, 117, 26),
        array("Tuesday", 117, 28, 26, null, 26),
        array("Wednesday", 28, 126, 26, 28, null),
        array("Wednesday", 28, null, 117, 126, 28),
        array("Wednesday", 126, null, 28, null, 28)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "var_all", "stddev_all", "var_week", "stddev_week");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 8.231845225404745E7, 9072.951683661026, 0.0, 0.0),
        array("Friday", 6.679895672601085E7, 8173.06287299999, 2067598.9574552178, 1437.914794921875),
        array("Friday", 5.1184632705608554E7, 7154.343625072013, 5.487896816936497E7, 7408.034028631683),
        array("Monday", 6.969968318306601E7, 8348.63361173947, 0.0, 0.0),
        array("Monday", 8.12162782754362E7, 9012.007449810291, 2029473.6999160806, 1424.5959777832031),
        array("Monday", 5.127455815817303E7, 7160.625542379174, 5.464851168646029E7, 7392.463167744584),
        array("Saturday", 7.166847393316415E7, 8465.72347370053, 0.0, 0.0),
        array("Saturday", 7.63939531468838E7, 8740.363444782133, 2433819.2847247133, 1560.0702819824219),
        array("Saturday", 4.846446016742742E7, 6961.642059703115, 5.771177811329142E7, 7596.826845024929),
        array("Sunday", 5.949881195296468E7, 7713.547300235131, 0.0, 0.0),
        array("Sunday", 7.729272054922533E7, 8791.627866852949, 5093737.813163259, 2256.9310607910156),
        array("Sunday", 5.328744931253184E7, 7299.825293288315, 4.367181961957E7, 6608.465753831974),
        array("Thursday", 7.151765434438092E7, 8456.811121479592, 0.0, 0.0),
        array("Thursday", 8.136008031186101E7, 9019.982278910586, 3607412.8965388695, 1899.3190612792969),
        array("Thursday", 4.9546366069246866E7, 7038.917961536906, 6.060217830148526E7, 7784.740092095899),
        array("Tuesday", 6.557534318900989E7, 8097.860408096072, 0.0, 0.0),
        array("Tuesday", 8.171276109404276E7, 9039.511109238307, 1833719.726447993, 1354.1490783691406),
        array("Tuesday", 5.808205533999761E7, 7621.158398826101, 5.204555885662041E7, 7214.260797657679),
        array("Wednesday", 7.966593968129846E7, 8925.577834588552, 0.0, 0.0),
        array("Wednesday", 1.0041589776873899E8, 10020.773311912559, 3549008.1271662274, 1883.8811340332031),
        array("Wednesday", 9.538117693680261E7, 9766.32873380794, 6.358745129120174E7, 7974.174019370391)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "p5_all", "p5_all_win", "p3_week", "p7_week");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 30173.691955566406, 27619.58477783203, 30173.691955566406, 30173.691955566406),
        array("Friday", 30173.691955566406, 27619.58477783203, 27297.862365722656, 30173.691955566406),
        array("Friday", 27297.862365722656, 27820.831176757812, 13219.574020385742, 30173.691955566406),
        array("Monday", 30173.691955566406, 27619.58477783203, 30468.776733398438, 30468.776733398438),
        array("Monday", 27619.58477783203, 27820.831176757812, 27619.58477783203, 30468.776733398438),
        array("Monday", 27619.58477783203, 27619.58477783203, 13557.73889541626, 30468.776733398438),
        array("Saturday", 27619.58477783203, 27297.862365722656, 30940.971740722656, 30940.971740722656),
        array("Saturday", 27820.831176757812, 27619.58477783203, 27820.831176757812, 30940.971740722656),
        array("Saturday", 27619.58477783203, 27820.831176757812, 13493.751190185547, 30940.971740722656),
        array("Sunday", 27820.831176757812, 27619.58477783203, 29305.0859375, 29305.0859375),
        array("Sunday", 27619.58477783203, 27820.831176757812, 24791.22381591797, 29305.0859375),
        array("Sunday", 27619.58477783203, 27820.831176757812, 13585.540908813477, 29305.0859375),
        array("Thursday", 27619.58477783203, 26968.28009033203, 32361.38690185547, 32361.38690185547),
        array("Thursday", 27820.831176757812, 26968.28009033203, 28562.748779296875, 32361.38690185547),
        array("Thursday", 27619.58477783203, 28562.748779296875, 14279.127326965332, 32361.38690185547),
        array("Tuesday", 27820.831176757812, 26968.28009033203, 29676.578247070312, 29676.578247070312),
        array("Tuesday", 27619.58477783203, 28562.748779296875, 26968.28009033203, 29676.578247070312),
        array("Tuesday", 27619.58477783203, 28562.748779296875, 13199.471267700195, 29676.578247070312),
        array("Wednesday", 27619.58477783203, 28562.748779296875, 32753.337280273438, 32753.337280273438),
        array("Wednesday", 27820.831176757812, 26968.28009033203, 28985.57501220703, 32753.337280273438),
        array("Wednesday", 27619.58477783203, 28985.57501220703, 14271.368713378906, 32753.337280273438)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "p5_all", "p5_all_first_two", "p5_week", "p5_week_last");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 30173.691955566406, 27619.58477783203, 30173.691955566406, null),
        array("Friday", 30173.691955566406, 27619.58477783203, 30173.691955566406, null),
        array("Friday", 27297.862365722656, null, 27297.862365722656, 27297.862365722656),
        array("Monday", 30173.691955566406, null, 30468.776733398438, null),
        array("Monday", 27619.58477783203, null, 30468.776733398438, null),
        array("Monday", 27619.58477783203, null, 27619.58477783203, 27619.58477783203),
        array("Saturday", 27619.58477783203, null, 30940.971740722656, null),
        array("Saturday", 27820.831176757812, null, 30940.971740722656, null),
        array("Saturday", 27619.58477783203, null, 27820.831176757812, 27820.831176757812),
        array("Sunday", 27820.831176757812, null, 29305.0859375, null),
        array("Sunday", 27619.58477783203, null, 29305.0859375, null),
        array("Sunday", 27619.58477783203, null, 24791.22381591797, 24791.22381591797),
        array("Thursday", 27619.58477783203, null, 32361.38690185547, null),
        array("Thursday", 27820.831176757812, null, 32361.38690185547, null),
        array("Thursday", 27619.58477783203, null, 28562.748779296875, 28562.748779296875),
        array("Tuesday", 27820.831176757812, null, 29676.578247070312, null),
        array("Tuesday", 27619.58477783203, null, 29676.578247070312, null),
        array("Tuesday", 27619.58477783203, null, 26968.28009033203, 26968.28009033203),
        array("Wednesday", 27619.58477783203, null, 32753.337280273438, null),
        array("Wednesday", 27820.831176757812, null, 32753.337280273438, null),
        array("Wednesday", 27619.58477783203, null, 28985.57501220703, 28985.57501220703)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, dayPlusRows,
                    "p5_all = $median(index)", "$assign(p5_all_first_two, 0, 2) = $last(p5_all)"
                ),
                new WindowingSpec(
                    dayOfWeek, Arrays.asList(rowsAsc),
                    Arrays.asList("p5_week = $median(index)", "$assign(p5_week_last, -1) = $last(p5_week)"),
                    FlattenSpec.array(Arrays.asList(columnNames), null)
                )
            )
        )
    );

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday",
              list(30173.691955566406D, 28735.77716064453D, 27297.862365722656D),
              list(27619.58477783203D, 27619.58477783203D, null),
              list(30173.691955566406D, 28735.77716064453D, 27297.862365722656D),
              list(null, null, 27297.862365722656D)),
        array("Monday",
              list(28735.77716064453D, 27619.58477783203D, 27458.723571777344D),
              list(null, null, null),
              list(30468.776733398438D, 29044.180755615234D, 27619.58477783203D),
              list(null, null, 27619.58477783203D)),
        array("Saturday",
              list(27619.58477783203D, 27720.207977294922D, 27619.58477783203D),
              list(null, null, null),
              list(30940.971740722656D, 29380.901458740234D, 27820.831176757812D),
              list(null, null, 27820.831176757812D)),
        array("Sunday",
              list(27720.207977294922D, 27619.58477783203D, 27458.723571777344D),
              list(null, null, null),
              list(29305.0859375D, 27048.154876708984D, 24791.22381591797D),
              list(null, null, 24791.22381591797D)),
        array("Thursday",
              list(27619.58477783203D, 27720.207977294922D, 27619.58477783203D),
              list(null, null, null),
              list(32361.38690185547D, 30462.067840576172D, 28562.748779296875D),
              list(null, null, 28562.748779296875D)),
        array("Tuesday",
              list(27720.207977294922D, 27619.58477783203D, 27458.723571777344D),
              list(null, null, null),
              list(29676.578247070312D, 28322.429168701172D, 26968.28009033203D),
              list(null, null, 26968.28009033203D)),
        array("Wednesday",
              list(27619.58477783203D, 27720.207977294922D, 27619.58477783203D),
              list(null, null, null),
              list(32753.337280273438D, 30869.456146240234D, 28985.57501220703D),
              list(null, null, 28985.57501220703D))
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek",
        "0.p5_all", "0.p5_all_first_two", "0.p5_week", "0.p5_week_last",
        "1.p5_all", "1.p5_all_first_two", "1.p5_week", "1.p5_week_last",
        "2.p5_all", "2.p5_all_first_two", "2.p5_week", "2.p5_week_last");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday",
              30173.691955566406, 27619.58477783203, 30173.691955566406, null, 30173.691955566406, 27619.58477783203,
              30173.691955566406, null, 27297.862365722656, null, 27297.862365722656, 27297.862365722656),
        array("Monday",
              30173.691955566406, null, 30468.776733398438, null, 27619.58477783203, null,
              30468.776733398438, null, 27619.58477783203, null, 27619.58477783203, 27619.58477783203),
        array("Saturday",
              27619.58477783203, null, 30940.971740722656, null, 27820.831176757812, null,
              30940.971740722656, null, 27619.58477783203, null, 27820.831176757812, 27820.831176757812),
        array("Sunday",
              27820.831176757812, null, 29305.0859375, null, 27619.58477783203, null,
              29305.0859375, null, 27619.58477783203, null, 24791.22381591797, 24791.22381591797),
        array("Thursday",
              27619.58477783203, null, 32361.38690185547, null, 27820.831176757812, null,
              32361.38690185547, null, 27619.58477783203, null, 28562.748779296875, 28562.748779296875),
        array("Tuesday",
              27820.831176757812, null, 29676.578247070312, null, 27619.58477783203, null,
              29676.578247070312, null, 27619.58477783203, null, 26968.28009033203, 26968.28009033203),
        array("Wednesday",
              27619.58477783203, null, 32753.337280273438, null, 27820.831176757812, null,
              32753.337280273438, null, 27619.58477783203, null, 28985.57501220703, 28985.57501220703)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec( null, dayPlusRows, "index_bin = $histogram(index, 3)")
            )
        )
    );

    columnNames = array("index", "index_bin");

    expectedResults = createExpectedRows(
        columnNames,
        array(30173.691955566406, null),
        array(27297.862365722656, null),
        array(13219.574020385742, null),
        array(30468.776733398438, null),
        array(27619.58477783203, null),
        array(13557.73889541626, null),
        array(30940.971740722656, null),
        array(27820.831176757812, null),
        array(13493.751190185547, null),
        array(29305.0859375, null),
        array(24791.22381591797, null),
        array(13585.540908813477, null),
        array(32361.38690185547, null),
        array(28562.748779296875, null),
        array(14279.127326965332, null),
        array(29676.578247070312, null),
        array(26968.28009033203, null),
        array(13199.471267700195, null),
        array(32753.337280273438, null),
        array(28985.57501220703, null),
        array(14271.368713378906, ImmutableMap.of(
            "min", 13199.471267700195,
            "max", 32753.337280273438,
            "breaks", Doubles.asList(13199.471267700195, 19717.42660522461, 26235.381942749023, 32753.337280273438),
            "counts", Ints.asList(7, 1, 13))
        )
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(null, dayPlusRows, "index_bin = $histogram(index, 8, 26000, 1000)")
            )
        )
    );

    columnNames = array("index", "index_bin");

    expectedResults = createExpectedRows(
        columnNames,
        array(30173.691955566406, null),
        array(27297.862365722656, null),
        array(13219.574020385742, null),
        array(30468.776733398438, null),
        array(27619.58477783203, null),
        array(13557.73889541626, null),
        array(30940.971740722656, null),
        array(27820.831176757812, null),
        array(13493.751190185547, null),
        array(29305.0859375, null),
        array(24791.22381591797, null),
        array(13585.540908813477, null),
        array(32361.38690185547, null),
        array(28562.748779296875, null),
        array(14279.127326965332, null),
        array(29676.578247070312, null),
        array(26968.28009033203, null),
        array(13199.471267700195, null),
        array(32753.337280273438, null),
        array(28985.57501220703, null),
        array(14271.368713378906, ImmutableMap.of(
            "min", 13199.471267700195,
            "max", 32753.337280273438,
            "breaks", Doubles.asList(26000, 27000, 28000, 29000, 30000, 31000, 32000, 33000, 34000),
            "counts", Ints.asList(1, 3, 2, 2, 3, 0, 2, 0))
        )
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);
  }

  @Test
  public void testPivot()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek");
    OrderByColumnSpec marketDesc = OrderByColumnSpec.desc("marketDesc");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
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
    String[] columnNames = array("dayOfWeek", "upfront", "spot", "total_market");

    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27297.862365722656, 13219.574020385742, 30173.691955566406),
        array("Monday", 27619.58477783203, 13557.73889541626, 30468.776733398438),
        array("Saturday", 27820.831176757812, 13493.751190185547, 30940.971740722656),
        array("Sunday", 24791.22381591797, 13585.540908813477, 29305.0859375),
        array("Thursday", 28562.748779296875, 14279.127326965332, 32361.38690185547),
        array("Tuesday", 26968.28009033203, 13199.471267700195, 29676.578247070312),
        array("Wednesday", 28985.57501220703, 14271.368713378906, 32753.337280273438)
    );

    List<Row> results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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
    columnNames = array("dayOfWeek", "upfront-index", "spot-index", "total_market-index");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27297.862365722656, 13219.574020385742, 30173.691955566406),
        array("Monday", 27619.58477783203, 13557.73889541626, 30468.776733398438),
        array("Saturday", 27820.831176757812, 13493.751190185547, 30940.971740722656),
        array("Sunday", 24791.22381591797, 13585.540908813477, 29305.0859375),
        array("Thursday", 28562.748779296875, 14279.127326965332, 32361.38690185547),
        array("Tuesday", 26968.28009033203, 13199.471267700195, 29676.578247070312),
        array("Wednesday", 28985.57501220703, 14271.368713378906, 32753.337280273438)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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
    columnNames = array("dayOfWeek", "spot", "total_market", "upfront");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", list(13219.574020385742D, 117), list(30173.691955566406D, 26), list(27297.862365722656D, 26)),
        array("Monday", list(13557.73889541626D, 117), list(30468.776733398438D, 26), list(27619.58477783203D, 26)),
        array("Saturday", list(13493.751190185547D, 117), list(30940.971740722656D, 26), list(27820.831176757812D, 26)),
        array("Sunday", list(13585.540908813477D, 117), list(29305.0859375D, 26), list(24791.22381591797D, 26)),
        array("Thursday", list(14279.127326965332D, 126), list(32361.38690185547D, 28), list(28562.748779296875D, 28)),
        array("Tuesday", list(13199.471267700195D, 117), list(29676.578247070312D, 26), list(26968.28009033203D, 26)),
        array("Wednesday", list(14271.368713378906D, 126), list(32753.337280273438D, 28), list(28985.57501220703D, 28))
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "spot-index", "spot-rows", "total_market-index", "total_market-rows", "upfront-index", "upfront-rows");
    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 13219.574020385742, 117, 30173.691955566406, 26, 27297.862365722656, 26),
        array("Monday", 13557.73889541626, 117, 30468.776733398438, 26, 27619.58477783203, 26),
        array("Saturday", 13493.751190185547, 117, 30940.971740722656, 26, 27820.831176757812, 26),
        array("Sunday", 13585.540908813477, 117, 29305.0859375, 26, 24791.22381591797, 26),
        array("Thursday", 14279.127326965332, 126, 32361.38690185547, 28, 28562.748779296875, 28),
        array("Tuesday", 13199.471267700195, 117, 29676.578247070312, 26, 26968.28009033203, 26),
        array("Wednesday", 14271.368713378906, 126, 32753.337280273438, 28, 28985.57501220703, 28)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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

    columnNames = array("dayOfWeek", "upfront", "spot", "total_market");
    expectedResults = createExpectedRows(
        columnNames,
        array("Monday", 27619.58477783203, 13557.73889541626, 30468.776733398438),
        array("Tuesday", 26968.28009033203, 13199.471267700195, 29676.578247070312),
        array("Wednesday", 28985.57501220703, 14271.368713378906, 32753.337280273438),
        array("Thursday", 28562.748779296875, 14279.127326965332, 32361.38690185547),
        array("Friday", 27297.862365722656, 13219.574020385742, 30173.691955566406),
        array("Saturday", 27820.831176757812, 13493.751190185547, 30940.971740722656),
        array("Sunday", 24791.22381591797, 13585.540908813477, 29305.0859375)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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
    columnNames = array("dayOfWeek", "upfront", "spot");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27297.862365722656, 13219.574020385742),
        array("Monday", 27619.58477783203, 13557.73889541626),
        array("Saturday", 27820.831176757812, 13493.751190185547),
        array("Sunday", 24791.22381591797, 13585.540908813477),
        array("Thursday", 28562.748779296875, 14279.127326965332),
        array("Tuesday", 26968.28009033203, 13199.471267700195),
        array("Wednesday", 28985.57501220703, 14271.368713378906)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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
    columnNames = array("dayOfWeek", "upfr", "spot");

    expectedResults = createExpectedRows(
        columnNames,
        array("Friday", 27297.862365722656, 13219.574020385742),
        array("Monday", 27619.58477783203, 13557.73889541626),
        array("Saturday", 27820.831176757812, 13493.751190185547),
        array("Sunday", 24791.22381591797, 13585.540908813477),
        array("Thursday", 28562.748779296875, 14279.127326965332),
        array("Tuesday", 26968.28009033203, 13199.471267700195),
        array("Wednesday", 28985.57501220703, 14271.368713378906)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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
    columnNames = array("market", "Monday", "Wednesday", "Friday", "sum");

    expectedResults = createExpectedRows(
        columnNames,
        array("spot", 13557.73889541626, 14271.368713378906, 13219.574020385742, 41048.68162918091),
        array("total_market", 30468.776733398438, 32753.337280273438, 30173.691955566406, 93395.80596923828),
        array("upfront", 27619.58477783203, 28985.57501220703, 27297.862365722656, 83903.02215576172)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);

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
    columnNames = array("market", "Monday", "Wednesday", "Friday", "sum");

    expectedResults = createExpectedRows(
        columnNames,
        array("spot", 13557.73889541626, 0.0, 18.700471092342656, 41048.68162918091),
        array("total_market", 44026.5156288147, 18481.96856689453, 42.683845432097876, 93395.80596923828),
        array("upfront", 71646.10040664673, -3767.7622680664062, 38.61568347555947, 83903.02215576172)
    );

    results = runQuery(builder.build(), true);
    validate(columnNames, expectedResults, results);
  }

  @Test
  public void testPivotTable()
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
    String[] columnNames = array(
        "dayOfWeek",
        "spot-automotive", "spot-business", "spot-entertainment", "spot-health", "spot-mezzanine",
        "spot-news", "spot-premium", "spot-technology", "spot-travel",
        "total_market-mezzanine", "total_market-premium", "upfront-mezzanine", "upfront-premium"
    );

    List<Row> expectedResults = createExpectedRows(
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
    List<Row> results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
    columnNames = array(
        "dayOfWeek",
        "spot-automotive", "spot-business", "spot-entertainment", "spot-health", "spot-mezzanine",
        "spot-news", "spot-premium", "spot-technology", "spot-travel",
        "total_market-mezzanine", "total_market-premium", "upfront-mezzanine", "upfront-premium", "test"
    );

    expectedResults = createExpectedRows(
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
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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

    columnNames = array(
        "dayOfWeek",
        "spot-automotive-index", "spot-automotive-rows", "spot-business-index", "spot-business-rows",
        "spot-entertainment-index", "spot-entertainment-rows", "spot-health-index", "spot-health-rows",
        "spot-mezzanine-index", "spot-mezzanine-rows", "spot-news-index", "spot-news-rows",
        "spot-premium-index", "spot-premium-rows", "spot-technology-index", "spot-technology-rows",
        "spot-travel-index", "spot-travel-rows",
        "total_market-mezzanine-index", "total_market-mezzanine-rows", "total_market-premium-index", "total_market-premium-rows",
        "upfront-mezzanine-index", "upfront-mezzanine-rows", "upfront-premium-index", "upfront-premium-rows"
    );

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
    expectedResults = createExpectedRows(
        columnNames,
        array("Monday",
              null, null, null, null, null, null, null, null,
              null, null, null, null, null, null, null, null,
              null, null,
              15301.728393554688, 13, 15167.04833984375, 13, 15479.327270507812, 13, 12140.257507324219, 13),
        array("Tuesday",
              1664.368782043457, 13, 1404.3215408325195, 13, 1653.3230514526367, 13, 1522.367774963379, 13,
              1369.873420715332, 13, 1425.5140914916992, 13, 1560.511329650879, 13, 1068.2061462402344, 13,
              1530.9851303100586, 13,
              null, null, null, null, 15147.467102050781, 13, 11820.81298828125, 13),
        array("Wednesday",
              1801.9095306396484, 14, 1559.0761184692383, 14, 1783.8484954833984, 14, 1556.1792068481445, 14,
              1477.5527877807617, 14, 1566.9974746704102, 14, null, null, 1268.3166580200195, 14,
              1623.1850204467773, 14,
              15749.735595703125, 14, null, null, 14765.832275390625, 14, null, null)
    );
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index")
                             .withPartitionExpressions(PartitionExpression.of("_ = $sum0(_)"))
                )
            )
        )
    );

    columnNames = array(
        "dayOfWeek",
        "spot-automotive", "spot-business", "spot-entertainment", "spot-health", "spot-mezzanine",
        "spot-news", "spot-premium", "spot-technology", "spot-travel",
        "total_market-mezzanine", "total_market-premium", "upfront-mezzanine", "upfront-premium"
    );

    expectedResults = createExpectedRows(
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
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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

    expectedResults = createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null, null, null, null, null, null, null,
            16.813941650603674, 19.44656562237923, 15.17663768899694, 16.009429362202997
        ),
        array(
            "Tuesday",
            15.75634222579802, 15.882278300416639, 15.990390808152055, 17.304017518163935, 16.148675284780392,
            15.990723077592929, 20.09860503302568, 15.024990146457418, 15.946557910143886,
            null, null, 14.851266860403003, 15.588175986014235
        ),
        array(
            "Wednesday",
            17.058420904667074, 17.632486638624368, 17.25278950187719, 17.68833569624825, 17.418047408740776,
            17.57781478997598, null, 17.839670138962944, 16.906900932597882,
            17.306223748562424, null, 14.477094689189887, null
        )
    );
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
                                     array("^spot-.*", "#_ = $sum(_)"),
                                     array("^spot-.*", "_ = case(#_ == 0, 0.0, _ / #_ * 100)")
                                 )
                             )
                )
            )
        )
    );

    expectedResults = createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null, null, null, null, null, null, null,
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
            17.57781478997598, null, 17.839670138962944, 16.906900932597882,
            15749.735595703125, null, 14765.832275390625, null
        )
    );
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

    // empty pivot columns
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

    columnNames = array("dayOfWeek", "rows_part", "index_part", "rows", "index");

    expectedResults = createExpectedRows(
        columnNames,
        array("Monday", 13, 15301.728393554688, 13, 15301.728393554688),
        array("Monday", 26, 30468.776733398438, 26, 30468.776733398438),
        array("Monday", 39, 45948.10400390625, 39, 45948.10400390625),
        array("Monday", 52, 58088.36151123047, 52, 58088.36151123047),
        array("Tuesday", 13, 1664.368782043457, 65, 59752.730293273926),
        array("Tuesday", 26, 3068.6903228759766, 78, 61157.051834106445),
        array("Tuesday", 39, 4722.013374328613, 91, 62810.37488555908),
        array("Tuesday", 52, 6244.381149291992, 104, 64332.74266052246),
        array("Tuesday", 65, 7614.254570007324, 117, 65702.6160812378),
        array("Tuesday", 78, 9039.768661499023, 130, 67128.13017272949),
        array("Tuesday", 91, 10600.279991149902, 143, 68688.64150238037),
        array("Tuesday", 104, 11668.486137390137, 156, 69756.8476486206),
        array("Tuesday", 117, 13199.471267700195, 169, 71287.83277893066),
        array("Tuesday", 130, 28346.938369750977, 182, 86435.29988098145),
        array("Tuesday", 143, 40167.75135803223, 195, 98256.1128692627),
        array("Wednesday", 14, 1801.9095306396484, 209, 100058.02239990234),
        array("Wednesday", 28, 3360.9856491088867, 223, 101617.09851837158),
        array("Wednesday", 42, 5144.834144592285, 237, 103400.94701385498),
        array("Wednesday", 56, 6701.01335144043, 251, 104957.12622070312),
        array("Wednesday", 70, 8178.566139221191, 265, 106434.67900848389),
        array("Wednesday", 84, 9745.563613891602, 279, 108001.6764831543),
        array("Wednesday", 98, 11013.880271911621, 293, 109269.99314117432),
        array("Wednesday", 112, 12637.065292358398, 307, 110893.1781616211),
        array("Wednesday", 126, 28386.800888061523, 321, 126642.91375732422)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);

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
                                 "concat(_, '.percent') = round(100.0 * _ / #_, 3)"
                             )
                )
            )
        )
    );

    columnNames = array("dayOfWeek", "index.percent", "rows.percent", "index", "rows");

    expectedResults = createExpectedRows(
        columnNames,
        array("Monday", 26.342, 25.0, 15301.728393554688, 13),
        array("Monday", 26.11, 25.0, 15167.04833984375, 13),
        array("Monday", 26.648, 25.0, 15479.327270507812, 13),
        array("Monday", 20.9, 25.0, 12140.257507324219, 13),
        array("Tuesday", 4.144, 9.091, 1664.368782043457, 13),
        array("Tuesday", 3.496, 9.091, 1404.3215408325195, 13),
        array("Tuesday", 4.116, 9.091, 1653.3230514526367, 13),
        array("Tuesday", 3.79, 9.091, 1522.367774963379, 13),
        array("Tuesday", 3.41, 9.091, 1369.873420715332, 13),
        array("Tuesday", 3.549, 9.091, 1425.5140914916992, 13),
        array("Tuesday", 3.885, 9.091, 1560.511329650879, 13),
        array("Tuesday", 2.659, 9.091, 1068.2061462402344, 13),
        array("Tuesday", 3.811, 9.091, 1530.9851303100586, 13),
        array("Tuesday", 37.711, 9.091, 15147.467102050781, 13),
        array("Tuesday", 29.429, 9.091, 11820.81298828125, 13),
        array("Wednesday", 4.176, 10.0, 1801.9095306396484, 14),
        array("Wednesday", 3.613, 10.0, 1559.0761184692383, 14),
        array("Wednesday", 4.134, 10.0, 1783.8484954833984, 14),
        array("Wednesday", 3.606, 10.0, 1556.1792068481445, 14),
        array("Wednesday", 3.424, 10.0, 1477.5527877807617, 14),
        array("Wednesday", 3.631, 10.0, 1566.9974746704102, 14),
        array("Wednesday", 2.939, 10.0, 1268.3166580200195, 14),
        array("Wednesday", 3.761, 10.0, 1623.1850204467773, 14),
        array("Wednesday", 36.498, 10.0, 15749.735595703125, 14)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);
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

    String[] columnNames = array("dayOfWeek", "market", "sum_post", "sum_post.percent");

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

    List<Row> results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);
  }

  @Test
  public void test2281()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek", "dayofweek");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setVirtualColumns(new ExprVirtualColumn(
            "time_format(__time, out.format='yyyy-MM-dd HH:mm',out.timezone='Asia/Seoul', out.locale='en')",
            "MINUTE(request_time).inner")
        )
        .setDimensions(
            DefaultDimensionSpec.of("MINUTE(request_time).inner", "MINUTE(request_time)")
        )
        .setAggregatorSpecs(
            new GenericSumAggregatorFactory("aggregationfunc_000", null, "index + 1", null, ValueDesc.DOUBLE)
        )
        .setPostAggregatorSpecs(
            new MathPostAggregator("SUM(MEASURE_1)", "aggregationfunc_000")
        );

    builder.setLimitSpec(
        new LimitSpec(
            null, 24,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("MINUTE(request_time)"), null, null,
                    PivotSpec.tabular(PivotColumnSpec.toSpecs(), "SUM(MEASURE_1)")
                             .withPartitionExpressions(
                                 PartitionExpression.of("#_ = $sum(_)"),
                                 PartitionExpression.of("concat(_, '.percent') = _ / #_ * 100"))
                             .withAppendValueColumn(true)
                )
            )
        )
    );

    validate(
        builder.build(),
        array("MINUTE(request_time)", "SUM(MEASURE_1)", "SUM(MEASURE_1).percent"),
        array("2011-01-12 09:00", 4513.0, 0.8944754665688779),
        array("2011-01-13 09:00", 6090.949111938477, 1.2072245842562512),
        array("2011-01-14 09:00", 4935.488838195801, 0.9782126481920403),
        array("2011-01-15 09:00", 5739.140853881836, 1.1374962758653808),
        array("2011-01-16 09:00", 4711.468170166016, 0.9338118080508896),
        array("2011-01-17 09:00", 4664.030891418457, 0.9244097513169032),
        array("2011-01-18 09:00", 4411.145851135254, 0.8742879998443556),
        array("2011-01-19 09:00", 4609.068244934082, 0.9135161685874612),
        array("2011-01-20 10:00", 4447.630561828613, 0.8815192603407155),
        array("2011-01-22 09:00", 6175.801361083984, 1.2240422787264142),
        array("2011-01-23 09:00", 5603.292701721191, 1.110571206225139),
        array("2011-01-24 09:00", 5007.298484802246, 0.9924453021145856),
        array("2011-01-25 09:00", 5192.679672241211, 1.0291878069068658),
        array("2011-01-26 09:00", 6301.556800842285, 1.248966975303277),
        array("2011-01-27 09:00", 6038.663551330566, 1.1968615992751797),
        array("2011-01-28 09:00", 5785.855537414551, 1.1467551144092636),
        array("2011-01-29 09:00", 5359.517524719238, 1.0622550273670106),
        array("2011-01-30 09:00", 5510.331253051758, 1.092146270445943),
        array("2011-01-31 09:00", 5922.684387207031, 1.1738745580738352),
        array("2011-02-01 09:00", 5875.711364746094, 1.16456451681926),
        array("2011-02-02 09:00", 5971.373008728027, 1.1835246306312446),
        array("2011-02-03 09:00", 5237.882194519043, 1.038146935469807),
        array("2011-02-04 09:00", 5469.789611816406, 1.0841109273350502),
        array("2011-02-05 09:00", 5469.095397949219, 1.0839733343209241)
    );
  }

  @Test
  public void test2351()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek", "dayofweek");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setAggregatorSpecs(
            new DoubleSumAggregatorFactory("aggregationfunc_000", null, "index + 1", null),
            new DoubleSumAggregatorFactory("aggregationfunc_001", null, "index + 2", null),
            new DoubleSumAggregatorFactory("aggregationfunc_002", null, "index + 3", null),
            new DoubleSumAggregatorFactory("aggregationfunc_003", null, "index + 4", null)
        )
        .setPostAggregatorSpecs(
            new MathPostAggregator("SUM(MEASURE_1)", "aggregationfunc_000 - aggregationfunc_001"),
            new MathPostAggregator("SUM(MEASURE_2)", "aggregationfunc_001 - aggregationfunc_002"),
            new MathPostAggregator("SUM(MEASURE_3)", "aggregationfunc_002 - aggregationfunc_003")
        );

    builder.setLimitSpec(
        new LimitSpec(
            null, 24,
            Arrays.asList(
                new WindowingSpec(
                    null, null, null,
                    PivotSpec.tabular(PivotColumnSpec.toSpecs(), "SUM(MEASURE_1)", "SUM(MEASURE_2)", "SUM(MEASURE_3)")
                             .withPartitionExpressions(
                                 PartitionExpression.of("#_ = $sum(_)"),
                                 PartitionExpression.of("concat(_, '.percent') = _ / #_ * 100"))
                             .withAppendValueColumn(true)
                )
            )
        )
    );

    validate(
        builder.build(),
        array(
            "SUM(MEASURE_1)", "SUM(MEASURE_1).percent",
            "SUM(MEASURE_2)", "SUM(MEASURE_2).percent",
            "SUM(MEASURE_3)", "SUM(MEASURE_3).percent"
        ),
        array(-1208.9999160766602, 100.0, -1208.999984741211, 100.0, -1208.9999923706055, 100.0)
    );
  }

  @Test
  public void test2357()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setAggregatorSpecs(
            new DoubleSumAggregatorFactory("aggregationfunc_000", null, "isNull(partial_null_column)", null),
            new DoubleSumAggregatorFactory("aggregationfunc_001", null, "isNotNull(partial_null_column)", null)
        );

    String[] columnNames = array("aggregationfunc_000", "aggregationfunc_001");

    List<Row> expectedResults = createExpectedRows(columnNames, array(1023D, 186D));
    List<Row> results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);

    builder.aggregators(
        new LongSumAggregatorFactory("aggregationfunc_000", null, "isNull(partial_null_column)", null),
        new LongSumAggregatorFactory("aggregationfunc_001", null, "isNotNull(partial_null_column)", null)
    );
    expectedResults = createExpectedRows(columnNames, array(1023, 186));
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);

    builder.aggregators(
        new GenericSumAggregatorFactory("aggregationfunc_000", null, "if (index < 100, 0, 1) && if (index > 1000, 0, 1)", null, ValueDesc.LONG),
        new GenericSumAggregatorFactory("aggregationfunc_001", null, "isNotNull(partial_null_column)", null, ValueDesc.LONG)
    );
    expectedResults = createExpectedRows(columnNames, array(791, 186));
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);
  }

  @Test
  public void test2505()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setAggregatorSpecs(
            GenericSumAggregatorFactory.expr("aggregationfunc_000", "isNull(partial_null_column)", ValueDesc.DOUBLE),
            GenericSumAggregatorFactory.expr("aggregationfunc_001", "isNotNull(partial_null_column)", ValueDesc.DOUBLE)
        );

    validate(
        builder.build(),
        array("aggregationfunc_000", "aggregationfunc_001"),
        array(1023D, 186D)
    );
  }

  @Test
  public void test2541()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setAggregatorSpecs(
            GenericSumAggregatorFactory.expr("aggregationfunc_000", "isNull(partial_null_column)", ValueDesc.DOUBLE),
            GenericSumAggregatorFactory.expr("aggregationfunc_001", "isNotNull(partial_null_column)", ValueDesc.DOUBLE)
        )
        .setPostAggregatorSpecs(new MathPostAggregator(
            "D2.0누적실적",
            "case("
            + "isFinite(aggregationfunc_000/aggregationfunc_001),aggregationfunc_000/aggregationfunc_001,''"
            + ")"))
        .setLimitSpec(LimitSpec.of(
            new WindowingSpec(
                null, null, null,
                PivotSpec.tabular(Arrays.<PivotColumnSpec>asList(), "D2.0누적실적")
                         .withPartitionExpressions(
                             "#_ = $sum(_)",
                             "concat(_, '.percent') = case(#_ == 0, 0.0, cast(\"_\", 'DOUBLE') / #_ * 100)"
                         )
        )));

    String[] columnNames = array("D2.0누적실적", "D2.0누적실적.percent");

    List<Row> expectedResults = createExpectedRows(columnNames, array(5.5D, 100D));
    List<Row> results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);

    // explicit null
    builder.setPostAggregatorSpecs(new MathPostAggregator(
            "D2.0누적실적",
            "case("
            + "isFinite(aggregationfunc_000/aggregationfunc_001),aggregationfunc_000/aggregationfunc_001,NULL"
            + ")"));
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results, true);
  }

  @Test
  public void testPivotWithGroupingSet()
  {
    OrderByColumnSpec dayOfWeekAsc = OrderByColumnSpec.asc("dayOfWeek", "dayofweek");

    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setGroupingSets(new GroupingSetSpec.Indices.Builder().add(0, 2).add(0, 1, 2).build())
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

    String[] columnNames = array(
        "market", "quality", "dayOfWeek", "rows", "index", "addRowsIndexConstant"
    );
    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("spot", null, "Friday", 26, 2880.166572570801, 2907.166572570801),
        array("spot", null, "Saturday", 26, 2912.418525695801, 2939.418525695801),
        array("spot", null, "Sunday", 26, 2925.456039428711, 2952.456039428711),
        array("spot", null, "Thursday", 28, 3121.1823120117188, 3150.1823120117188),
        array("spot", null, "Tuesday", 26, 2930.384750366211, 2957.384750366211),
        array("spot", null, "Wednesday", 28, 3111.8562088012695, 3140.8562088012695),
        array("spot", "mezzanine", "Friday", 13, 1358.3970184326172, 1372.3970184326172),
        array("spot", "mezzanine", "Saturday", 13, 1398.4330215454102, 1412.4330215454102),
        array("spot", "mezzanine", "Sunday", 13, 1392.425064086914, 1406.425064086914),
        array("spot", "mezzanine", "Thursday", 14, 1486.202865600586, 1501.202865600586),
        array("spot", "mezzanine", "Tuesday", 13, 1369.873420715332, 1383.873420715332),
        array("spot", "mezzanine", "Wednesday", 14, 1477.5527877807617, 1492.5527877807617),
        array("spot", "premium", "Friday", 13, 1521.7695541381836, 1535.7695541381836),
        array("spot", "premium", "Saturday", 13, 1513.9855041503906, 1527.9855041503906),
        array("spot", "premium", "Sunday", 13, 1533.0309753417969, 1547.0309753417969),
        array("spot", "premium", "Thursday", 14, 1634.9794464111328, 1649.9794464111328),
        array("spot", "premium", "Tuesday", 13, 1560.511329650879, 1574.511329650879),
        array("total_market", null, "Friday", 26, 30173.691955566406, 30200.691955566406),
        array("total_market", null, "Monday", 26, 30468.776733398438, 30495.776733398438),
        array("total_market", null, "Saturday", 26, 30940.971740722656, 30967.971740722656),
        array("total_market", null, "Sunday", 26, 29305.0859375, 29332.0859375),
        array("total_market", null, "Thursday", 28, 32361.38690185547, 32390.38690185547),
        array("total_market", null, "Wednesday", 28, 32753.337280273438, 32782.33728027344),
        array("total_market", "mezzanine", "Friday", 13, 14696.267150878906, 14710.267150878906),
        array("total_market", "mezzanine", "Monday", 13, 15301.728393554688, 15315.728393554688),
        array("total_market", "mezzanine", "Saturday", 13, 14784.656677246094, 14798.656677246094),
        array("total_market", "mezzanine", "Sunday", 13, 14311.892395019531, 14325.892395019531),
        array("total_market", "mezzanine", "Thursday", 14, 16161.914001464844, 16176.914001464844),
        array("total_market", "mezzanine", "Wednesday", 14, 15749.735595703125, 15764.735595703125),
        array("total_market", "premium", "Friday", 13, 15477.4248046875, 15491.4248046875),
        array("total_market", "premium", "Monday", 13, 15167.04833984375, 15181.04833984375),
        array("total_market", "premium", "Saturday", 13, 16156.315063476562, 16170.315063476562),
        array("total_market", "premium", "Sunday", 13, 14993.193542480469, 15007.193542480469),
        array("total_market", "premium", "Thursday", 14, 16199.472900390625, 16214.472900390625),
        array("upfront", null, "Friday", 26, 27297.862365722656, 27324.862365722656),
        array("upfront", null, "Monday", 26, 27619.58477783203, 27646.58477783203),
        array("upfront", null, "Saturday", 26, 27820.831176757812, 27847.831176757812),
        array("upfront", null, "Sunday", 26, 24791.22381591797, 24818.22381591797),
        array("upfront", null, "Thursday", 28, 28562.748779296875, 28591.748779296875),
        array("upfront", null, "Tuesday", 26, 26968.28009033203, 26995.28009033203),
        array("upfront", null, "Wednesday", 28, 28985.57501220703, 29014.57501220703),
        array("upfront", "mezzanine", "Friday", 13, 14354.38134765625, 14368.38134765625),
        array("upfront", "mezzanine", "Monday", 13, 15479.327270507812, 15493.327270507812),
        array("upfront", "mezzanine", "Saturday", 13, 13736.503540039062, 13750.503540039062),
        array("upfront", "mezzanine", "Sunday", 13, 12682.70849609375, 12696.70849609375),
        array("upfront", "mezzanine", "Thursday", 14, 15828.224243164062, 15843.224243164062),
        array("upfront", "mezzanine", "Tuesday", 13, 15147.467102050781, 15161.467102050781),
        array("upfront", "mezzanine", "Wednesday", 14, 14765.832275390625, 14780.832275390625),
        array("upfront", "premium", "Friday", 13, 12943.481018066406, 12957.481018066406),
        array("upfront", "premium", "Monday", 13, 12140.257507324219, 12154.257507324219),
        array("upfront", "premium", "Saturday", 13, 14084.32763671875, 14098.32763671875),
        array("upfront", "premium", "Sunday", 13, 12108.515319824219, 12122.515319824219),
        array("upfront", "premium", "Thursday", 14, 12734.524536132812, 12749.524536132812),
        array("upfront", "premium", "Tuesday", 13, 11820.81298828125, 11834.81298828125)
    );
    List<Row> results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
    columnNames = array(
        "dayOfWeek",
        "spot-", "spot-mezzanine", "spot-premium",
        "total_market-", "total_market-mezzanine", "total_market-premium",
        "upfront-", "upfront-mezzanine", "upfront-premium"
    );

    expectedResults = createExpectedRows(
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
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

    // with nullValue 'X'
    builder.setLimitSpec(
        new LimitSpec(
            null, 3,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("dayOfWeek"),
                    Arrays.asList(dayOfWeekAsc),
                    Arrays.<String>asList(),
                    PivotSpec.tabular(PivotColumnSpec.toSpecs("market", "quality"), "index").withNullValue("X")
                )
            )
        )
    );
    columnNames = array(
        "dayOfWeek",
        "spot-X", "spot-mezzanine", "spot-premium",
        "total_market-X", "total_market-mezzanine", "total_market-premium",
        "upfront-X", "upfront-mezzanine", "upfront-premium"
    );
    expectedResults = createExpectedRows(
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

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
                    ).withPartitionExpressions(PartitionExpression.of("_ = $sum0(_)")
                    ).withAppendValueColumn(true)
                )
            )
        )
    );
    columnNames = array(
        "dayOfWeek",
        "spot--index", "spot-mezzanine-index", "spot-premium-index",
        "total_market--index", "total_market-mezzanine-index", "total_market-premium-index",
        "upfront--index", "upfront-mezzanine-index", "upfront-premium-index"
    );

    expectedResults = createExpectedRows(
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
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

    // with nullValue 'X'
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
                    ).withPartitionExpressions(PartitionExpression.of("_ = $sum0(_)")
                    ).withAppendValueColumn(true).withNullValue("X")
                )
            )
        )
    );
    columnNames = array(
        "dayOfWeek",
        "spot-X-index", "spot-mezzanine-index", "spot-premium-index",
        "total_market-X-index", "total_market-mezzanine-index", "total_market-premium-index",
        "upfront-X-index", "upfront-mezzanine-index", "upfront-premium-index"
    );
    expectedResults = createExpectedRows(
        columnNames,
        array("Monday",
              0.0, 0.0, 0.0,
              30468.776733398438, 15301.728393554688, 15167.04833984375,
              27619.58477783203, 15479.327270507812, 12140.257507324219),
        array("Tuesday",
              2930.384750366211, 1369.873420715332, 1560.511329650879,
              30468.776733398438, 15301.728393554688, 15167.04833984375,
              54587.86486816406, 30626.794372558594, 23961.07049560547),
        array("Wednesday",
              6042.2409591674805, 2847.4262084960938, 1560.511329650879,
              63222.114013671875, 31051.463989257812, 15167.04833984375,
              83573.4398803711, 45392.62664794922, 23961.07049560547
        )
    );
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
    columnNames = array(
        "dayOfWeek",
        "spot-", "spot-mezzanine", "spot-premium",
        "total_market-", "total_market-mezzanine", "total_market-premium",
        "upfront-", "upfront-mezzanine", "upfront-premium",
        "spot-mezzanine.percent", "spot-premium.percent",
        "total_market-mezzanine.percent", "total_market-premium.percent",
        "upfront-mezzanine.percent", "upfront-premium.percent"
    );
    expectedResults = createExpectedRows(
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
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
    columnNames = array(
        "dayOfWeek",
        "spot-", "spot-mezzanine", "spot-premium",
        "total_market-", "total_market-mezzanine", "total_market-premium",
        "upfront-", "upfront-mezzanine", "upfront-premium",
        "spot-mezzanine.percent", "spot-premium.percent",
        "total_market-mezzanine.percent", "total_market-premium.percent",
        "upfront-mezzanine.percent", "upfront-premium.percent"
    );

    expectedResults = createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null,
            list(30468.776733398438, 26), list(15301.728393554688, 13), list(15167.04833984375, 13),
            list(27619.58477783203, 26), list(15479.327270507812, 13), list(12140.257507324219, 13),
            null, null, list(50.221, 50.0),
            list(49.779, 50.0), list(56.045, 50.0), list(43.955, 50.0)
        ),
        array(
            "Tuesday",
            list(2930.384750366211, 26), list(1369.873420715332, 13), list(1560.511329650879, 13),
            null, null, null,
            list(26968.28009033203, 26), list(15147.467102050781, 13), list(11820.81298828125, 13),
            list(46.747, 50.0), list(53.253, 50.0), null,
            null, list(56.168, 50.0), list(43.832, 50.0)
        ),
        array(
            "Wednesday",
            list(3111.8562088012695, 28), list(1477.5527877807617, 14), null,
            list(32753.337280273438, 28), list(15749.735595703125, 14), null,
            list(28985.57501220703, 28), list(14765.832275390625, 14), null,
            list(47.481, 50.0), null, list(48.086, 50.0), null, list(50.942, 50.0), null
        )
    );
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

    // with nullValue 'X'
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
                             .withNullValue("X")
                )
            )
        )
    );
    columnNames = array(
        "dayOfWeek",
        "spot-X", "spot-mezzanine", "spot-premium",
        "total_market-X", "total_market-mezzanine", "total_market-premium",
        "upfront-X", "upfront-mezzanine", "upfront-premium",
        "spot-mezzanine.percent", "spot-premium.percent",
        "total_market-mezzanine.percent", "total_market-premium.percent",
        "upfront-mezzanine.percent", "upfront-premium.percent"
    );

    expectedResults = createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null, null,
            list(30468.776733398438, 26), list(15301.728393554688, 13), list(15167.04833984375, 13),
            list(27619.58477783203, 26), list(15479.327270507812, 13), list(12140.257507324219, 13),
            null, null, list(50.221, 50.0),
            list(49.779, 50.0), list(56.045, 50.0), list(43.955, 50.0)
        ),
        array(
            "Tuesday",
            list(2930.384750366211, 26), list(1369.873420715332, 13), list(1560.511329650879, 13),
            null, null, null,
            list(26968.28009033203, 26), list(15147.467102050781, 13), list(11820.81298828125, 13),
            list(46.747, 50.0), list(53.253, 50.0), null,
            null, list(56.168, 50.0), list(43.832, 50.0)
        ),
        array(
            "Wednesday",
            list(3111.8562088012695, 28), list(1477.5527877807617, 14), null,
            list(32753.337280273438, 28), list(15749.735595703125, 14), null,
            list(28985.57501220703, 28), list(14765.832275390625, 14), null,
            list(47.481, 50.0), null, list(48.086, 50.0), null, list(50.942, 50.0), null
        )
    );
    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

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
    columnNames = array(
        "dayOfWeek",
        "spot--index", "spot--rows",
        "spot-mezzanine-index", "spot-mezzanine-rows", "spot-premium-index", "spot-premium-rows",
        "total_market--index", "total_market--rows",
        "total_market-mezzanine-index", "total_market-mezzanine-rows", "total_market-premium-index", "total_market-premium-rows",
        "upfront--index", "upfront--rows",
        "upfront-mezzanine-index", "upfront-mezzanine-rows", "upfront-premium-index", "upfront-premium-rows",
        "spot-mezzanine-index.percent", "spot-mezzanine-rows.percent", "spot-premium-index.percent", "spot-premium-rows.percent",
        "total_market-mezzanine-index.percent", "total_market-mezzanine-rows.percent", "total_market-premium-index.percent", "total_market-premium-rows.percent",
        "upfront-mezzanine-index.percent", "upfront-mezzanine-rows.percent", "upfront-premium-index.percent", "upfront-premium-rows.percent"
    );
    expectedResults = createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null,
            null, null, null, null,
            30468.776733398438, 26,
            15301.728393554688, 13, 15167.04833984375, 13,
            27619.58477783203, 26,
            15479.327270507812, 13, 12140.257507324219, 13,
            null, null, null, null,
            50.221, 50.0, 49.779, 50.0,
            56.045, 50.0, 43.955, 50.0),
        array(
            "Tuesday",
            2930.384750366211, 26,
            1369.873420715332, 13, 1560.511329650879, 13,
            null, null,
            null, null, null, null,
            26968.28009033203, 26,
            15147.467102050781, 13, 11820.81298828125, 13,
            46.747, 50.0, 53.253, 50.0,
            null, null, null, null,
              56.168, 50.0, 43.832, 50.0),
        array(
            "Wednesday",
            3111.8562088012695, 28,
            1477.5527877807617, 14, null, null,
            32753.337280273438, 28,
            15749.735595703125, 14, null, null,
            28985.57501220703, 28,
            14765.832275390625, 14, null, null,
            47.481, 50.0, null, null,
            48.086, 50.0, null, null,
            50.942, 50.0, null, null)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);

    // with nullValue 'X'
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
                             .withNullValue("X")
                )
            )
        )
    );
    columnNames = array(
        "dayOfWeek",
        "spot-X-index", "spot-X-rows",
        "spot-mezzanine-index", "spot-mezzanine-rows", "spot-premium-index", "spot-premium-rows",
        "total_market-X-index", "total_market-X-rows",
        "total_market-mezzanine-index", "total_market-mezzanine-rows", "total_market-premium-index", "total_market-premium-rows",
        "upfront-X-index", "upfront-X-rows",
        "upfront-mezzanine-index", "upfront-mezzanine-rows", "upfront-premium-index", "upfront-premium-rows",
        "spot-mezzanine-index.percent", "spot-mezzanine-rows.percent", "spot-premium-index.percent", "spot-premium-rows.percent",
        "total_market-mezzanine-index.percent", "total_market-mezzanine-rows.percent", "total_market-premium-index.percent", "total_market-premium-rows.percent",
        "upfront-mezzanine-index.percent", "upfront-mezzanine-rows.percent", "upfront-premium-index.percent", "upfront-premium-rows.percent"
    );
    expectedResults = createExpectedRows(
        columnNames,
        array(
            "Monday",
            null, null,
            null, null, null, null,
            30468.776733398438, 26,
            15301.728393554688, 13, 15167.04833984375, 13,
            27619.58477783203, 26,
            15479.327270507812, 13, 12140.257507324219, 13,
            null, null, null, null,
            50.221, 50.0, 49.779, 50.0,
            56.045, 50.0, 43.955, 50.0),
        array(
            "Tuesday",
            2930.384750366211, 26,
            1369.873420715332, 13, 1560.511329650879, 13,
            null, null,
            null, null, null, null,
            26968.28009033203, 26,
            15147.467102050781, 13, 11820.81298828125, 13,
            46.747, 50.0, 53.253, 50.0,
            null, null, null, null,
              56.168, 50.0, 43.832, 50.0),
        array(
            "Wednesday",
            3111.8562088012695, 28,
            1477.5527877807617, 14, null, null,
            32753.337280273438, 28,
            15749.735595703125, 14, null, null,
            28985.57501220703, 28,
            14765.832275390625, 14, null, null,
            47.481, 50.0, null, null,
            48.086, 50.0, null, null,
            50.942, 50.0, null, null)
    );

    results = runQuery(builder.build());
    validate(columnNames, expectedResults, results);
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
        new SelectorDimFilter("quality", "automotiveAndBusinessAndNewsAndMezzanine", lookupExtractionFn),
        new SelectorDimFilter("quality", "entertainment", null),
        new SelectorDimFilter("quality", "health", null),
        new SelectorDimFilter("quality", "premium", null),
        new SelectorDimFilter("quality", "technology", null),
        new SelectorDimFilter("quality", "travel", null)
    );

    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .setDimFilter(DimFilters.or(dimFilters))
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "automotive", 1, 135),
        array("2011-04-01", "business", 1, 118),
        array("2011-04-01", "entertainment", 1, 158),
        array("2011-04-01", "health", 1, 120),
        array("2011-04-01", "mezzanine", 3, 2870),
        array("2011-04-01", "news", 1, 121),
        array("2011-04-01", "premium", 3, 2900),
        array("2011-04-01", "technology", 1, 78),
        array("2011-04-01", "travel", 1, 119),
        array("2011-04-02", "automotive", 1, 147),
        array("2011-04-02", "business", 1, 112),
        array("2011-04-02", "entertainment", 1, 166),
        array("2011-04-02", "health", 1, 113),
        array("2011-04-02", "mezzanine", 3, 2447),
        array("2011-04-02", "news", 1, 114),
        array("2011-04-02", "premium", 3, 2505),
        array("2011-04-02", "technology", 1, 97),
        array("2011-04-02", "travel", 1, 126)
    );
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
    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .setDimFilter(new SelectorDimFilter("quality", "", lookupExtractionFn))
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "mezzanine", 3, 2870),
        array("2011-04-01", "news", 1, 121),
        array("2011-04-02", "mezzanine", 3, 2447),
        array("2011-04-02", "news", 1, 114)
    );
  }

  @Test
  public void testGroupByWithExtractionDimFilterWhenSearchValueNotInTheMap()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .setDimFilter(new SelectorDimFilter("quality", "NOT_THERE", lookupExtractionFn))
        .build();

    validate(query, array("__time", "alias", "rows", "idx"));
  }

  @Test
  public void testGroupByWithExtractionDimFilterKeyIsNull()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("", "NULLorEMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .setDimFilter(
            new SelectorDimFilter(
                "null_column",
                "NULLorEMPTY",
                lookupExtractionFn
            )
        ).build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", null, 13, 6619),
        array("2011-04-02", null, 13, 5827)
    );
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
    DimFilter filter = new SelectorDimFilter("quality", "mezzanineANDnews", lookupExtractionFn);
    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
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
            new GenericSumAggregatorFactory("idx3", "index", null, "in(quality, 'automotive', 'business')", ValueDesc.LONG)
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx", "idx2", "idx3"),
        array("2011-04-01", "automotive", 0, 0, 0, 135),
        array("2011-04-01", "business", 0, 0, 0, 118),
        array("2011-04-01", "entertainment", 0, 0, 0, 0),
        array("2011-04-01", "health", 0, 0, 0, 0),
        array("2011-04-01", "mezzanine", 3, 2870, 0, 0),
        array("2011-04-01", "news", 1, 121, 0, 0),
        array("2011-04-01", "premium", 0, 0, 0, 0),
        array("2011-04-01", "technology", 0, 0, 0, 0),
        array("2011-04-01", "travel", 0, 0, 0, 0),

        array("2011-04-02", "automotive", 0, 0, 0, 147),
        array("2011-04-02", "business", 0, 0, 0, 112),
        array("2011-04-02", "entertainment", 0, 0, 0, 0),
        array("2011-04-02", "health", 0, 0, 0, 0),
        array("2011-04-02", "mezzanine", 3, 2447, 0, 0),
        array("2011-04-02", "news", 1, 114, 0, 0),
        array("2011-04-02", "premium", 0, 0, 0, 0),
        array("2011-04-02", "technology", 0, 0, 0, 0),
        array("2011-04-02", "travel", 0, 0, 0, 0)
    );

    query = query.withFilter(new RegexDimFilter("quality", "^[a-m].*$", null));

    validate(
        query,
        array("__time", "alias", "rows", "idx", "idx2", "idx3"),
        array("2011-04-01", "automotive", 0, 0, 0, 135),
        array("2011-04-01", "business", 0, 0, 0, 118),
        array("2011-04-01", "entertainment", 0, 0, 0, 0),
        array("2011-04-01", "health", 0, 0, 0, 0),
        array("2011-04-01", "mezzanine", 3, 2870, 0, 0, 0),

        array("2011-04-02", "automotive", 0, 0, 0, 147),
        array("2011-04-02", "business", 0, 0, 0, 112),
        array("2011-04-02", "entertainment", 0, 0, 0, 0),
        array("2011-04-02", "health", 0, 0, 0, 0),
        array("2011-04-02", "mezzanine", 3, 2447, 0, 0)
    );
  }

  @Test
  public void testGroupByWithExtractionDimFilterOptimizationManyToOne()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("mezzanine", "newsANDmezzanine");
    extractionMap.put("news", "newsANDmezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .setDimFilter(
            new SelectorDimFilter(
                "quality",
                "newsANDmezzanine",
                lookupExtractionFn
            )
        )
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", "mezzanine", 3, 2870),
        array("2011-04-01", "news", 1, 121),
        array("2011-04-02", "mezzanine", 3, 2447),
        array("2011-04-02", "news", 1, 114)
    );
  }

  @Test
  public void testGroupByWithExtractionDimFilterNullDims()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);

    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(Granularities.DAY)
        .setDimFilter(
            new SelectorDimFilter(
                "null_column",
                "EMPTY",
                lookupExtractionFn
            )
        ).build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", null, 13, 6619),
        array("2011-04-02", null, 13, 5827)
    );
  }

  @Test
  public void testBySegmentResultsWithAllFiltersWithExtractionFns()
  {
    List<Result> bySegmentResults;
    if (dataSource.equals(TestIndex.MMAPPED_SPLIT)) {
      bySegmentResults = Arrays.<Result>asList(
          new Result<BySegmentResultValue<Row>>(
              new DateTime("2011-01-12T00:00:00.000Z"),
              new BySegmentResultValueClass<Row>(
                  createExpectedRows(
                      array("__time", "alias", "rows", "idx"),
                      array("2011-01-01", "mezzanine", 57, 44838),
                      array("2011-02-01", "mezzanine", 84, 64211)
                  ),
                  getSegmentId(TestIndex.INTERVAL_TOP), new Interval("2011-01-12/2011-03-01")
              )
          ),
          new Result<BySegmentResultValue<Row>>(
              new DateTime("2011-03-01T00:00:00.000Z"),
              new BySegmentResultValueClass<Row>(
                  createExpectedRows(
                      array("__time", "alias", "rows", "idx"),
                      array("2011-03-01", "mezzanine", 93, 72926),
                      array("2011-04-01", "mezzanine", 45, 35611)
                  ),
                  getSegmentId(TestIndex.INTERVAL_BOTTOM), new Interval("2011-03-01/2011-05-01")
              )
          )
      );
    } else {
      bySegmentResults = Arrays.<Result>asList(
          new Result<BySegmentResultValue<Row>>(
              new DateTime("2011-01-12T00:00:00.000Z"),
              new BySegmentResultValueClass<Row>(
                  createExpectedRows(
                      array("__time", "alias", "rows", "idx"),
                      array("2011-01-01", "mezzanine", 57, 44838),
                      array("2011-02-01", "mezzanine", 84, 64211),
                      array("2011-03-01", "mezzanine", 93, 72926),
                      array("2011-04-01", "mezzanine", 45, 35611)
                  ),
                  getSegmentId(TestIndex.INTERVAL), new Interval("2011-01-12/2011-05-01")
              )
          )
      );
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
        .setDataSource(dataSource)
        .setInterval(TestIndex.INTERVAL)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(superFilter)
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));

    TestHelper.assertExpectedObjects(bySegmentResults, runSegmentQuery(builder.build()), "");
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

    GroupByQuery query = GroupByQuery.builder().setDataSource(dataSource)
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
        .setGranularity(Granularities.DAY)
        .setDimFilter(superFilter).build();

    validate(
        query,
        array("__time", "alias", "rows", "idx"),
        array("2011-04-01", null, 13, 6619),
        array("2011-04-02", null, 13, 5827)
    );
  }

  @Test
  public void testGroupByCardinalityAggWithExtractionFn()
  {
    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getDefault());

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
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
                    null,
                    false,
                    false,
                    0
                )
            )
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "rows", "numVals"),
        array("2011-04-01", "spot", 9, 1.0002442201269182D),
        array("2011-04-01", "total_market", 2, 1.0002442201269182D),
        array("2011-04-01", "upfront", 2, 1.0002442201269182D),
        array("2011-04-02", "spot", 9, 1.0002442201269182D),
        array("2011-04-02", "total_market", 2, 1.0002442201269182D),
        array("2011-04-02", "upfront", 2, 1.0002442201269182D)
    );
  }

  @Test
  public void testAverage()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            new AverageAggregatorFactory("idx1", "index", null),
            new AverageAggregatorFactory("idx2", "indexMaxPlusTen", null)
        )
        .setGranularity(Granularities.DAY)
        .build();

    validate(
        query,
        array("__time", "alias", "idx1", "idx2"),
        array("2011-04-01T00:00:00.000Z", "automotive", 135.88510131835938, 145.88510131835938),
        array("2011-04-01T00:00:00.000Z", "business", 118.57034301757812, 128.57034301757812),
        array("2011-04-01T00:00:00.000Z", "entertainment", 158.74722290039062, 168.74722290039062),
        array("2011-04-01T00:00:00.000Z", "health", 120.13470458984375, 130.13470458984375),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 957.2955754597982, 967.2955754597982),
        array("2011-04-01T00:00:00.000Z", "news", 121.58358001708984, 131.58358764648438),
        array("2011-04-01T00:00:00.000Z", "premium", 966.9328765869141, 976.9328765869141),
        array("2011-04-01T00:00:00.000Z", "technology", 78.62254333496094, 88.62254333496094),
        array("2011-04-01T00:00:00.000Z", "travel", 119.92274475097656, 129.92274475097656),
        array("2011-04-02T00:00:00.000Z", "automotive", 147.42593383789062, 157.42593383789062),
        array("2011-04-02T00:00:00.000Z", "business", 112.98703002929688, 122.98703002929688),
        array("2011-04-02T00:00:00.000Z", "entertainment", 166.01605224609375, 176.01605224609375),
        array("2011-04-02T00:00:00.000Z", "health", 113.44600677490234, 123.44600677490234),
        array("2011-04-02T00:00:00.000Z", "mezzanine", 816.2768707275391, 826.2768707275391),
        array("2011-04-02T00:00:00.000Z", "news", 114.2901382446289, 124.2901382446289),
        array("2011-04-02T00:00:00.000Z", "premium", 835.4716746012369, 845.4716746012369),
        array("2011-04-02T00:00:00.000Z", "technology", 97.38743591308594, 107.38743591308594),
        array("2011-04-02T00:00:00.000Z", "travel", 126.41136169433594, 136.41136169433594)
    );
  }

  @Test
  public void testAliasMapping()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setVirtualColumns(new ExprVirtualColumn("quality", "VC"))
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            new AverageAggregatorFactory("idx1", "index", null),
            new AverageAggregatorFactory("idx2", "indexMaxPlusTen", null)
        )
        .setDimFilter(SelectorDimFilter.of("alias", "automotive"))
        .setGranularity(Granularities.DAY)
        .build();

    String[] columnNames = {"__time", "alias", "idx1", "idx2"};
    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-04-01", "automotive", 135.88510131835938, 145.88510131835938),
        array("2011-04-02", "automotive", 147.42593383789062, 157.42593383789062)
    );
    List<Row> results = runQuery(query, true);
    TestHelper.validate(columnNames, expectedResults, results);

    results = runQuery(query.withFilter(SelectorDimFilter.of("VC", "automotive")), true);
    TestHelper.validate(columnNames, expectedResults, results);
  }

  @Test
  public void test3823()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .dataSource(dataSource)
        .granularity(Granularities.WEEK)
        .dimensions(DefaultDimensionSpec.toSpec("market"))
        .aggregators(QueryRunnerTestHelper.indexDoubleSum)
        .limitSpec(
            LimitSpec.of(
                20,
                new WindowingSpec(
                    Arrays.asList("market"),
                    OrderByColumnSpec.ascending("__time"),
                    "sum5 = $SUM(index,-10,0)"
                ).withIncrement(4)
            )
        )
        .outputColumns("__time", "market", "index", "sum5");

    validate(
        builder.build(),
        array("__time", "market", "index", "sum5"),
        array("2011-01-31", "spot", 6702.265960693359D, 23925.748176574707D),
        array("2011-02-28", "spot", 7058.767166137695D, 52978.629051208496D),
        array("2011-03-28", "spot", 7914.987197875977D, 78976.86014938354D),
        array("2011-01-31", "total_market", 16211.757202148438D, 56602.0625D),
        array("2011-02-28", "total_market", 16089.98095703125D, 123067.95086669922D),
        array("2011-03-28", "total_market", 17989.742065429688D, 178131.49340820312D),
        array("2011-01-31", "upfront", 15596.994750976562D, 52846.12664794922D),
        array("2011-02-28", "upfront", 14309.646179199219D, 112967.61041259766D),
        array("2011-03-28", "upfront", 15950.494201660156D, 158644.24676513672D)
    );
  }

  @Test
  public void test3849()
  {
    StreamQuery inner = Druids.newSelectQueryBuilder()
                              .dataSource(dataSource)
                              .intervals(firstToThird)
                              .columns("__time", "placementish", "index").streaming();
    GroupByQuery query = GroupByQuery
        .builder()
        .dataSource(inner)
        .dimensions(DefaultDimensionSpec.of("placementish", "alias"))
        .aggregators(rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .build();

    validate(
        query,
        array("alias", "rows", "idx"),
        array("a", 2, 282),
        array("preferred", 26, 12446),
        array("b", 2, 230),
        array("e", 2, 324),
        array("h", 2, 233),
        array("m", 6, 5317),
        array("n", 2, 235),
        array("p", 6, 5405),
        array("t", 4, 420)
    );
  }

  @Test(expected = QueryException.class)
  public void test3876()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .dataSource(dataSource)
        .intervals(firstToThird)
        .dimensions(
            DefaultDimensionSpec.of("placementish", "d0"),
            DefaultDimensionSpec.of("placementish", "d1")
        )
        .aggregators(rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addContext(Query.GBY_MAX_MULTIVALUE_DIMENSIONS, 1)
        .build();
    runQuery(query, false);
  }

  @Test
  public void test3877()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .dataSource(dataSource)
        .intervals(firstToThird)
        .dimensions(
            DefaultDimensionSpec.of("placementish", "d0"),
            DefaultDimensionSpec.of("placementish", "d1")
        )
        .aggregators(rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addContext(Query.GROUPED_DIMENSIONS, null)
        .build();

    String[] columns = array("d0", "d1", "rows", "idx");
    List<Row> expectedResults = createExpectedRows(
        columns,
        array("a", "a", 2, 282),
        array("a", "preferred", 2, 282),
        array("b", "b", 2, 230),
        array("b", "preferred", 2, 230),
        array("e", "e", 2, 324),
        array("e", "preferred", 2, 324),
        array("h", "h", 2, 233),
        array("h", "preferred", 2, 233),
        array("m", "m", 6, 5317),
        array("m", "preferred", 6, 5317),
        array("n", "n", 2, 235),
        array("n", "preferred", 2, 235),
        array("p", "p", 6, 5405),
        array("p", "preferred", 6, 5405),
        array("preferred", "a", 2, 282),
        array("preferred", "b", 2, 230),
        array("preferred", "e", 2, 324),
        array("preferred", "h", 2, 233),
        array("preferred", "m", 6, 5317),
        array("preferred", "n", 2, 235),
        array("preferred", "p", 6, 5405),
        array("preferred", "preferred", 26, 12446),
        array("preferred", "t", 4, 420),
        array("t", "preferred", 4, 420),
        array("t", "t", 4, 420)
    );
    List<Row> results = runQuery(query, true);
    TestHelper.validate(columns, expectedResults, results);

    expectedResults = createExpectedRows(
        columns,
        array("a", "a", 2, 282),
        array("b", "b", 2, 230),
        array("e", "e", 2, 324),
        array("h", "h", 2, 233),
        array("m", "m", 6, 5317),
        array("n", "n", 2, 235),
        array("p", "p", 6, 5405),
        array("preferred", "preferred", 26, 12446),
        array("t", "t", 4, 420)
    );
    query = query.withOverriddenContext(Query.GROUPED_DIMENSIONS, "placementish");
    results = runQuery(query, false);   // todo cardinality estimation (timeseries query)
    TestHelper.validate(columns, expectedResults, results);

    expectedResults = createExpectedRows(columns, array("preferred", "preferred", 26, 12446));
    query = query.withFilter(SelectorDimFilter.of("d0", "preferred"));
    results = runQuery(query, false);   // todo cardinality estimation (timeseries query)
    TestHelper.validate(columns, expectedResults, results);
  }
}
