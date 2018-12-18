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

package io.druid.query.join;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import com.metamx.common.ISE;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.Granularities;
import io.druid.math.expr.Parser;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinQuery;
import io.druid.query.JoinType;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.SelectToRow;
import io.druid.query.TableDataSource;
import io.druid.query.TopNToRow;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.select.SelectQuery;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class JoinQueryRunnerTest extends QueryRunnerTestHelper
{
  static final String JOIN_DS = "join_test";

  static {
    Parser.register(ModuleBuiltinFunctions.class);

    AggregatorFactory metric = new GenericSumAggregatorFactory("value", "value", "long");
    DimensionsSpec dimensions = new DimensionsSpec(
        StringDimensionSchema.ofNames("market", "market_month"), null, null
    );
    IncrementalIndexSchema schema = TestIndex.SAMPLE_SCHEMA
        .withMinTimestamp(new DateTime("2011-01-01T00:00:00.000Z").getMillis())
        .withDimensionsSpec(dimensions)
        .withMetrics(metric)
        .withRollup(false);

    DataSegment segment = new DataSegment(
        JOIN_DS,
        TestIndex.INTERVAL,
        "0",
        null,
        Arrays.asList("market", "market_month"),
        Arrays.asList("value"),
        null,
        null,
        0
    );
    StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec("ts", "iso", null),
            dimensions,
            "\t",
            "\u0001",
            Arrays.asList("ts", "market", "market_month", "value")
        )
        , "utf8"
    );
    CharSource source = TestIndex.asCharSource("druid.sample.join.tsv");
    TestIndex.segmentWalker.add(segment, TestIndex.makeRealtimeIndex(source, schema, parser));
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public JoinQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testJoin()
  {
    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSource(dataSource, ViewDataSource.of(dataSource, "__time", "market", "quality", "index"))
        .dataSource(JOIN_DS, ViewDataSource.of(JOIN_DS))
        .intervals(firstToThird)
        .element(JoinElement.inner(dataSource + ".market = " + JOIN_DS + ".market"))
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    String[] columns = new String[]{"__time", "market", "index", "market_month", "value"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01T00:00:00.000Z", "spot", 135.88510131835938, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 118.57034301757812, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 158.74722290039062, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 120.13470458984375, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 109.70581817626953, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 121.58358001708984, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 144.5073699951172, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 78.62254333496094, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 119.92274475097656, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 147.42593383789062, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 112.98703002929688, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 166.01605224609375, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 113.44600677490234, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 110.93193054199219, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 114.2901382446289, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 135.30149841308594, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 97.38743591308594, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 126.41136169433594, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1314.8397216796875, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1522.043701171875, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1193.5562744140625, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1321.375, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1447.3411865234375, "april_upfront", 41113L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1234.24755859375, "april_upfront", 41113L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1144.3424072265625, "april_upfront", 41113L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1049.738525390625, "april_upfront", 41113L)
    );

    Iterable<Row> rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow(Column.TIME_COLUMN_NAME));
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    // prefixed
    rows = Iterables.transform(runTabularQuery(joinQuery.withPrefixAlias(true)), Rows.mapToRow(dataSource + ".__time"));
    columns = new String[]{
        Column.TIME_COLUMN_NAME,
        dataSource + ".market", dataSource + ".__time", dataSource + ".index",
        JOIN_DS + ".market", JOIN_DS + ".__time", JOIN_DS + ".market_month", JOIN_DS + ".value"
    };
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 1301616000000L, 135.88510131835938, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 118.57034301757812, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 158.74722290039062, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 120.13470458984375, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 109.70581817626953, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 121.58358001708984, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 144.5073699951172, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 78.62254333496094, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 119.92274475097656, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 147.42593383789062, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 112.98703002929688, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 166.01605224609375, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 113.44600677490234, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 110.93193054199219, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 114.2901382446289, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 135.30149841308594, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 97.38743591308594, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 126.41136169433594, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "total_market", 1301616000000L, 1314.8397216796875, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("2011-04-01", "total_market", 1301616000000L, 1522.043701171875, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("2011-04-02", "total_market", 1301702400000L, 1193.5562744140625, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("2011-04-02", "total_market", 1301702400000L, 1321.375, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("2011-04-01", "upfront", 1301616000000L, 1447.3411865234375, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("2011-04-01", "upfront", 1301616000000L, 1234.24755859375, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("2011-04-02", "upfront", 1301702400000L, 1144.3424072265625, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("2011-04-02", "upfront", 1301702400000L, 1049.738525390625, "upfront", 1301616000000L, "april_upfront", 41113L)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }

  @Test
  public void testJoinOnGroupBy()
  {
    GroupByQuery groupByQuery = new GroupByQuery(
        TableDataSource.of(dataSource), firstToThird, BoundDimFilter.between("index", 120, 1200),
        Granularities.ALL, DefaultDimensionSpec.toSpec("market"), null, null,
        Arrays.<AggregatorFactory>asList(
            new CountAggregatorFactory("COUNT"),
            new GenericSumAggregatorFactory("SUM", "index", "double")
        ),
        null, null, null, null, null, null
    );

    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSource("X", new QueryDataSource(groupByQuery))
        .dataSource("Y", ViewDataSource.of(JOIN_DS))
        .intervals(firstToThird)
        .element(JoinElement.inner("X.market = Y.market"))
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    String[] columns = new String[]{"__time", "market", "COUNT", "SUM", "market_month", "value"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 9L, 1256.012825012207, "april_spot", 41111L),
        array("2011-04-01", "total_market", 1L, 1193.5562744140625, "april_total_market", 41112L),
        array("2011-04-01", "upfront", 2L, 2194.0809326171875, "april_upfront", 41113L)
    );
    Iterable<Row> rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow(Column.TIME_COLUMN_NAME));
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }

  @Test
  public void testJoin3way()
  {
    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSources(
            ImmutableMap.<String, DataSource>of(
                "X", ViewDataSource.of(dataSource, BoundDimFilter.between("index", 150, 1200), "__time", "market", "index"),
                "Y", ViewDataSource.of(dataSource, BoundDimFilter.between("index", 150, 1200), "market", "indexMin"),
                "Z", ViewDataSource.of(dataSource, BoundDimFilter.between("index", 150, 1200), "market", "indexMaxPlusTen")
            )
        )
        .intervals(firstToThird)
        .element(JoinElement.inner("X.market = Y.market"))
        .element(JoinElement.inner("Y.market = Z.market"))
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    String[] columns = new String[]{"__time", "market", "index", "indexMin", "indexMaxPlusTen"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 158.74722290039062, 158.74722, 168.74722290039062),
        array("2011-04-01", "spot", 158.74722290039062, 158.74722, 176.01605224609375),
        array("2011-04-01", "spot", 158.74722290039062, 166.01605, 168.74722290039062),
        array("2011-04-01", "spot", 158.74722290039062, 166.01605, 176.01605224609375),
        array("2011-04-02", "spot", 166.01605224609375, 158.74722, 168.74722290039062),
        array("2011-04-02", "spot", 166.01605224609375, 158.74722, 176.01605224609375),
        array("2011-04-02", "spot", 166.01605224609375, 166.01605, 168.74722290039062),
        array("2011-04-02", "spot", 166.01605224609375, 166.01605, 176.01605224609375),
        array("2011-04-02", "total_market", 1193.5562744140625, 1193.5563, 1203.5562744140625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1144.3424, 1154.3424072265625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1144.3424, 1059.738525390625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1049.7385, 1154.3424072265625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1049.7385, 1059.738525390625),
        array("2011-04-02", "upfront", 1049.738525390625, 1144.3424, 1154.3424072265625),
        array("2011-04-02", "upfront", 1049.738525390625, 1144.3424, 1059.738525390625),
        array("2011-04-02", "upfront", 1049.738525390625, 1049.7385, 1154.3424072265625),
        array("2011-04-02", "upfront", 1049.738525390625, 1049.7385, 1059.738525390625)
    );

    Iterable<Row> rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow(Column.TIME_COLUMN_NAME));
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }

  @Test
  public void testJoinOnJoin()
  {
    JoinQuery joinQuery1 = Druids
        .newJoinQueryBuilder()
        .dataSources(
            ImmutableMap.<String, DataSource>of(
                "X", ViewDataSource.of(dataSource, BoundDimFilter.between("index", 150, 1200), "__time", "market", "index"),
                "Y", ViewDataSource.of(dataSource, BoundDimFilter.between("index", 150, 1200), "market", "indexMin")
            )
        )
        .intervals(firstToThird)
        .element(JoinElement.inner("X.market = Y.market"))
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSources(
            ImmutableMap.<String, DataSource>of(
                "A", new QueryDataSource(joinQuery1),
                "B", ViewDataSource.of(dataSource, BoundDimFilter.between("index", 150, 1200), "market", "indexMaxPlusTen")
            )
        )
        .intervals(firstToThird)
        .element(JoinElement.inner("A.market = B.market"))
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    String[] columns = new String[]{"__time", "market", "index", "indexMin", "indexMaxPlusTen"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 158.74722290039062, 158.74722, 168.74722290039062),
        array("2011-04-01", "spot", 158.74722290039062, 158.74722, 176.01605224609375),
        array("2011-04-01", "spot", 158.74722290039062, 166.01605, 168.74722290039062),
        array("2011-04-01", "spot", 158.74722290039062, 166.01605, 176.01605224609375),
        array("2011-04-02", "spot", 166.01605224609375, 158.74722, 168.74722290039062),
        array("2011-04-02", "spot", 166.01605224609375, 158.74722, 176.01605224609375),
        array("2011-04-02", "spot", 166.01605224609375, 166.01605, 168.74722290039062),
        array("2011-04-02", "spot", 166.01605224609375, 166.01605, 176.01605224609375),
        array("2011-04-02", "total_market", 1193.5562744140625, 1193.5563, 1203.5562744140625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1144.3424, 1154.3424072265625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1144.3424, 1059.738525390625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1049.7385, 1154.3424072265625),
        array("2011-04-02", "upfront", 1144.3424072265625, 1049.7385, 1059.738525390625),
        array("2011-04-02", "upfront", 1049.738525390625, 1144.3424, 1154.3424072265625),
        array("2011-04-02", "upfront", 1049.738525390625, 1144.3424, 1059.738525390625),
        array("2011-04-02", "upfront", 1049.738525390625, 1049.7385, 1154.3424072265625),
        array("2011-04-02", "upfront", 1049.738525390625, 1049.7385, 1059.738525390625)
    );

    Iterable<Row> rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow(Column.TIME_COLUMN_NAME));
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }

  @Test
  public void testQueryOnJoin()
  {
    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSources(
            ImmutableMap.<String, DataSource>of(
                dataSource, ViewDataSource.of(dataSource, "__time", "market", "quality", "index"),
                JOIN_DS, ViewDataSource.of(JOIN_DS)
            )
        )
        .intervals(firstToThird)
        .element(JoinElement.inner(dataSource + ".market = " + JOIN_DS + ".market"))
        .prefixAlias(true)
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    // select on join
    SelectQuery selectQuery = new SelectQuery(
        new QueryDataSource(joinQuery), firstToThird, false, BoundDimFilter.between(dataSource + ".index", 120, 1200),
        Granularities.ALL, null, null, null, null, null, null, null,
        ImmutableMap.<String, Object>of(Query.POST_PROCESSING, new SelectToRow())
    );
    List<Row> rows = runQuery(selectQuery);

    String[] columns = new String[]{
        "__time",
        dataSource + ".market", dataSource + ".__time", dataSource + ".index",
        JOIN_DS + ".market", JOIN_DS + ".__time", JOIN_DS + ".market_month", JOIN_DS + ".value"
    };
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 1301616000000L, 135.88510131835938, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 158.74722290039062, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 120.13470458984375, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 121.58358001708984, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 144.5073699951172, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 147.42593383789062, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 166.01605224609375, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 135.30149841308594, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 126.41136169433594, "spot", 1301616000000L, "april_spot", 41111L),
        array("2011-04-01", "total_market", 1301702400000L, 1193.5562744140625, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("2011-04-01", "upfront", 1301702400000L, 1144.3424072265625, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("2011-04-01", "upfront", 1301702400000L, 1049.738525390625, "upfront", 1301616000000L, "april_upfront", 41113L)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    selectQuery = selectQuery.withDimensionSpecs(DefaultDimensionSpec.toSpec(dataSource + ".market", "not-existing"));
    rows = runQuery(selectQuery);
    columns = new String[]{"__time", dataSource + ".market", "not-existing"};
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "spot", null),
        array("2011-04-01T00:00:00.000Z", "total_market", null),
        array("2011-04-01T00:00:00.000Z", "upfront", null),
        array("2011-04-01T00:00:00.000Z", "upfront", null)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    // group-by on join
    GroupByQuery groupByQuery = new GroupByQuery(
        new QueryDataSource(joinQuery), firstToThird, BoundDimFilter.between(dataSource + ".index", 120, 1200),
        Granularities.ALL, DefaultDimensionSpec.toSpec(dataSource + ".market"), null, null,
        Arrays.<AggregatorFactory>asList(
            new CountAggregatorFactory("COUNT"),
            new GenericSumAggregatorFactory("SUM", dataSource + ".index", "double")
        ),
        null, null, null, null, null, null
    );
    rows = runQuery(groupByQuery);

    columns = new String[]{"__time", dataSource + ".market", "COUNT", "SUM"};
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01T00:00:00.000Z", "spot", 9L, 1256.012825012207),
        array("2011-04-01T00:00:00.000Z", "total_market", 1L, 1193.5562744140625),
        array("2011-04-01T00:00:00.000Z", "upfront", 2L, 2194.0809326171875)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    // top-n on join
    TopNQuery topNQuery = groupByQuery
        .asTopNQuery(DefaultDimensionSpec.of(dataSource + ".market"), new NumericTopNMetricSpec("SUM"))
        .withThreshold(10)
        .withOverriddenContext(ImmutableMap.<String, Object>of(Query.POST_PROCESSING, new TopNToRow()));
    rows = runQuery(topNQuery);

    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01T00:00:00.000Z", "upfront", 2L, 2194.0809326171875),
        array("2011-04-01T00:00:00.000Z", "spot", 9L, 1256.012825012207),
        array("2011-04-01T00:00:00.000Z", "total_market", 1L, 1193.5562744140625)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }

  @Test(expected = ISE.class)
  public void testJoinMaxGroup()
  {
    JoinQuery query = Druids
        .newJoinQueryBuilder()
        .dataSources(
            ImmutableMap.<String, DataSource>of(
                dataSource, ViewDataSource.of(dataSource, "__time", "market", "quality", "index"),
                JOIN_DS, ViewDataSource.of(JOIN_DS)
            )
        )
        .intervals(firstToThird)
        .element(JoinElement.inner(dataSource + ".market = " + JOIN_DS + ".market"))
        .maxRowsInGroup(10)
        .addContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1)
        .build();

    Sequences.toList(query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()));
  }
}
