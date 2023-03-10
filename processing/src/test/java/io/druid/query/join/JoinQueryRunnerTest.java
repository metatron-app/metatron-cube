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

package io.druid.query.join;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.CharSource;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Parser;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinQuery;
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
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
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
  static final TestQuerySegmentWalker SEGMENT_WALKER = TestIndex.segmentWalker.duplicate();

  static {
    Parser.register(ModuleBuiltinFunctions.class);

    AggregatorFactory metric = new GenericSumAggregatorFactory("value", "value", ValueDesc.LONG);
    DimensionsSpec dimensions = new DimensionsSpec(
        StringDimensionSchema.ofNames("market", "market_month"), null, null
    );
    IncrementalIndexSchema schema = TestIndex.SAMPLE_SCHEMA
        .withMinTimestamp(new DateTime("2011-01-01").getMillis())
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
    SEGMENT_WALKER.add(segment, TestIndex.makeRealtimeIndex(source, schema, parser));
    SEGMENT_WALKER.getQueryConfig().getJoin().setHashJoinThreshold(-1);
    SEGMENT_WALKER.getQueryConfig().getJoin().setSemiJoinThreshold(-1);
    SEGMENT_WALKER.getQueryConfig().getJoin().setBroadcastJoinThreshold(-1);
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

  @Override
  protected TestQuerySegmentWalker getSegmentWalker()
  {
    return SEGMENT_WALKER;
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
        .asMap(true)
        .build();

    String[] columns = new String[]{"__time", "market", "index", "market_month", "value"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 135.88510131835938D, "april_spot", 41111L),
        array("2011-04-01", "spot", 118.57034301757812D, "april_spot", 41111L),
        array("2011-04-01", "spot", 158.74722290039062D, "april_spot", 41111L),
        array("2011-04-01", "spot", 120.13470458984375D, "april_spot", 41111L),
        array("2011-04-01", "spot", 109.70581817626953D, "april_spot", 41111L),
        array("2011-04-01", "spot", 121.58358001708984D, "april_spot", 41111L),
        array("2011-04-01", "spot", 144.5073699951172D, "april_spot", 41111L),
        array("2011-04-01", "spot", 78.62254333496094D, "april_spot", 41111L),
        array("2011-04-01", "spot", 119.92274475097656D, "april_spot", 41111L),
        array("2011-04-02", "spot", 147.42593383789062D, "april_spot", 41111L),
        array("2011-04-02", "spot", 112.98703002929688D, "april_spot", 41111L),
        array("2011-04-02", "spot", 166.01605224609375D, "april_spot", 41111L),
        array("2011-04-02", "spot", 113.44600677490234D, "april_spot", 41111L),
        array("2011-04-02", "spot", 110.93193054199219D, "april_spot", 41111L),
        array("2011-04-02", "spot", 114.2901382446289D, "april_spot", 41111L),
        array("2011-04-02", "spot", 135.30149841308594D, "april_spot", 41111L),
        array("2011-04-02", "spot", 97.38743591308594D, "april_spot", 41111L),
        array("2011-04-02", "spot", 126.41136169433594D, "april_spot", 41111L),
        array("2011-04-01", "total_market", 1314.8397216796875D, "april_total_market", 41112L),
        array("2011-04-01", "total_market", 1522.043701171875D, "april_total_market", 41112L),
        array("2011-04-02", "total_market", 1193.5562744140625D, "april_total_market", 41112L),
        array("2011-04-02", "total_market", 1321.375D, "april_total_market", 41112L),
        array("2011-04-01", "upfront", 1447.3411865234375D, "april_upfront", 41113L),
        array("2011-04-01", "upfront", 1234.24755859375D, "april_upfront", 41113L),
        array("2011-04-02", "upfront", 1144.3424072265625D, "april_upfront", 41113L),
        array("2011-04-02", "upfront", 1049.738525390625D, "april_upfront", 41113L)
    );

    Iterable<Row> rows;
    rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow(Column.TIME_COLUMN_NAME));
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    columns = new String[]{"market", "index", "value"};
    // with outoutColumns
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("spot", 135.88510131835938D, 41111L),
        array("spot", 118.57034301757812D, 41111L),
        array("spot", 158.74722290039062D, 41111L),
        array("spot", 120.13470458984375D, 41111L),
        array("spot", 109.70581817626953D, 41111L),
        array("spot", 121.58358001708984D, 41111L),
        array("spot", 144.5073699951172D, 41111L),
        array("spot", 78.62254333496094D, 41111L),
        array("spot", 119.92274475097656D, 41111L),
        array("spot", 147.42593383789062D, 41111L),
        array("spot", 112.98703002929688D, 41111L),
        array("spot", 166.01605224609375D, 41111L),
        array("spot", 113.44600677490234D, 41111L),
        array("spot", 110.93193054199219D, 41111L),
        array("spot", 114.2901382446289D, 41111L),
        array("spot", 135.30149841308594D, 41111L),
        array("spot", 97.38743591308594D, 41111L),
        array("spot", 126.41136169433594D, 41111L),
        array("total_market", 1314.8397216796875D, 41112L),
        array("total_market", 1522.043701171875D, 41112L),
        array("total_market", 1193.5562744140625D, 41112L),
        array("total_market", 1321.375D, 41112L),
        array("upfront", 1447.3411865234375D, 41113L),
        array("upfront", 1234.24755859375D, 41113L),
        array("upfront", 1144.3424072265625D, 41113L),
        array("upfront", 1049.738525390625D, 41113L)
    );
    rows = Iterables.transform(
        runTabularQuery(joinQuery.withOutputColumns(Arrays.asList("market", "index", "value"))),
        Rows.mapToRow(Column.TIME_COLUMN_NAME)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    // prefixed
    rows = Iterables.transform(runTabularQuery(joinQuery.withPrefixAlias(true)), Rows.mapToRow(dataSource + ".__time"));
    columns = new String[]{
        Column.TIME_COLUMN_NAME,
        dataSource + ".market", dataSource + ".__time", dataSource + ".index",
        JOIN_DS + ".market", JOIN_DS + ".market_month", JOIN_DS + ".value"
    };
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 1301616000000L, 135.88510131835938D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 118.57034301757812D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 158.74722290039062D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 120.13470458984375D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 109.70581817626953D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 121.58358001708984D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 144.5073699951172D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 78.62254333496094D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 119.92274475097656D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 147.42593383789062D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 112.98703002929688D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 166.01605224609375D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 113.44600677490234D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 110.93193054199219D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 114.2901382446289D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 135.30149841308594D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 97.38743591308594D, "spot", "april_spot", 41111L),
        array("2011-04-02", "spot", 1301702400000L, 126.41136169433594D, "spot", "april_spot", 41111L),
        array("2011-04-01", "total_market", 1301616000000L, 1314.8397216796875D, "total_market", "april_total_market", 41112L),
        array("2011-04-01", "total_market", 1301616000000L, 1522.043701171875D, "total_market", "april_total_market", 41112L),
        array("2011-04-02", "total_market", 1301702400000L, 1193.5562744140625D, "total_market", "april_total_market", 41112L),
        array("2011-04-02", "total_market", 1301702400000L, 1321.375D, "total_market", "april_total_market", 41112L),
        array("2011-04-01", "upfront", 1301616000000L, 1447.3411865234375D, "upfront", "april_upfront", 41113L),
        array("2011-04-01", "upfront", 1301616000000L, 1234.24755859375D, "upfront", "april_upfront", 41113L),
        array("2011-04-02", "upfront", 1301702400000L, 1144.3424072265625D, "upfront", "april_upfront", 41113L),
        array("2011-04-02", "upfront", 1301702400000L, 1049.738525390625D, "upfront", "april_upfront", 41113L)
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
            new GenericSumAggregatorFactory("SUM", "index", ValueDesc.DOUBLE)
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
        .asMap(true)
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
        .asMap(true)
        .build();

    String[] columns = new String[]{"__time", "market", "index", "indexMin", "indexMaxPlusTen"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 158.74722290039062D, 158.74722F, 168.74722290039062D),
        array("2011-04-01", "spot", 158.74722290039062D, 158.74722F, 176.01605224609375D),
        array("2011-04-01", "spot", 158.74722290039062D, 166.01605F, 168.74722290039062D),
        array("2011-04-01", "spot", 158.74722290039062D, 166.01605F, 176.01605224609375D),
        array("2011-04-02", "spot", 166.01605224609375D, 158.74722F, 168.74722290039062D),
        array("2011-04-02", "spot", 166.01605224609375D, 158.74722F, 176.01605224609375D),
        array("2011-04-02", "spot", 166.01605224609375D, 166.01605F, 168.74722290039062D),
        array("2011-04-02", "spot", 166.01605224609375D, 166.01605F, 176.01605224609375D),
        array("2011-04-02", "total_market", 1193.5562744140625D, 1193.5563F, 1203.5562744140625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1144.3424F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1144.3424F, 1059.738525390625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1049.7385F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1049.7385F, 1059.738525390625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1144.3424F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1144.3424F, 1059.738525390625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1049.7385F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1049.7385F, 1059.738525390625D)
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
        .asMap(true)
        .build();

    String[] columns = new String[]{"__time", "market", "index", "indexMin", "indexMaxPlusTen"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 158.74722290039062D, 158.74722F, 168.74722290039062D),
        array("2011-04-01", "spot", 158.74722290039062D, 158.74722F, 176.01605224609375D),
        array("2011-04-01", "spot", 158.74722290039062D, 166.01605F, 168.74722290039062D),
        array("2011-04-01", "spot", 158.74722290039062D, 166.01605F, 176.01605224609375D),
        array("2011-04-02", "spot", 166.01605224609375D, 158.74722F, 168.74722290039062D),
        array("2011-04-02", "spot", 166.01605224609375D, 158.74722F, 176.01605224609375D),
        array("2011-04-02", "spot", 166.01605224609375D, 166.01605F, 168.74722290039062D),
        array("2011-04-02", "spot", 166.01605224609375D, 166.01605F, 176.01605224609375D),
        array("2011-04-02", "total_market", 1193.5562744140625D, 1193.5563F, 1203.5562744140625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1144.3424F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1144.3424F, 1059.738525390625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1049.7385F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1144.3424072265625D, 1049.7385F, 1059.738525390625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1144.3424F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1144.3424F, 1059.738525390625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1049.7385F, 1154.3424072265625D),
        array("2011-04-02", "upfront", 1049.738525390625D, 1049.7385F, 1059.738525390625D)
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
        .asMap(true)
        .build();

    // select on join
    SelectQuery selectQuery = new SelectQuery(
        QueryDataSource.of(joinQuery), firstToThird, false, BoundDimFilter.between(dataSource + ".index", 120, 1200),
        Granularities.ALL, null, null, null, null, null, null, null,
        ImmutableMap.<String, Object>of(Query.POST_PROCESSING, new SelectToRow())
    );
    List<Row> rows = runQuery(selectQuery);

    String[] columns = new String[]{
        "__time",
        dataSource + ".market", dataSource + ".__time", dataSource + ".index",
        JOIN_DS + ".market", JOIN_DS + ".market_month", JOIN_DS + ".value"
    };
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 1301616000000L, 135.88510131835938D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 158.74722290039062D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 120.13470458984375D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 121.58358001708984D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301616000000L, 144.5073699951172D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 147.42593383789062D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 166.01605224609375D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 135.30149841308594D, "spot", "april_spot", 41111L),
        array("2011-04-01", "spot", 1301702400000L, 126.41136169433594D, "spot", "april_spot", 41111L),
        array("2011-04-01", "total_market", 1301702400000L, 1193.5562744140625D, "total_market", "april_total_market", 41112L),
        array("2011-04-01", "upfront", 1301702400000L, 1144.3424072265625D, "upfront", "april_upfront", 41113L),
        array("2011-04-01", "upfront", 1301702400000L, 1049.738525390625D, "upfront", "april_upfront", 41113L)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    selectQuery = selectQuery.withDimensionSpecs(DefaultDimensionSpec.toSpec(dataSource + ".market", "not-existing"));
    rows = runQuery(selectQuery);
    columns = new String[]{"__time", dataSource + ".market", "not-existing"};
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "spot", null),
        array("2011-04-01", "total_market", null),
        array("2011-04-01", "upfront", null),
        array("2011-04-01", "upfront", null)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    // group-by on join
    GroupByQuery groupByQuery = new GroupByQuery(
        QueryDataSource.of(joinQuery), firstToThird, BoundDimFilter.between(dataSource + ".index", 120, 1200),
        Granularities.ALL, DefaultDimensionSpec.toSpec(dataSource + ".market"), null, null,
        Arrays.<AggregatorFactory>asList(
            new CountAggregatorFactory("COUNT"),
            new GenericSumAggregatorFactory("SUM", dataSource + ".index", ValueDesc.DOUBLE)
        ),
        null, null, null, null, null, null
    );
    rows = runQuery(groupByQuery);

    columns = new String[]{"__time", dataSource + ".market", "COUNT", "SUM"};
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "spot", 9L, 1256.012825012207),
        array("2011-04-01", "upfront", 2L, 2194.0809326171875),
        array("2011-04-01", "total_market", 1L, 1193.5562744140625)
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
        array("2011-04-01", "upfront", 2L, 2194.0809326171875),
        array("2011-04-01", "spot", 9L, 1256.012825012207),
        array("2011-04-01", "total_market", 1L, 1193.5562744140625)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }

  @Test(expected = ISE.class)
  public void testJoinMaxGroup()
  {
    JoinQuery query = Druids
        .newJoinQueryBuilder()
        .dataSource(dataSource, ViewDataSource.of(dataSource, "__time", "market", "quality", "index"))
        .dataSource(JOIN_DS, ViewDataSource.of(JOIN_DS))
        .intervals(firstToThird)
        .element(JoinElement.inner(dataSource + ".market = " + JOIN_DS + ".market"))
        .maxOutputRow(10)
        .asMap(true)
        .build();

    runQuery(query);
  }
}
