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

package io.druid.sql.calcite;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.guava.Files;
import io.druid.common.utils.JodaUtils;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.PeriodGranularity;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinType;
import io.druid.query.Query;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.rel.TypedDummyQuery;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.apache.calcite.plan.RelOptPlanner;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.druid.sql.calcite.util.CalciteTests.FORBIDDEN_DATASOURCE;

public class CalciteQueryTest extends CalciteQueryTestHelper
{
  private static final Logger log = new Logger(CalciteQueryTest.class);

  private static final String LOS_ANGELES = "America/Los_Angeles";
  private static final DateTimeZone LOS_ANGELES_DTZ = DateTimeZone.forID(LOS_ANGELES);

  private static final Map<String, Object> QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      "skipEmptyBuckets", false,
      GroupByQuery.SORT_ON_TIME, false
  );

  private static final Map<String, Object> QUERY_CONTEXT_NO_TOPN = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, "false",
      GroupByQuery.SORT_ON_TIME, false
  );

  private static final Map<String, Object> QUERY_CONTEXT_LOS_ANGELES = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES,
      GroupByQuery.SORT_ON_TIME, false
  );

  // Matches QUERY_CONTEXT_DEFAULT
  public static final Map<String, Object> TIMESERIES_CONTEXT_DEFAULT = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      "skipEmptyBuckets", true,
      GroupByQuery.SORT_ON_TIME, false
  );

  private static MiscQueryHook hook;
  private static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    hook = new MiscQueryHook();
    walker = CalciteTests.createMockWalker(Files.createTempDir()).withQueryHook(hook);
    walker.getQueryConfig().getJoin().setSemiJoinThreshold(-1);
    walker.getQueryConfig().getJoin().setBroadcastJoinThreshold(-1);
    walker.populate("sales");
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Before
  public void before()
  {
    hook.clear();
  }

  @Override
  protected <T extends Throwable> Pair<String, List<Object[]>> failed(T ex) throws T
  {
    hook.printHooked();
    throw ex;
  }

  @Test
  public void testShowTables() throws Exception
  {
    testQuery(
        "SHOW TABLES",
        new Object[]{"foo"},
        new Object[]{"foo2"},
        new Object[]{"foo3"},
        new Object[]{"forbiddenDatasource"},
        new Object[]{"mmapped"},
        new Object[]{"mmapped-split"},
        new Object[]{"mmapped_merged"},
        new Object[]{"mmapped_norollup"},
        new Object[]{"realtime"},
        new Object[]{"realtime_norollup"},
        new Object[]{"sales"},
        new Object[]{"aview"},
        new Object[]{"bview"}
    );
    testQuery(
        "SHOW TABLES LIKE 'foo%'",
        new Object[]{"foo"},
        new Object[]{"foo2"},
        new Object[]{"foo3"}
    );
  }

  @Test
  public void testDescTable() throws Exception
  {
    testQuery(
        "DESC foo",
        new Object[]{"__time", "TIMESTAMP", "TIMESTAMP(3) NOT NULL", "NO", ""},
        new Object[]{"cnt", "BIGINT", "BIGINT", "YES", ""},
        new Object[]{"dim1", "VARCHAR", "dimension.string", "YES", ""},
        new Object[]{"dim2", "VARCHAR", "dimension.string", "YES", ""},
        new Object[]{"m1", "DOUBLE", "DOUBLE", "YES", ""},
        new Object[]{"m2", "DOUBLE", "DOUBLE", "YES", ""},
        new Object[]{"unique_dim1", "OTHER", "hyperUnique", "YES", ""}
    );
    testQuery(
        "DESCRIBE foo2 '%1'",
        new Object[]{"dim1", "VARCHAR", "dimension.string", "YES", ""},
        new Object[]{"m1", "DOUBLE", "DOUBLE", "YES", ""},
        new Object[]{"unique_dim1", "OTHER", "hyperUnique", "YES", ""}
    );
  }

  @Test
  public void testSelectConstantExpression() throws Exception
  {
    testQuery(
        "SELECT 1 + 1",
        TypedDummyQuery.DUMMY,
        new Object[]{2}
    );
  }

  @Test
  public void testInsertInto() throws Exception
  {
    testQuery(
        "INSERT INTO DIRECTORY '/__temporary' AS 'CSV' SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        new Object[]{true, 1, MASKED, 7L}
    );
  }

  @Test
  public void testInsertIntoWithHeader() throws Exception
  {
    testQuery(
        "INSERT INTO DIRECTORY '/__temporary' AS 'CSV' WITH ('withHeader' : true) SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        new Object[]{true, 1, MASKED, 19L}
    );
  }

  @Test
  public void testSelectConstantExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns(Arrays.asList("v0", "dim1"))
                 .virtualColumns(EXPR_VC("v0", "2"))
                 .limit(1)
                 .streaming(),
        new Object[]{2, ""}
    );
  }

  @Test
  public void testSelectTrimFamily() throws Exception
  {
    // TRIM has some whacky parsing. Make sure the different forms work.

    testQuery(
        "SELECT\n"
        + "TRIM(BOTH 'x' FROM 'xfoox'),\n"
        + "TRIM(TRAILING 'x' FROM 'xfoox'),\n"
        + "TRIM(' ' FROM ' foo '),\n"
        + "TRIM(TRAILING FROM ' foo '),\n"
        + "TRIM(' foo '),\n"
        + "BTRIM(' foo '),\n"
        + "BTRIM('xfoox', 'x'),\n"
        + "LTRIM(' foo '),\n"
        + "LTRIM('xfoox', 'x'),\n"
        + "RTRIM(' foo '),\n"
        + "RTRIM('xfoox', 'x'),\n"
        + "COUNT(*)\n"
        + "FROM foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0"))
              .postAggregators(
                  ImmutableList.<PostAggregator>builder()
                      .add(EXPR_POST_AGG("p0", "'foo'"))
                      .add(EXPR_POST_AGG("p1", "'xfoo'"))
                      .add(EXPR_POST_AGG("p2", "'foo'"))
                      .add(EXPR_POST_AGG("p3", "' foo'"))
                      .add(EXPR_POST_AGG("p4", "'foo'"))
                      .add(EXPR_POST_AGG("p5", "'foo'"))
                      .add(EXPR_POST_AGG("p6", "'foo'"))
                      .add(EXPR_POST_AGG("p7", "'foo '"))
                      .add(EXPR_POST_AGG("p8", "'foox'"))
                      .add(EXPR_POST_AGG("p9", "' foo'"))
                      .add(EXPR_POST_AGG("p10", "'xfoo'"))
                      .build()
              )
              .outputColumns("p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "a0")
              .build(),
        new Object[]{"foo", "xfoo", "foo", " foo", "foo", "foo", "foo", "foo ", "foox", " foo", "xfoo", 6L}
    );
  }

  @Test
  public void testExplainSelectConstantExpression() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR SELECT 1 + 1",
        new Object[]{"DruidValuesRel\n"}
    );
  }

  @Test
  public void testInformationSchemaSchemata() throws Exception
  {
    testQuery(
        "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        new Object[]{"druid"},
        new Object[]{"INFORMATION_SCHEMA"},
        new Object[]{"sys"}
    );
  }

  @Test
  public void testInformationSchemaTables() throws Exception
  {
    testQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        new Object[]{"druid", "foo", "TABLE"},
        new Object[]{"druid", "foo2", "TABLE"},
        new Object[]{"druid", "foo3", "TABLE"},
        new Object[]{"druid", "forbiddenDatasource", "TABLE"},
        new Object[]{"druid", "mmapped", "TABLE"},
        new Object[]{"druid", "mmapped-split", "TABLE"},
        new Object[]{"druid", "mmapped_merged", "TABLE"},
        new Object[]{"druid", "mmapped_norollup", "TABLE"},
        new Object[]{"druid", "realtime", "TABLE"},
        new Object[]{"druid", "realtime_norollup", "TABLE"},
        new Object[]{"druid", "sales", "TABLE"},
        new Object[]{"druid", "aview", "VIEW"},
        new Object[]{"druid", "bview", "VIEW"},
        new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE"},
        new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE"},
        new Object[]{"INFORMATION_SCHEMA", "SERVERS", "SYSTEM_TABLE"},
        new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE"},
        new Object[]{"sys", "functions", "SYSTEM_TABLE"},
        new Object[]{"sys", "locks", "SYSTEM_TABLE"},
        new Object[]{"sys", "segments", "SYSTEM_TABLE"},
        new Object[]{"sys", "server_segments", "SYSTEM_TABLE"},
        new Object[]{"sys", "servers", "SYSTEM_TABLE"},
        new Object[]{"sys", "servers_extended", "SYSTEM_TABLE"},
        new Object[]{"sys", "tasks", "SYSTEM_TABLE"}
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        null,
        ImmutableList.of(
            new Object[]{"druid", CalciteTests.DATASOURCE1, "TABLE"},
            new Object[]{"druid", CalciteTests.DATASOURCE2, "TABLE"},
            new Object[]{"druid", CalciteTests.DATASOURCE3, "TABLE"},
            new Object[]{"druid", FORBIDDEN_DATASOURCE, "TABLE"},
            new Object[]{"druid", "mmapped", "TABLE"},
            new Object[]{"druid", "mmapped-split", "TABLE"},
            new Object[]{"druid", "mmapped_merged", "TABLE"},
            new Object[]{"druid", "mmapped_norollup", "TABLE"},
            new Object[]{"druid", "realtime", "TABLE"},
            new Object[]{"druid", "realtime_norollup", "TABLE"},
            new Object[]{"druid", "sales", "TABLE"},
            new Object[]{"druid", "aview", "VIEW"},
            new Object[]{"druid", "bview", "VIEW"},
            new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "SERVERS", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE"},
            new Object[]{"sys", "functions", "SYSTEM_TABLE"},
            new Object[]{"sys", "locks", "SYSTEM_TABLE"},
            new Object[]{"sys", "segments", "SYSTEM_TABLE"},
            new Object[]{"sys", "server_segments", "SYSTEM_TABLE"},
            new Object[]{"sys", "servers", "SYSTEM_TABLE"},
            new Object[]{"sys", "servers_extended", "SYSTEM_TABLE"},
            new Object[]{"sys", "tasks", "SYSTEM_TABLE"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumnsOnTable() throws Exception
  {
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        new Object[]{"__time", "TIMESTAMP", "NO"},
        new Object[]{"cnt", "BIGINT", "YES"},
        new Object[]{"dim1", "VARCHAR", "YES"},
        new Object[]{"dim2", "VARCHAR", "YES"},
        new Object[]{"m1", "DOUBLE", "YES"},
        new Object[]{"m2", "DOUBLE", "YES"},
        new Object[]{"unique_dim1", "OTHER", "YES"}
    );
  }

  @Test
  public void testInformationSchemaColumnsOnForbiddenTable() throws Exception
  {
//    testQuery(
//        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
//        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
//        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'forbiddenDatasource'",
//        ImmutableList.of(),
//        ImmutableList.of()
//    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'forbiddenDatasource'",
        null,
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", "YES"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"m1", "DOUBLE", "YES"},
            new Object[]{"m2", "DOUBLE", "YES"},
            new Object[]{"unique_dim1", "OTHER", "YES"}
        )
    );
  }


  @Test
  public void testInformationSchemaColumnsOnView() throws Exception
  {
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'aview'",
        new Object[]{"dim1_firstchar", "VARCHAR", "YES"}
    );
  }

  @Test
  public void testExplainInformationSchemaColumns() throws Exception
  {
    final Object[] explanation = {
        "DruidOuterQueryRel(scanProject=[$3, $7])\n"
        + "  DruidValuesRel(table=[INFORMATION_SCHEMA.COLUMNS])\n"
    };
    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        explanation
    );
    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME LIKE 'foo%'",
        explanation
    );
  }

  @Test
  public void testExplainInformationSchemaColumnsOrderBy() throws Exception
  {
    final String explanation =
        "DruidOuterQueryRel(scanProject=[$3, $7], sort=[$0:ASC])\n"
        + "  DruidValuesRel(table=[INFORMATION_SCHEMA.COLUMNS])\n";

    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "ORDER BY COLUMN_NAME",
        new Object[]{explanation}
    );
  }

  @Test
  public void testSelectStar() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                 .streaming(),
        new Object[]{T("2000-01-01"), 1L, "", "a", 1d, 1.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2000-01-02"), 1L, "10.1", "", 2d, 2.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2000-01-03"), 1L, "2", "", 3d, 3.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-01"), 1L, "1", "a", 4d, 4.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-02"), 1L, "def", "abc", 5d, 5.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-03"), 1L, "abc", "", 6d, 6.0, HyperLogLogCollector.class.getName()}
    );
  }

  @Test
  public void testSelectStarOnForbiddenTable() throws Exception
  {
    assertForbidden(
        "SELECT * FROM druid.forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.forbiddenDatasource",
        NO_PARAM,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        null,
        newScan().dataSource(FORBIDDEN_DATASOURCE)
                 .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                 .streaming(),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "forbidden", "abcd", 9999.0d, 0.0, HyperLogLogCollector.class.getName()}
        )
    );
  }

  @Test
  public void testUnqualifiedTableName() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testExplainSelectStar() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR SELECT * FROM druid.foo",
        new Object[]{
            "DruidQueryRel(table=[druid.foo])\n"
        }
    );
  }

  @Test
  public void testSelectStarWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo LIMIT 2",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns(Arrays.asList("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
                 .limit(2)
                 .streaming(),
        new Object[]{T("2000-01-01"), 1L, "", "a", 1.0d, 1.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2000-01-02"), 1L, "10.1", "", 2.0d, 2.0, HyperLogLogCollector.class.getName()}
    );
  }

  @Test
  public void testSelectOnMultiValue() throws Exception
  {
    testQuery(
        "SELECT dim1,dim2 FROM druid.foo3 LIMIT 3",
        newScan().dataSource(CalciteTests.DATASOURCE3)
                 .columns(Arrays.asList("dim1", "dim2"))
                 .limit(3)
                 .streaming(),
        new Object[]{"", "[a, b]"},
        new Object[]{"10.1", ""},
        new Object[]{"2", "b"}
    );
  }

  @Test
  public void testSelectWithProjection() throws Exception
  {
    testQuery(
        "SELECT SUBSTRING(dim2, 1, 1) FROM druid.foo LIMIT 2",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns(Arrays.asList("v0"))
                 .virtualColumns(EXPR_VC("v0", "substring(dim2, 0, 1)"))
                 .limit(2)
                 .streaming(),
        new Object[]{"a"},
        new Object[]{""}
    );
  }

  @Test
  public void testSelectStarWithLimitTimeDescending() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo ORDER BY __time DESC LIMIT 2",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                 .descending(true)
                 .orderBy(OrderByColumnSpec.desc("__time"))
                 .limit(2)
                 .streaming(),
        new Object[]{T("2001-01-03"), 1L, "abc", "", 6d, 6d, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-02"), 1L, "def", "abc", 5d, 5d, HyperLogLogCollector.class.getName()}
    );
  }

  @Test
  public void testSelectStarWithoutLimitTimeAscending() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo ORDER BY __time",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                 .orderBy(OrderByColumnSpec.asc("__time"))
                 .streaming(),
        new Object[]{T("2000-01-01"), 1L, "", "a", 1d, 1.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2000-01-02"), 1L, "10.1", "", 2d, 2.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2000-01-03"), 1L, "2", "", 3d, 3.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-01"), 1L, "1", "a", 4d, 4.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-02"), 1L, "def", "abc", 5d, 5.0, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-03"), 1L, "abc", "", 6d, 6.0, HyperLogLogCollector.class.getName()}
    );
  }

  @Test
  public void testSelectSingleColumnTwice() throws Exception
  {
    testQuery(
        "SELECT dim2 x, dim2 y FROM druid.foo LIMIT 2",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns("dim2", "dim2")
                 .limit(2)
                 .streaming(),
        new Object[]{"a", "a"},
        new Object[]{"", ""}
    );
  }

  @Test
  public void testSelectSingleColumnWithLimitDescending() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns("dim1", "__time")
                 .descending(true)
                 .orderBy(OrderByColumnSpec.desc("__time"))
                 .limit(2)
                 .streaming(),
        new Object[]{"abc"},
        new Object[]{"def"}
    );
  }

  @Test
  public void testOrderByOnTypeCasting() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY cast(dim1 as bigint) DESC LIMIT 3",
        newScan().dataSource(CalciteTests.DATASOURCE1)
                 .columns("dim1", "v0")
                 .virtualColumns(EXPR_VC("v0", "CAST(dim1, 'LONG')"))
                 .orderBy(OrderByColumnSpec.desc("v0"))
                 .limit(3)
                 .streaming(),
        new Object[]{"10.1"},
        new Object[]{"2"},
        new Object[]{"1"}
    );
  }

  @Test
  public void testGroupBySingleColumnDescendingNoTopN() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
            .outputColumns("d0")
            .build(),
        new Object[]{"def"},
        new Object[]{"abc"},
        new Object[]{"2"},
        new Object[]{"10.1"},
        new Object[]{"1"},
        new Object[]{""}
    );
  }

  @Test
  public void testSelfJoinWithFallback() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ''",
        newJoin()
            .dataSource("foo$", newScan()
                .dataSource("foo")
                .columns("dim1", "dim2")
                .filters(NOT(SELECTOR("dim2", "")))
                .streaming()
            )
            .dataSource("foo", newScan()
                .dataSource("foo")
                .columns(Arrays.asList("dim1"))
                .filters(NOT(SELECTOR("dim1", "")))
                .streaming())
            .element(JoinElement.inner("foo.dim1 = foo$.dim2"))
            .build(),
        new Object[]{"abc", "def", "abc"}
    );
  }

  @Test
  public void testGroupByLong() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY cnt",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("cnt", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1L, 6L}
    );
  }

  @Test
  public void testGroupByOrdinal() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("cnt", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1L, 6L}
    );
  }

  @Test
  @Ignore // Disabled since GROUP BY alias can confuse the validator; see DruidConformance::isGroupByAlias
  public void testGroupByAndOrderByAlias() throws Exception
  {
    testQuery(
        "SELECT cnt AS theCnt, COUNT(*) FROM druid.foo GROUP BY theCnt ORDER BY theCnt ASC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("cnt", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.NUMERIC_NAME)))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1L, 6L}
    );
  }

  @Test
  public void testGroupByExpressionAliasedAsOriginalColumnName() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "FLOOR(__time TO MONTH) AS __time,\n"
        + "COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY FLOOR(__time TO MONTH)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(Granularities.MONTH)
              .aggregators(CountAggregatorFactory.of("a0"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{T("2000-01-01"), 3L},
        new Object[]{T("2001-01-01"), 3L}
    );

    testQuery(
        "SELECT\n"
        + "CEIL(__time TO MONTH) AS __time,\n"
        + "COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY CEIL(__time TO MONTH)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(Granularities.MONTH)
              .aggregators(CountAggregatorFactory.of("a0"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_ceil(__time,'P1M','','UTC')"))
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{T("2000-02-01"), 3L},
        new Object[]{T("2001-02-01"), 3L}
    );
  }

  @Test
  public void testGroupByAndOrderByOrdinalOfAlias() throws Exception
  {
    testQuery(
        "SELECT cnt as theCnt, COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 1 ASC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("cnt", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1L, 6L}
    );
  }

  @Test
  public void testGroupByFloat() throws Exception
  {
    testQuery(
        "SELECT m1, COUNT(*) FROM druid.foo GROUP BY m1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("m1", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1.0d, 1L},
        new Object[]{2.0d, 1L},
        new Object[]{3.0d, 1L},
        new Object[]{4.0d, 1L},
        new Object[]{5.0d, 1L},
        new Object[]{6.0d, 1L}
    );
  }

  @Test
  public void testGroupByDouble() throws Exception
  {
    testQuery(
        "SELECT m2, COUNT(*) FROM druid.foo GROUP BY m2",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("m2", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1.0d, 1L},
        new Object[]{2.0d, 1L},
        new Object[]{3.0d, 1L},
        new Object[]{4.0d, 1L},
        new Object[]{5.0d, 1L},
        new Object[]{6.0d, 1L}
    );
  }

  @Test
  public void testFilterOnFloat() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m1 = 1.0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0"))
              .filters(SELECTOR("m1", "1.0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testFilterOnDouble() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m2 = 1.0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0"))
              .filters(SELECTOR("m2", "1.0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testHavingOnGrandTotal() throws Exception
  {
    testQuery(
        "SELECT SUM(m1) AS m1_sum FROM foo HAVING m1_sum = 21",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
              .havingSpec(EXPR_HAVING("(a0 == 21)"))
              .outputColumns("a0")
              .build(),
        new Object[]{21d}
    );
  }

  @Test
  public void testHavingOnDoubleSum() throws Exception
  {
    testQuery(
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING SUM(m1) > 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
            .havingSpec(EXPR_HAVING("(a0 > 1)"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"1", 4.0d},
        new Object[]{"10.1", 2.0d},
        new Object[]{"2", 3.0d},
        new Object[]{"abc", 6.0d},
        new Object[]{"def", 5.0d}
    );
  }

  @Test
  public void testHavingOnApproximateCountDistinct() throws Exception
  {
    testQuery(
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
            .aggregators(CARDINALITY("a0", "m1"))
            .havingSpec(EXPR_HAVING("(a0 > 1)"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"", 3L},
        new Object[]{"a", 2L}
    );
  }

  @Test
  public void testHavingOnExactCountDistinct() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        newGroupBy()
            .dataSource(
                newGroupBy()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .dimensions(
                        DefaultDimensionSpec.of("dim2", "d0"),
                        DefaultDimensionSpec.of("m1", "d1")
                    )
                    .outputColumns("d0", "d1")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("d0", "_d0"))
            .aggregators(CountAggregatorFactory.of("a0", "d1"))
            .havingSpec(EXPR_HAVING("(a0 > 1)"))
            .outputColumns("_d0", "a0")
            .build(),
        new Object[]{"", 3L},
        new Object[]{"a", 2L}
    );
  }

  @Test
  public void testHavingOnFloatSum() throws Exception
  {
    testQuery(
        "SELECT dim1, CAST(SUM(m1) AS FLOAT) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING CAST(SUM(m1) AS FLOAT) > 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
            .postAggregators(EXPR_POST_AGG("p0", "CAST(a0, 'FLOAT')"))
            .havingSpec(EXPR_HAVING("(CAST(a0, 'FLOAT') > 1)"))
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{"1", 4.0f},
        new Object[]{"10.1", 2.0f},
        new Object[]{"2", 3.0f},
        new Object[]{"abc", 6.0f},
        new Object[]{"def", 5.0f}
    );

    testQuery(
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING CAST(SUM(m1) AS FLOAT) > 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
            .havingSpec(EXPR_HAVING("(CAST(a0, 'FLOAT') > 1)"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"1", 4.0d},
        new Object[]{"10.1", 2.0d},
        new Object[]{"2", 3.0d},
        new Object[]{"abc", 6.0d},
        new Object[]{"def", 5.0d}
    );
  }

  @Test
  @Ignore("dimension is now any type, which disables implicit casting on it")
  public void testColumnComparison() throws Exception
  {
    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - 1 = dim1 GROUP BY dim1, m1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .filters(EXPR_FILTER("((m1 - 1) == CAST(dim1, 'DOUBLE'))"))
            .dimensions(
                DefaultDimensionSpec.of("dim1", "d0"),
                DefaultDimensionSpec.of("m1", "d1")
            )
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{"2", 3.0d, 1L}
    );
  }

  @Test
  public void testHavingOnRatio() throws Exception
  {
    // Test for https://github.com/druid-io/druid/issues/4264

    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  COUNT(*) FILTER(WHERE dim2 <> 'a')/COUNT(*) as ratio\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "HAVING COUNT(*) FILTER(WHERE dim2 <> 'a')/COUNT(*) = 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(
                new FilteredAggregatorFactory(
                    CountAggregatorFactory.of("a0"),
                    NOT(SELECTOR("dim2", "a"))
                ),
                CountAggregatorFactory.of("a1")
            )
            .postAggregators(EXPR_POST_AGG("p0", "(a0 / a1)"))
            .havingSpec(EXPR_HAVING("((a0 / a1) == 1)"))
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{"10.1", 1L},
        new Object[]{"2", 1L},
        new Object[]{"abc", 1L},
        new Object[]{"def", 1L}
    );
  }

  @Test
  public void testGroupByWithSelectProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .postAggregators(EXPR_POST_AGG("p0", "substring(d0, 1, -1)"))
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{"", ""},
        new Object[]{"1", ""},
        new Object[]{"10.1", "0.1"},
        new Object[]{"2", ""},
        new Object[]{"abc", "bc"},
        new Object[]{"def", "ef"}
    );
  }

  @Test
  public void testGroupByWithSelectAndOrderByProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "ORDER BY CHARACTER_LENGTH(dim1) DESC, dim1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .postAggregators(
                EXPR_POST_AGG("p0", "substring(d0, 1, -1)"),
                EXPR_POST_AGG("p1", "strlen(d0)")
            )
            .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("p1"), OrderByColumnSpec.asc("d0")))
            .outputColumns("d0", "p0", "p1")
            .build(),
        new Object[]{"10.1", "0.1"},
        new Object[]{"abc", "bc"},
        new Object[]{"def", "ef"},
        new Object[]{"1", ""},
        new Object[]{"2", ""},
        new Object[]{"", ""}
    );
  }

  @Test
  public void testTopNWithSelectProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "LIMIT 10",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim1", "d0"))
            .postAggregators(EXPR_POST_AGG("s0", "substring(d0, 1, -1)"))
            .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
            .outputColumns("d0", "s0")
            .threshold(10)
            .build(),
        new Object[]{"", ""},
        new Object[]{"1", ""},
        new Object[]{"10.1", "0.1"},
        new Object[]{"2", ""},
        new Object[]{"abc", "bc"},
        new Object[]{"def", "ef"}
    );
  }

  @Test
  public void testTopNWithSelectAndOrderByProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "ORDER BY CHARACTER_LENGTH(dim1) DESC\n"
        + "LIMIT 10",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim1", "d0"))
            .postAggregators(
                EXPR_POST_AGG("p0", "substring(d0, 1, -1)"),
                EXPR_POST_AGG("p1", "strlen(d0)")
            )
            .metric(new NumericTopNMetricSpec("p1"))
            .outputColumns("d0", "p0", "p1")
            .threshold(10)
            .build(),
        new Object[]{"10.1", "0.1"},
        new Object[]{"abc", "bc"},
        new Object[]{"def", "ef"},
        new Object[]{"1", ""},
        new Object[]{"2", ""},
        new Object[]{"", ""}
    );
  }


  @Test
  public void testUnionAll() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo",
        UnionAllQuery.union(
            Arrays.asList(
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .aggregators(CountAggregatorFactory.of("a0"))
                      .outputColumns("a0")
                      .build(),
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .outputColumns("a0")
                      .build(),
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .aggregators(CountAggregatorFactory.of("a0"))
                      .outputColumns("a0")
                      .build()
            )
        ),
        new Object[]{6L}, new Object[]{6L}, new Object[]{6L}
    );
  }

  @Test
  public void testUnionAllWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM ("
        + "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo"
        + ") LIMIT 2",
        UnionAllQuery.union(
            Arrays.asList(
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .aggregators(CountAggregatorFactory.of("a0"))
                      .outputColumns("a0")
                      .limit(2)
                      .build(),
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .outputColumns("a0")
                      .limit(2)
                      .build(),
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .aggregators(CountAggregatorFactory.of("a0"))
                      .outputColumns("a0")
                      .limit(2)
                      .build()
            ),
            2
        ),
        new Object[]{6L}, new Object[]{6L}
    );
  }

  @Test
  public void testPruneDeadAggregators() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.

    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt)\n"
        + "  WHEN 'foo' THEN SUM(m1)\n"
        + "  WHEN 'baz' THEN SUM(m2)\n"
        + "  END\n"
        + "FROM foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
              .outputColumns("a0")
              .build(),
        new Object[]{21.0}
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughPostProjection() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.

    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt) / 10\n"
        + "  WHEN 'foo' THEN SUM(m1) / 10\n"
        + "  WHEN 'baz' THEN SUM(m2) / 10\n"
        + "  END\n"
        + "FROM foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
              .postAggregators(EXPR_POST_AGG("p0", "(a0 / 10)"))
              .outputColumns("p0")
              .build(),
        new Object[]{2.1}
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughHaving() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.

    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt)\n"
        + "  WHEN 'foo' THEN SUM(m1)\n"
        + "  WHEN 'baz' THEN SUM(m2)\n"
        + "  END AS theCase\n"
        + "FROM foo\n"
        + "HAVING theCase = 21",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
              .havingSpec(EXPR_HAVING("(a0 == 21)"))
              .outputColumns("a0")
              .build(),
        new Object[]{21.0}
    );
  }

  @Test
  public void testGroupByCaseWhen() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CASE EXTRACT(DAY FROM __time)\n"
        + "    WHEN m1 THEN 'match-m1'\n"
        + "    WHEN cnt THEN 'match-cnt'\n"
        + "    WHEN 0 THEN 'zero'"
        + "    END,"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY"
        + "  CASE EXTRACT(DAY FROM __time)\n"
        + "    WHEN m1 THEN 'match-m1'\n"
        + "    WHEN cnt THEN 'match-cnt'\n"
        + "    WHEN 0 THEN 'zero'"
        + "    END",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC(
                    "d0:v",
                    "case("
                    + "(CAST(timestamp_extract('DAY',__time,'UTC'), 'DOUBLE') == m1),"
                    + "'match-m1',"
                    + "(timestamp_extract('DAY',__time,'UTC') == cnt),"
                    + "'match-cnt',"
                    + "(timestamp_extract('DAY',__time,'UTC') == 0),"
                    + "'zero',"
                    + "'')"
                )
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"", 2L},
        new Object[]{"match-cnt", 1L},
        new Object[]{"match-m1", 3L}
    );
  }

  @Test
  public void testGroupByCaseWhenOfTripleAnd() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CASE WHEN m1 > 1 AND m1 < 5 AND cnt = 1 THEN 'x' ELSE NULL END,"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC(
                    "d0:v",
                    "case(((m1 > 1) && (m1 < 5) && (cnt == 1)),'x','')"
                )
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"", 3L},
        new Object[]{"x", 3L}
    );
  }

  @Test
  public void testNullEmptyStringEquality() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE NULLIF(dim2, 'a') IS NULL",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  OR(
                      SELECTOR("dim2", "a"),
                      AND(
                          SELECTOR("dim2", null),
                          NOT(SELECTOR("dim2", "a"))
                      )
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        // Matches everything but "abc"
        new Object[]{5L}
    );
  }

  @Test
  public void testCoalesceColumns() throws Exception
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    testQuery(
        "SELECT COALESCE(dim2, dim1), COUNT(*) FROM druid.foo GROUP BY COALESCE(dim2, dim1)\n",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC(
                    "d0:v",
                    "COALESCE(dim2,dim1)"
                )
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"10.1", 1L},
        new Object[]{"2", 1L},
        new Object[]{"a", 2L},
        new Object[]{"abc", 2L}
    );
  }

  @Test
  public void testColumnIsNull() throws Exception
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 IS NULL\n",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim2", null))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testUnplannableQueries() throws Exception
  {
    // All of these queries are unplannable because they rely on features Druid doesn't support.
    // This test is here to confirm that we don't fall back to Calcite's interpreter or enumerable implementation.
    // It's also here so when we do support these features, we can have "real" tests for these queries.

    final List<String> queries = ImmutableList.of(
//        "SELECT dim1 FROM druid.foo ORDER BY dim1", // SELECT query with order by
        "SELECT COUNT(*) FROM druid.foo x, druid.foo y", // Self-join
        "SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5" // DISTINCT with OFFSET
    );

    testQuery(queries.get(0), new Object[] {36L});
    hook.verifyHooked(
        "KJta7RCi5Zir3Ny01nqkvA==",
        "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='0', outputName='v0'}], $hash=true}, StreamQuery{dataSource='foo', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='0', outputName='v0'}]}], timeColumnName=__time}', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[a0]}",
        "StreamQuery{dataSource='foo', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='0', outputName='v0'}], $hash=true}",
        "StreamQuery{dataSource='foo', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='0', outputName='v0'}]}"
    );
    try {
      testQuery(queries.get(1));
      Assert.fail();
    }
    catch (RelOptPlanner.CannotPlanException e) {
    }
  }

  @Test
  public void testUnplannableExactCountDistinctQueries() throws Exception
  {
    // All of these queries are unplannable in exact COUNT DISTINCT mode.

    final List<String> queries = ImmutableList.of(
        "SELECT COUNT(distinct dim1), COUNT(distinct dim2) FROM druid.foo", // two COUNT DISTINCTs, same query
        "SELECT dim1, COUNT(distinct dim1), COUNT(distinct dim2) FROM druid.foo GROUP BY dim1", // two COUNT DISTINCTs
        "SELECT COUNT(distinct unique_dim1) FROM druid.foo" // COUNT DISTINCT on sketch cannot be exact
    );
    testQuery(queries.get(0), new Object[] {6L, 3L});
    hook.verifyHooked(
        "1lAQJ5+VVyCJK6ioAq5Vqw==",
        "TimeseriesQuery{dataSource='foo', aggregatorSpecs=[CardinalityAggregatorFactory{name='a0', fieldNames=[dim1], groupingSets=Noop, byRow=true, round=true, b=11}, CardinalityAggregatorFactory{name='a1', fieldNames=[dim2], groupingSets=Noop, byRow=true, round=true, b=11}], outputColumns=[a0, a1]}"
    );
    testQuery(
        queries.get(1),
        new Object[]{"", 1L, 1L},
        new Object[]{"1", 1L, 1L},
        new Object[]{"10.1", 1L, 1L},
        new Object[]{"2", 1L, 1L},
        new Object[]{"abc", 1L, 1L},
        new Object[]{"def", 1L, 1L}
    );
    hook.verifyHooked(
        "tTNHO4jIZURZegMs0XhiYQ==",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0', fieldNames=[dim1], groupingSets=Noop, byRow=true, round=true, b=11}, CardinalityAggregatorFactory{name='a1', fieldNames=[dim2], groupingSets=Noop, byRow=true, round=true, b=11}], outputColumns=[d0, a0, a1]}"
    );
    testQuery(queries.get(2), new Object[] {6L});
    hook.verifyHooked(
        "BXJ8y7AKRy2BZktOORIUAA==",
        "TimeseriesQuery{dataSource='foo', aggregatorSpecs=[HyperUniquesAggregatorFactory{name='a0', fieldName='unique_dim1', round=true, b=11}], outputColumns=[a0]}"
    );
  }

  @Test
  public void testSelectStarWithDimFilter() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo WHERE dim1 > 'd' OR dim2 = 'a'",
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .columns(Arrays.asList("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
            .filters(
                OR(
                    BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC_NAME),
                    SELECTOR("dim2", "a")
                )
            )
            .streaming(),
        new Object[]{T("2000-01-01"), 1L, "", "a", 1.0d, 1.0d, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-01"), 1L, "1", "a", 4.0d, 4.0d, HyperLogLogCollector.class.getName()},
        new Object[]{T("2001-01-02"), 1L, "def", "abc", 5.0d, 5.0d, HyperLogLogCollector.class.getName()}
    );
  }

  @Test
  public void testGroupByNothingWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0",
        TypedDummyQuery.DUMMY,
        new Object[]{0L, null}
    );
  }

  @Test
  public void testGroupByOneColumnWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0 GROUP BY dim1",
        TypedDummyQuery.DUMMY
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothing() throws Exception
  {
    // This query should actually return [0, null] rather than an empty result set, but it doesn't.
    // This test just "documents" the current behavior.

    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim1", "foobar"))
              .aggregators(
                  CountAggregatorFactory.of("a0"),
                  GenericMaxAggregatorFactory.ofLong("a1", "cnt")
              )
              .outputColumns("a0", "a1")
              .build()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothingWithGroupByLiteral() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar' GROUP BY 'dummy'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim1", "foobar"))
              .aggregators(
                  CountAggregatorFactory.of("a0"),
                  GenericMaxAggregatorFactory.ofLong("a1", "cnt")
              )
              .outputColumns("a0", "a1")
              .build()
    );
  }

  @Test
  public void testCountNonNullColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(cnt) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0", "cnt"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testCountNullableColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(dim2) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0", "dim2"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testCountNullableExpression() throws Exception
  {
    testQuery(
        "SELECT COUNT(CASE WHEN dim2 = 'abc' THEN 'yes' WHEN dim2 = 'def' THEN 'yes' END) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  new FilteredAggregatorFactory(
                      CountAggregatorFactory.of("a0"),
                      IN("dim2", Arrays.asList("abc", "def"), null)
                  )
              )
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStar() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testCountStarOnCommonTableExpression() throws Exception
  {
    testQuery(
        "WITH beep (dim1_firstchar) AS (SELECT SUBSTRING(dim1, 1, 1) FROM foo WHERE dim2 = 'a')\n"
        + "SELECT COUNT(*) FROM beep WHERE dim1_firstchar <> 'z'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(AND(
                  SELECTOR("dim2", "a"),
                  NOT(SELECTOR("dim1", "z", SUBSTRING_FN(0, 1)))
              ))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
  }

  @Test
  public void testCountStarOnView() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.aview WHERE dim1_firstchar <> 'z'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(AND(
                  SELECTOR("dim2", "a"),
                  NOT(SELECTOR("dim1", "z", SUBSTRING_FN(0, 1)))
              ))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
  }

  @Test
  public void testExplainCountStarOnView() throws Exception
  {
    // #4103 moved some rules to hep program. so view cannot exploit them.
    // we don't use views. who cares
    String explanation =
        "DruidQueryRel(table=[druid.foo], "
        + "scanFilter=[AND(=($3, 'a'), <>(SUBSTRING($2, 1, 1), 'z'))], "
        + "scanProject=[SUBSTRING($2, 1, 1)], EXPR$0=[COUNT()])\n";

    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR SELECT COUNT(*) FROM aview WHERE dim1_firstchar <> 'z'",
        new Object[]{explanation}
    );

    explanation = "{\n"
                  + "  \"queryType\" : \"timeseries\",\n"
                  + "  \"dataSource\" : {\n"
                  + "    \"type\" : \"table\",\n"
                  + "    \"name\" : \"foo\"\n"
                  + "  },\n"
                  + "  \"descending\" : false,\n"
                  + "  \"filter\" : {\n"
                  + "    \"type\" : \"and\",\n"
                  + "    \"fields\" : [ {\n"
                  + "      \"type\" : \"selector\",\n"
                  + "      \"dimension\" : \"dim2\",\n"
                  + "      \"value\" : \"a\"\n"
                  + "    }, {\n"
                  + "      \"type\" : \"not\",\n"
                  + "      \"field\" : {\n"
                  + "        \"type\" : \"selector\",\n"
                  + "        \"dimension\" : \"dim1\",\n"
                  + "        \"value\" : \"z\",\n"
                  + "        \"extractionFn\" : {\n"
                  + "          \"type\" : \"substring\",\n"
                  + "          \"index\" : 0,\n"
                  + "          \"length\" : 1\n"
                  + "        }\n"
                  + "      }\n"
                  + "    } ]\n"
                  + "  },\n"
                  + "  \"granularity\" : {\n"
                  + "    \"type\" : \"all\"\n"
                  + "  },\n"
                  + "  \"aggregations\" : [ {\n"
                  + "    \"type\" : \"count\",\n"
                  + "    \"name\" : \"a0\"\n"
                  + "  } ],\n"
                  + "  \"limitSpec\" : {\n"
                  + "    \"type\" : \"noop\"\n"
                  + "  },\n"
                  + "  \"outputColumns\" : [ \"a0\" ],\n"
                  + "  \"context\" : {\n"
                  + "    \"sqlCurrentTimestamp\" : \"2000-01-01T00:00:00Z\",\n"
                  + "    \"groupby.sort.on.time\" : false\n"
                  + "  }\n"
                  + "}";
    testQuery(
        "EXPLAIN PLAN WITH IMPLEMENTATION FOR SELECT COUNT(*) FROM aview WHERE dim1_firstchar <> 'z'",
        new Object[]{explanation}
    );
  }

//  @Test
//  public void testCountStarWithLikeFilter() throws Exception
//  {
//    testQuery(
//        "SELECT COUNT(*) FROM druid.foo WHERE dim1 like 'a%' OR dim2 like '%xb%' escape 'x'",
//        ImmutableList.of(
//            Druids.newTimeseriesQueryBuilder()
//                  .dataSource(CalciteTests.DATASOURCE1)
//                  //                  .granularity(Granularities.ALL)
//                  .filters(
//                      OR(
//                          new LikeDimFilter("dim1", "a%", null, null),
//                          new LikeDimFilter("dim2", "%xb%", "x", null)
//                      )
//                  )
//                  .aggregators(CountAggregatorFactory.of("a0")))
//                  .context(TIMESERIES_CONTEXT_DEFAULT)
//                  .build()
//        ),
//        ImmutableList.of(
//            new Object[]{2L}
//        )
//    );
//  }

  @Test
  public void testCountStarWithLongColumnFilters() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt >= 3 OR cnt = 1",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  OR(
                      BOUND("cnt", "3", null, false, false, null, StringComparators.NUMERIC_NAME),
                      SELECTOR("cnt", "1")
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnFloatLiterals() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt > 1.1 and cnt < 100000001.0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  BOUND("cnt", "1.1", "100000001.0", true, true, null, StringComparators.NUMERIC_NAME)
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build()
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(EXPR_FILTER("(CAST(cnt, 'decimal') == 1.0B)"))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 100000001.0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(EXPR_FILTER("(CAST(cnt, 'decimal') == 100000001.0B)"))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build()
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0 or cnt = 100000001.0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(OR(
                  EXPR_FILTER("(CAST(cnt, 'decimal') == 1.0B)"),
                  EXPR_FILTER("(CAST(cnt, 'decimal') == 100000001.0B)")
              ))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnTwoPoints() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1 OR cnt = 2",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(IN("cnt", "1", "2"))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testFilterOnStringAsNumber() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE "
        + "dim1 = 10 OR "
        + "(floor(CAST(dim1 AS float)) = 10.00 and CAST(dim1 AS float) > 9 and CAST(dim1 AS float) <= 10.5)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .filters(
                OR(
                    SELECTOR("dim1", "10"),
                    AND(
                        EXPR_FILTER("(floor(CAST(dim1, 'FLOAT')) == 10.00F)"),
                        EXPR_FILTER("(CAST(dim1, 'FLOAT') > 9)"),
                        EXPR_FILTER("(CAST(dim1, 'FLOAT') <= 10.5B)")
                    )
                )
            )
            .outputColumns("d0")
            .build(),
        new Object[]{"10.1"}
    );
  }

  @Test
  public void testSimpleAggregations() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), COUNT(cnt), COUNT(dim1), MIN(dim1), MAX(dim1), AVG(cnt), SUM(cnt), SUM(cnt) + MIN(cnt) + MAX(cnt) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  CountAggregatorFactory.of("a0"),
                  CountAggregatorFactory.of("a1", "cnt"),
                  CountAggregatorFactory.of("a2", "dim1"),
                  RelayAggregatorFactory.min("a3", "dim1", ValueDesc.STRING_DIMENSION_TYPE),
                  RelayAggregatorFactory.max("a4", "dim1", ValueDesc.STRING_DIMENSION_TYPE),
                  GenericSumAggregatorFactory.ofLong("a5:sum", "cnt"),
                  CountAggregatorFactory.of("a5:count"),
                  GenericSumAggregatorFactory.ofLong("a6", "cnt"),
                  GenericMinAggregatorFactory.ofLong("a7", "cnt"),
                  GenericMaxAggregatorFactory.ofLong("a8", "cnt")
              )
              .postAggregators(
                  new ArithmeticPostAggregator(
                      "a5",
                      "quotient",
                      ImmutableList.of(
                          new FieldAccessPostAggregator(null, "a5:sum"),
                          new FieldAccessPostAggregator(null, "a5:count")
                      )
                  ),
                  EXPR_POST_AGG("p0", "((a6 + a7) + a8)")
              )
              .outputColumns("a0", "a1", "a2", "a3", "a4", "a5", "a6", "p0")
              .build(),
        new Object[]{6L, 6L, 5L, "1", "def", 1L, 6L, 8L}
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationDefault() throws Exception
  {
    // By default this query uses topN.

    testQuery(
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim1", "d0"))
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("p0")))
            .aggregators(
                GenericMinAggregatorFactory.ofDouble("a0", "m1"),
                GenericMaxAggregatorFactory.ofDouble("a1", "m1")
            )
            .postAggregators(EXPR_POST_AGG("p0", "(a0 + a1)"))
            .threshold(3)
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{"", 2.0d},
        new Object[]{"10.1", 4.0d},
        new Object[]{"2", 6.0d}
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNConfig() throws Exception
  {
    // Use PlannerConfig to disable topN, so this query becomes a groupBy.

    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(
                GenericMinAggregatorFactory.ofDouble("a0", "m1"),
                GenericMaxAggregatorFactory.ofDouble("a1", "m1")
            )
            .postAggregators(EXPR_POST_AGG("p0", "(a0 + a1)"))
            .limitSpec(LimitSpec.of(3, OrderByColumnSpec.asc("p0")))
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{"", 2.0d},
        new Object[]{"10.1", 4.0d},
        new Object[]{"2", 6.0d}
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNContext() throws Exception
  {
    // Use context to disable topN, so this query becomes a groupBy.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
            newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(
                GenericMinAggregatorFactory.ofDouble("a0", "m1"),
                GenericMaxAggregatorFactory.ofDouble("a1", "m1")
            )
            .postAggregators(EXPR_POST_AGG("p0", "(a0 + a1)"))
            .limitSpec(LimitSpec.of(3, OrderByColumnSpec.asc("p0")))
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{"", 2.0d},
        new Object[]{"10.1", 4.0d},
        new Object[]{"2", 6.0d}
    );
  }

  @Test
  public void testFilteredAggregations() throws Exception
  {
    testQuery(
        "SELECT "
        + "SUM(case dim1 when 'abc' then cnt end), "
        + "SUM(case dim1 when 'abc' then null else cnt end), "
        + "SUM(case substring(dim1, 1, 1) when 'a' then cnt end), "
        + "COUNT(dim2) filter(WHERE dim1 <> '1'), "
        + "COUNT(CASE WHEN dim1 <> '1' THEN 'dummy' END), "
        + "SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END), "
        + "SUM(cnt) filter(WHERE dim2 = 'a'), "
        + "SUM(case when dim1 <> '1' then cnt end) filter(WHERE dim2 = 'a'), "
        + "SUM(CASE WHEN dim1 <> '1' THEN cnt ELSE 0 END), "
        + "MAX(CASE WHEN dim1 <> '1' THEN cnt END) "
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                      SELECTOR("dim1", "abc")
                  ),
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a1", "cnt"),
                      NOT(SELECTOR("dim1", "abc"))
                  ),
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a2", "cnt"),
                      SELECTOR("dim1", "a", SUBSTRING_FN(0, 1))
                  ),
                  new FilteredAggregatorFactory(
                      CountAggregatorFactory.of("a3", "dim2"),
                      NOT(SELECTOR("dim1", "1"))
                  ),
                  new FilteredAggregatorFactory(
                      CountAggregatorFactory.of("a4"),
                      NOT(SELECTOR("dim1", "1"))
                  ),
                  new FilteredAggregatorFactory(
                      CountAggregatorFactory.of("a5"),
                      NOT(SELECTOR("dim1", "1"))
                  ),
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a6", "cnt"),
                      SELECTOR("dim2", "a")
                  ),
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a7", "cnt"),
                      AND(
                          SELECTOR("dim2", "a"),
                          NOT(SELECTOR("dim1", "1"))
                      )
                  ),
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a8", "cnt"),
                      NOT(SELECTOR("dim1", "1"))
                  ),
                  new FilteredAggregatorFactory(
                      GenericMaxAggregatorFactory.ofLong("a9", "cnt"),
                      NOT(SELECTOR("dim1", "1"))
                  )
              )
              .outputColumns("a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9")
              .build(),
        new Object[]{1L, 5L, 1L, 2L, 5L, 5L, 2L, 1L, 5L, 1L}
    );
  }

  @Test
  public void testCaseFilteredAggregationWithGroupBy() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  cnt,\n"
        + "  SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END) + SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY cnt",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("cnt", "d0"))
            .aggregators(
                new FilteredAggregatorFactory(
                    CountAggregatorFactory.of("a0"),
                    NOT(SELECTOR("dim1", "1"))
                ),
                GenericSumAggregatorFactory.ofLong("a1", "cnt")
            )
            .postAggregators(EXPR_POST_AGG("p0", "(a0 + a1)"))
            .outputColumns("d0", "p0")
            .build(),
        new Object[]{1L, 11L}
    );
  }

  @Test
  @Ignore // https://issues.apache.org/jira/browse/CALCITE-1910
  public void testFilteredAggregationWithNotIn() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "COUNT(*) filter(WHERE dim1 NOT IN ('1')),\n"
        + "COUNT(dim2) filter(WHERE dim1 NOT IN ('1'))\n"
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .outputColumns("a0")
              .build(),
        new Object[]{1L, 5L}
    );
  }

  @Test
  public void testExpressionAggregations() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt * 3),\n"
        + "  LN(SUM(cnt) + SUM(m1)),\n"
        + "  MOD(SUM(cnt), 4),\n"
        + "  SUM(CHARACTER_LENGTH(CAST(cnt * 10 AS VARCHAR))),\n"
        + "  MAX(CHARACTER_LENGTH(dim2) + LN(m1))\n"
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  GenericSumAggregatorFactory.expr("a0", "(cnt * 3)", ValueDesc.LONG),
                  GenericSumAggregatorFactory.ofLong("a1", "cnt"),
                  GenericSumAggregatorFactory.ofDouble("a2", "m1"),
                  GenericSumAggregatorFactory.expr("a3", "strlen(CAST((cnt * 10), 'STRING'))", ValueDesc.LONG),
                  GenericMaxAggregatorFactory.expr("a4", "(strlen(dim2) + log(m1))", ValueDesc.DOUBLE)
              )
              .postAggregators(
                  EXPR_POST_AGG("p0", "log((a1 + a2))"),
                  EXPR_POST_AGG("p1", "(a1 % 4)")
              )
              .outputColumns("a0", "p0", "p1", "a3", "a4")
              .build(),
        new Object[]{18L, 3.295836866004329, 2, 12L, 3f + (Math.log(5.0))}
    );
  }

  @Test
  public void testExpressionFilteringAndGrouping() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  FLOOR(m1 / 2) * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(m1 / 2) * 2 > -1\n"
        + "GROUP BY FLOOR(m1 / 2) * 2\n"
        + "ORDER BY 1 DESC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC("d0:v", "(floor((m1 / 2)) * 2)")
            )
            .filters(EXPR_FILTER("((floor((m1 / 2)) * 2) > -1)"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{6.0d, 1L},
        new Object[]{4.0d, 2L},
        new Object[]{2.0d, 2L},
        new Object[]{0.0d, 1L}
    );
  }

  @Test
  public void testExpressionFilteringAndGroupingUsingCastToLong() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(m1 AS BIGINT) / 2 * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE CAST(m1 AS BIGINT) / 2 * 2 > -1\n"
        + "GROUP BY CAST(m1 AS BIGINT) / 2 * 2\n"
        + "ORDER BY 1 DESC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("d0:v", "((CAST(m1, 'LONG') / 2) * 2)"))
            .filters(EXPR_FILTER("(((CAST(m1, 'LONG') / 2) * 2) > -1)"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{6L, 1L},
        new Object[]{4L, 2L},
        new Object[]{2L, 2L},
        new Object[]{0L, 1L}
    );
  }

  @Test
  public void testExpressionFilteringAndGroupingOnStringCastToNumber() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  FLOOR(CAST(dim1 AS FLOAT) / 2) * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(CAST(dim1 AS FLOAT) / 2) * 2 > -1\n"
        + "GROUP BY FLOOR(CAST(dim1 AS FLOAT) / 2) * 2\n"
        + "ORDER BY 1 DESC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("d0:v", "(floor((CAST(dim1, 'FLOAT') / 2)) * 2)"))
            .filters(EXPR_FILTER("((floor((CAST(dim1, 'FLOAT') / 2)) * 2) > -1)"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{10.0f, 1L},
        new Object[]{2.0f, 1L},
        new Object[]{0.0f, 1L}
    );
  }

  @Test
  public void testInFilter() throws Exception
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') GROUP BY dim1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .filters(IN("dim1", "abc", "def", "ghi"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"abc", 1L},
        new Object[]{"def", 1L}
    );
  }

  @Test
  public void testInFilterWith23Elements() throws Exception
  {
    // Regression test for https://github.com/druid-io/druid/issues/4203.

    final List<String> elements = new ArrayList<>();
    elements.add("abc");
    elements.add("def");
    elements.add("ghi");
    for (int i = 0; i < 20; i++) {
      elements.add("dummy" + i);
    }

    final String elementsString = Joiner.on(",").join(elements.stream().map(s -> "'" + s + "'").iterator());

    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN (" + elementsString + ") GROUP BY dim1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .filters(IN("dim1", elements, null))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"abc", 1L},
        new Object[]{"def", 1L}
    );
  }

  @Test
  public void testCountStarWithDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and (dim1 > 'a' OR dim1 < 'b')",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim2", "a"))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
  }

  @Test
  public void testCountStarWithNotOfDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and not (dim1 > 'a' OR dim1 < 'b')",
        TypedDummyQuery.DUMMY,
        new Object[]{0L}
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOnMetric() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE 2.5 < m1 AND m1 < 3.5",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(BOUND("m1", "2.5", "3.5", true, true, null, StringComparators.NUMERIC_NAME))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOr() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') OR dim1 = 'ab'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(BOUND("dim1", "a", "b", false, true, null, StringComparators.LEXICOGRAPHIC_NAME))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyAnd() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') and dim1 = 'abc'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim1", "abc"))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStarWithFilterOnCastedString() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE CAST(dim1 AS bigint) = 2",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim1", "2"))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStarWithTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01/2001-01-01")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testCountStarWithTimeFilterUsingStringLiterals() throws Exception
  {
    // Strings are implicitly cast to timestamps.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= '2000-01-01 00:00:00' AND __time < '2001-01-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01/2001-01-01")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testRemoveUselessCaseWhen() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "  CASE\n"
        + "    WHEN __time >= TIME_PARSE('2000-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AND __time < TIMESTAMP '2001-01-01 00:00:00'\n"
        + "    THEN true\n"
        + "    ELSE false\n"
        + "  END\n"
        + "OR\n"
        + "  __time >= TIMESTAMP '2010-01-01 00:00:00' AND __time < TIMESTAMP '2011-01-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000/2001"), Intervals.of("2010/2011")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testCountStarWithTimeMillisecondFilters() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time = TIMESTAMP '2000-01-01 00:00:00.111'\n"
        + "OR (__time >= TIMESTAMP '2000-01-01 00:00:00.888' AND __time < TIMESTAMP '2000-01-02 00:00:00.222')",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(
                  QSS(
                      Intervals.of("2000-01-01T00:00:00.111/2000-01-01T00:00:00.112"),
                      Intervals.of("2000-01-01T00:00:00.888/2000-01-02T00:00:00.222")
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }


  @Test
  public void testCountStarWithSinglePointInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE __time = TIMESTAMP '2000-01-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01/2000-01-01T00:00:00.001")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStarWithTwoPointsInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "__time = TIMESTAMP '2000-01-01 00:00:00' OR __time = TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1' DAY",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(
                  QSS(
                      Intervals.of("2000-01-01/2000-01-01T00:00:00.001"),
                      Intervals.of("2000-01-02/2000-01-02T00:00:00.001")
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
  }

  @Test
  public void testCountStarWithComplexDisjointTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 = 'a' and ("
        + "  (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "  OR ("
        + "    (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "    and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "    and dim1 = 'abc'"
        + "  )"
        + ")",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000/2001"), Intervals.of("2002-05-01/2003-05-01")))
              .filters(
                  AND(
                      SELECTOR("dim2", "a"),
                      OR(
                          TIME_BOUND("2000/2001"),
                          AND(
                              SELECTOR("dim1", "abc"),
                              TIME_BOUND("2002-05-01/2003-05-01")
                          )
                      )
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testCountStarWithNotOfComplexDisjointTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE not (dim2 = 'a' and ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR ("
        + "      (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "      and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "      and dim1 = 'abc'"
        + "    )"
        + "  )"
        + ")",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  OR(
                      NOT(SELECTOR("dim2", "a")),
                      AND(
                          NOT(TIME_BOUND("2000/2001")),
                          NOT(AND(
                              SELECTOR("dim1", "abc"),
                              TIME_BOUND("2002-05-01/2003-05-01")
                          ))
                      )
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{5L}
    );
  }

  @Test
  public void testCountStarWithNotTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim1 <> 'xxx' and not ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR (__time >= TIMESTAMP '2003-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00'))",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(
                  QSS(
                      new Interval(DateTimes.MIN, DateTimes.of("2000")),
                      Intervals.of("2001/2003"),
                      new Interval(DateTimes.of("2004"), DateTimes.MAX)
                  )
              )
              .filters(NOT(SELECTOR("dim1", "xxx")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testCountStarWithTimeAndDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "and __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01/2001-01-01")))
              .filters(NOT(SELECTOR("dim2", "a")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
  }

  @Test
  public void testCountStarWithTimeOrDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "or __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  OR(
                      NOT(SELECTOR("dim2", "a")),
                      BOUND(
                          "__time",
                          String.valueOf(T("2000-01-01")),
                          String.valueOf(T("2000-12-31T23:59:59.999")),
                          false,
                          false,
                          null,
                          StringComparators.NUMERIC_NAME
                      )
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{5L}
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpoch() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM TIMESTAMP '1970-01-02 00:00:00') * 1000",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  BOUND(
                      "cnt",
                      String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                      String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                      false,
                      true,
                      null,
                      StringComparators.NUMERIC_NAME
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpochFromDate() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM DATE '1970-01-01') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM DATE '1970-01-02') * 1000",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  BOUND(
                      "cnt",
                      String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                      String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                      false,
                      true,
                      null,
                      StringComparators.NUMERIC_NAME
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingTimestampToMillis() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-01 00:00:00') "
        + "AND cnt < TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-02 00:00:00')",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(
                  BOUND(
                      "cnt",
                      String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                      String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                      false,
                      true,
                      null,
                      StringComparators.NUMERIC_NAME
                  )
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{6L}
    );
  }

  @Test
  public void testSumOfString() throws Exception
  {
    // Perhaps should be 13, but dim1 has "1", "2" and "10.1"; and CAST('10.1' AS INTEGER) = 0 since parsing is strict.

    testQuery(
        "SELECT SUM(CAST(dim1 AS INTEGER)) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(GenericSumAggregatorFactory.expr("a0", "CAST(dim1, 'LONG')", ValueDesc.LONG))
              .outputColumns("a0")
              .build(),
        new Object[]{13L}
    );
  }

  @Test
  public void testSumOfExtractionFn() throws Exception
  {
    // Perhaps should be 13, but dim1 has "1", "2" and "10.1"; and CAST('10.1' AS INTEGER) = 0 since parsing is strict.

    testQuery(
        "SELECT SUM(CAST(SUBSTRING(dim1, 1, 10) AS INTEGER)) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  GenericSumAggregatorFactory.expr("a0", "CAST(substring(dim1, 0, 10), 'LONG')", ValueDesc.LONG)
              )
              .outputColumns("a0")
              .build(),
        new Object[]{13L}
    );
  }

  @Test
  public void testTimeseriesWithTimeFilterOnLongColumnUsingMillisToTimestamp() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  FLOOR(MILLIS_TO_TIMESTAMP(cnt) TO YEAR),\n"
        + "  COUNT(*)\n"
        + "FROM\n"
        + "  druid.foo\n"
        + "WHERE\n"
        + "  MILLIS_TO_TIMESTAMP(cnt) >= TIMESTAMP '1970-01-01 00:00:00'\n"
        + "  AND MILLIS_TO_TIMESTAMP(cnt) < TIMESTAMP '1970-01-02 00:00:00'\n"
        + "GROUP BY\n"
        + "  FLOOR(MILLIS_TO_TIMESTAMP(cnt) TO YEAR)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .filters(
                BOUND(
                    "cnt",
                    String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                    String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                    false,
                    true,
                    null,
                    StringComparators.NUMERIC_NAME
                )
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .virtualColumns(EXPR_VC("d0:v", "timestamp_floor(cnt,'P1Y','','UTC')"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{T("1970-01-01"), 6L}
    );

    testQuery(
        "SELECT\n"
        + "  CEIL(MILLIS_TO_TIMESTAMP(cnt) TO YEAR),\n"
        + "  COUNT(*)\n"
        + "FROM\n"
        + "  druid.foo\n"
        + "WHERE\n"
        + "  MILLIS_TO_TIMESTAMP(cnt) >= TIMESTAMP '1970-01-01 00:00:00'\n"
        + "  AND MILLIS_TO_TIMESTAMP(cnt) < TIMESTAMP '1970-01-02 00:00:00'\n"
        + "GROUP BY\n"
        + "  CEIL(MILLIS_TO_TIMESTAMP(cnt) TO YEAR)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .filters(
                BOUND(
                    "cnt",
                    String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                    String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                    false,
                    true,
                    null,
                    StringComparators.NUMERIC_NAME
                )
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .virtualColumns(EXPR_VC("d0:v", "timestamp_ceil(cnt,'P1Y','','UTC')"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{T("1971-01-01"), 6L}
    );
  }

  @Test
  public void testSelectDistinctWithCascadeExtractionFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE substring(substring(dim1, 2), 1, 1) = 'e' OR dim2 = 'a'",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .filters(
                OR(
                    SELECTOR(
                        "dim1",
                        "e",
                        CASCADE(
                            SUBSTRING_FN(1, null),
                            SUBSTRING_FN(0, 1)
                        )
                    ),
                    SELECTOR("dim2", "a")
                )
            )
            .outputColumns("d0")
            .build(),
        new Object[]{""},
        new Object[]{"1"},
        new Object[]{"def"}
    );
  }

  @Test
  public void testSelectDistinctWithStrlenFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo "
        + "WHERE CHARACTER_LENGTH(dim1) = 3 OR CAST(CHARACTER_LENGTH(dim1) AS varchar) = 3",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .filters(
                OR(
                    EXPR_FILTER("(strlen(dim1) == 3)"),
                    EXPR_FILTER("(CAST(CAST(strlen(dim1), 'STRING'), 'LONG') == 3)")
                )
            )
            .outputColumns("d0")
            .build(),
        new Object[]{"abc"},
        new Object[]{"def"}
    );
  }

  @Test
  public void testSelectDistinctWithLimit() throws Exception
  {
    // Should use topN even if approximate topNs are off, because this query is exact.

    testQuery(
        "SELECT DISTINCT dim2 FROM druid.foo LIMIT 10",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim2", "d0"))
            .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
            .threshold(10)
            .outputColumns("d0")
            .build(),
        new Object[]{""},
        new Object[]{"a"},
        new Object[]{"abc"}
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2) LIMIT 10",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim2", "d0"))
            .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
            .threshold(10)
            .outputColumns("d0")
            .build(),
        new Object[]{""},
        new Object[]{"a"},
        new Object[]{"abc"}
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery2() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 5) LIMIT 10",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim2", "d0"))
            .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
            .threshold(5)
            .outputColumns("d0")
            .build(),
        new Object[]{""},
        new Object[]{"a"},
        new Object[]{"abc"}
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery3() throws Exception
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        TypedDummyQuery.DUMMY
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery4() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 DESC LIMIT 5) LIMIT 10",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("dim2", "d0"))
            .metric(new InvertedTopNMetricSpec(
                new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME)
            ))
            .threshold(5)
            .outputColumns("d0")
            .build(),
        new Object[]{""},
        new Object[]{"abc"},
        new Object[]{"a"}
    );
  }

  @Test
  public void testCountDistinct() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(distinct dim2), COUNT(distinct unique_dim1) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                  CARDINALITY("a1", "dim2"),
                  HYPERUNIQUE("a2", "unique_dim1")
              )
              .outputColumns("a0", "a1", "a2")
              .build(),
        new Object[]{6L, 3L, 6L}
    );
  }

  @Test
  public void testCountDistinctOfCaseWhen() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN m1 END),\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN dim1 END),\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN unique_dim1 END)\n"
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  new FilteredAggregatorFactory(
                      CARDINALITY("a0", "m1"),
                      BOUND("m1", "4", null, false, false, null, StringComparators.NUMERIC_NAME)
                  ),
                  new FilteredAggregatorFactory(
                      CARDINALITY("a1", "dim1"),
                      BOUND("m1", "4", null, false, false, null, StringComparators.NUMERIC_NAME)
                  ),
                  new FilteredAggregatorFactory(
                      HYPERUNIQUE("a2", "unique_dim1"),
                      BOUND("m1", "4", null, false, false, null, StringComparators.NUMERIC_NAME)
                  )
              )
              .outputColumns("a0", "a1", "a2")
              .build(),
        new Object[]{3L, 3L, 3L}
    );
  }

  @Test
  public void testExactCountDistinct() throws Exception
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    // we consider explicit null as valid groupBy key
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT COUNT(distinct dim2) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .outputColumns("d0")
                      .build()
              )
              .aggregators(CountAggregatorFactory.of("a0", "d0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
  }

  @Test
  public void testApproxCountDistinctWhenHllDisabled() throws Exception
  {
    // When HLL is disabled, APPROX_COUNT_DISTINCT is still approximate.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT APPROX_COUNT_DISTINCT(dim2) FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(CARDINALITY("a0", "dim2"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testExactCountDistinctWithGroupingAndOtherAggregators() throws Exception
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, SUM(cnt), COUNT(distinct dim1) FROM druid.foo GROUP BY dim2",
        newGroupBy()
            .dataSource(
                newGroupBy()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .dimensions(
                        DefaultDimensionSpec.of("dim2", "d0"),
                        DefaultDimensionSpec.of("dim1", "d1")
                    )
                    .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                    .outputColumns("d0", "d1", "a0")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("d0", "_d0"))
            .aggregators(
                GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                CountAggregatorFactory.of("_a1", "d1")
            )
            .outputColumns("_d0", "_a0", "_a1")
            .build(),
        new Object[]{"", 3L, 3L},
        new Object[]{"a", 2L, 1L},
        new Object[]{"abc", 1L, 1L}
    );
  }

  @Test
  public void testApproxCountDistinct() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  APPROX_COUNT_DISTINCT(dim2),\n" // uppercase
        + "  approx_count_distinct(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
        + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
        + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
        + "  approx_count_distinct(unique_dim1)\n" // on native hyperUnique column
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .virtualColumns(
                  EXPR_VC("a4:v", "concat(substring(dim2, 0, 1),'x')")
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                  CARDINALITY("a1", "dim2"),
                  new FilteredAggregatorFactory(
                      CARDINALITY("a2", "dim2"),
                      NOT(SELECTOR("dim2", ""))
                  ),
                  CARDINALITY("a3", EXTRACT_SUBSTRING("dim2", "dim2", 0, 1)),
                  CARDINALITY("a4", DefaultDimensionSpec.of("a4:v")),
                  HYPERUNIQUE("a5", "unique_dim1")
              )
              .outputColumns("a0", "a1", "a2", "a3", "a4", "a5")
              .build(),
        new Object[]{6L, 3L, 2L, 2L, 2L, 6L}
    );
  }

  @Test
  public void testNestedGroupBy() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "    FLOOR(__time to hour) AS __time,\n"
        + "    dim1,\n"
        + "    COUNT(m2)\n"
        + "FROM (\n"
        + "    SELECT\n"
        + "        MAX(__time) AS __time,\n"
        + "        m2,\n"
        + "        dim1\n"
        + "    FROM druid.foo\n"
        + "    WHERE 1=1\n"
        + "        AND m1 = '5.0'\n"
        + "    GROUP BY m2, dim1\n"
        + ")\n"
        + "GROUP BY FLOOR(__time to hour), dim1",
        newGroupBy()
            .dataSource(
                newGroupBy()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .dimensions(
                        DefaultDimensionSpec.of("m2", "d0"),
                        DefaultDimensionSpec.of("dim1", "d1")
                    )
                    .filters(SELECTOR("m1", "5.0"))
                    .aggregators(GenericMaxAggregatorFactory.ofLong("a0", "__time"))
                    .postAggregators(EXPR_POST_AGG("p0", "timestamp_floor(a0,'PT1H','','UTC')"))
                    .outputColumns("p0", "d1", "d0")
                    .build()
            )
            .dimensions(
                DefaultDimensionSpec.of("p0", "_d0"),
                DefaultDimensionSpec.of("d1", "_d1")
            )
            .aggregators(CountAggregatorFactory.of("a0", "d0"))
            .outputColumns("_d0", "_d1", "a0")
            .build(),
        new Object[]{978393600000L, "def", 1L}
    );
  }

  @Test
  public void testDoubleNestedGroupBy() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(*) FROM (\n"
        + "  SELECT dim2, SUM(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .aggregators(CountAggregatorFactory.of("a0"))
                      .outputColumns("a0")
                      .build()
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                  CountAggregatorFactory.of("_a1")
              )
              .outputColumns("_a0", "_a1")
              .build(),
        new Object[]{6L, 3L}
    );
  }

  @Test
  public void testExplainDoubleNestedGroupBy() throws Exception
  {
    final String explanation =
        "DruidOuterQueryRel(EXPR$0=[SUM($0)], EXPR$1=[COUNT()])\n"
        + "  DruidQueryRel(table=[druid.foo], scanProject=[$2, $3], group=[{1}], cnt=[COUNT()], aggregateProject=[$1])\n";

    testQuery(
        "EXPLAIN PLAN WITH TYPE FOR SELECT SUM(cnt), COUNT(*) FROM (\n"
        + "  SELECT dim2, SUM(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        new Object[]{explanation}
    );
  }

  @Test
  public void testExactCountDistinctUsingSubquery() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_SINGLE_NESTING_ONLY, // Sanity check; this query should work with a single level of nesting.
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .outputColumns("a0")
                      .build()
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                  CountAggregatorFactory.of("_a1")
              )
              .outputColumns("_a0", "_a1")
              .build(),
        new Object[]{6L, 3L}
    );
  }

  @Test
  public void testMinMaxAvgDailyCountWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM ("
        + "  SELECT max(cnt), min(cnt), avg(cnt), TIME_EXTRACT(max(t), 'EPOCH') last_time, count(1) num_days FROM (\n"
        + "      SELECT TIME_FLOOR(__time, 'P1D') AS t, count(1) cnt\n"
        + "      FROM foo\n"
        + "      GROUP BY 1\n"
        + "  )"
        + ") LIMIT 1\n",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  Druids.newTimeseriesQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .granularity(Granularities.DAY)
                        .aggregators(CountAggregatorFactory.of("a0"))
                        .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1D','','UTC')"))
                        .outputColumns("d0", "a0")
                        .build()
              )
              .aggregators(
                  GenericMaxAggregatorFactory.ofLong("_a0", "a0"),
                  GenericMinAggregatorFactory.ofLong("_a1", "a0"),
                  GenericSumAggregatorFactory.ofLong("_a2:sum", "a0"),
                  CountAggregatorFactory.of("_a2:count"),
                  GenericMaxAggregatorFactory.ofLong("_a3", "d0"),
                  CountAggregatorFactory.of("_a4")
              )
              .postAggregators(
                  new ArithmeticPostAggregator(
                      "_a2",
                      "quotient",
                      ImmutableList.of(
                          new FieldAccessPostAggregator(null, "_a2:sum"),
                          new FieldAccessPostAggregator(null, "_a2:count")
                      )
                  ),
                  EXPR_POST_AGG("p0", "timestamp_extract('EPOCH',_a3,'UTC')")
              )
              .outputColumns("_a0", "_a1", "_a2", "p0", "_a4")
              .limit(1)
              .build(),
        new Object[]{1L, 1L, 1L, 978480000L, 6L}
    );
  }

  @Test
  public void testAvgDailyCountDistinct() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  AVG(u)\n"
        + "FROM (SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT(cnt) AS u FROM druid.foo GROUP BY 1)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  Druids.newTimeseriesQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .granularity(Granularities.DAY)
                        .aggregators(CARDINALITY("a0:a", "cnt"))
                        .postAggregators(
                            EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1D','','UTC')"),
                            new HyperUniqueFinalizingPostAggregator("a0", "a0:a", true)
                        )
                        .outputColumns("a0")
                        .build()
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0:sum", "a0"),
                  CountAggregatorFactory.of("_a0:count")
              )
              .postAggregators(
                  new ArithmeticPostAggregator(
                      "_a0",
                      "quotient",
                      ImmutableList.of(
                          new FieldAccessPostAggregator(null, "_a0:sum"),
                          new FieldAccessPostAggregator(null, "_a0:count")
                      )
                  )
              )
              .outputColumns("_a0")
              .build(),
        new Object[]{1L}
    );
  }

  @Test
  public void testTopNFilterJoin() throws Exception
  {
    // Filters on top N values of some dimension by using an inner join.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT t1.dim1, SUM(t1.cnt)\n"
                     + "FROM druid.foo t1\n"
                     + "  INNER JOIN (\n"
                     + "  SELECT\n"
                     + "    SUM(cnt) AS sum_cnt,\n"
                     + "    dim2\n"
                     + "  FROM druid.foo\n"
                     + "  GROUP BY dim2\n"
                     + "  ORDER BY 1 DESC\n"
                     + "  LIMIT 2\n"
                     + ") t2 ON (t1.dim2 = t2.dim2)\n"
                     + "GROUP BY t1.dim1\n"
                     + "ORDER BY 1\n",
        newGroupBy()
            .dataSource(
                newJoin()
                    .dataSource("foo$", new TopNQueryBuilder()
                        .dataSource("foo")
                        .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                        .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .metric(new NumericTopNMetricSpec("a0"))
                        .threshold(2)
                        .outputColumns("d0")
                        .build()
                    )
                    .dataSource("foo", newScan()
                        .dataSource("foo")
                        .columns("cnt", "dim1", "dim2")
                        .streaming()
                    )
                    .element(JoinElement.inner("foo.dim2 = foo$.d0"))
                    .outputColumns("dim1", "cnt")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"", 1L},
        new Object[]{"1", 1L},
        new Object[]{"10.1", 1L},
        new Object[]{"2", 1L},
        new Object[]{"abc", 1L}
    );
    hook.verifyHooked(
        "txkO6OEnCftb3GDSQweE+Q==",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[cnt, dim1, dim2], $hash=true}, TopNQuery{dataSource='foo', dimensionSpec=DefaultDimensionSpec{dimension='dim2', outputName='d0'}, virtualColumns=[], topNMetricSpec=NumericTopNMetricSpec{metric='a0'}, threshold=2, querySegmentSpec=null, filter=null, granularity='AllGranularity', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='cnt', inputType='long'}], postAggregatorSpecs=[], outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='cnt', inputType='long'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
        "StreamQuery{dataSource='foo', columns=[cnt, dim1, dim2], $hash=true}",
        "TopNQuery{dataSource='foo', dimensionSpec=DefaultDimensionSpec{dimension='dim2', outputName='d0'}, virtualColumns=[], topNMetricSpec=NumericTopNMetricSpec{metric='a0'}, threshold=2, querySegmentSpec=null, filter=null, granularity='AllGranularity', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='cnt', inputType='long'}], postAggregatorSpecs=[], outputColumns=[d0]}"
    );
  }

  @Test
  @Ignore // Doesn't work
  public void testTopNFilterJoinWithProjection() throws Exception
  {
    // Filters on top N values of some dimension by using an inner join. Also projects the outer dimension.

    testQuery(
        "SELECT SUBSTRING(t1.dim1, 1, 10), SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  INNER JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY SUBSTRING(t1.dim1, 1, 10)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .filters(IN("dim2", "", "a"))
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"", 1L},
        new Object[]{"1", 1L},
        new Object[]{"10.1", 1L},
        new Object[]{"2", 1L},
        new Object[]{"abc", 1L}
    );
  }

  @Test
  public void testRemovableLeftJoin() throws Exception
  {
    // LEFT JOIN where the right-hand side can be ignored.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT t1.dim1, SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  LEFT JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY t1.dim1\n"
        + "ORDER BY 1\n",
        newGroupBy()
            .dataSource(
                newJoin()
                    .dataSource("foo", newScan()
                        .dataSource("foo")
                        .columns("cnt", "dim1", "dim2")
                        .streaming())
                    .dataSource("foo$", new TopNQueryBuilder()
                        .dataSource("foo")
                        .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                        .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .metric(new NumericTopNMetricSpec("a0"))
                        .outputColumns("d0")
                        .threshold(2)
                        .build()
                    )
                    .element(JoinElement.of(JoinType.LO, "foo.dim2 = foo$.d0"))
                    .outputColumns("dim1", "cnt")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"", 1L},
        new Object[]{"1", 1L},
        new Object[]{"10.1", 1L},
        new Object[]{"2", 1L},
        new Object[]{"abc", 1L},
        new Object[]{"def", 1L}
    );
  }

  @Test
  public void testExactCountDistinctOfSemiJoinResult() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT COUNT(*)\n"
                     + "FROM (\n"
                     + "  SELECT DISTINCT dim2\n"
                     + "  FROM druid.foo\n"
                     + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
                     + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
                     + "  )\n"
                     + ")",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(
                          newJoin()
                              .dataSource("foo", newScan()
                                  .dataSource("foo")
                                  .virtualColumns(EXPR_VC("v0", "substring(dim2, 0, 1)"))
                                  .columns("dim2", "v0")
                                  .streaming()
                              )
                              .dataSource("foo$", newGroupBy()
                                  .dataSource("foo")
                                  .filters(NOT(SELECTOR("dim1", "")))
                                  .dimensions(EXTRACT_SUBSTRING("dim1", "d0", 0, 1))
                                  .outputColumns("d0")
                                  .build()
                              )
                              .element(JoinElement.inner("foo.v0 = foo$.d0"))
                              .outputColumns("dim2")
                              .build()
                      )
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .outputColumns("d0")
                      .build()
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
    hook.verifyHooked(
        "Oxl2e5uAQXk2u9G1I1fiPQ==",
        "TimeseriesQuery{dataSource='GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}, GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[a0]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}, GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}",
        "StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}",
        "GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}"
    );
  }

  @Test
  public void testExactCountDistinctOfJoinResult() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT COUNT(*)\n"
                     + "FROM (\n"
                     + "  SELECT DISTINCT dim2\n"
                     + "  FROM druid.foo\n"
                     + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
                     + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
                     + "  )\n"
                     + ")",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(
                          newJoin()
                              .dataSource("foo", newScan()
                                  .dataSource("foo")
                                  .virtualColumns(EXPR_VC("v0", "substring(dim2, 0, 1)"))
                                  .columns("dim2", "v0")
                                  .streaming()
                              )
                              .dataSource("foo$", newGroupBy()
                                  .dataSource("foo")
                                  .filters(NOT(SELECTOR("dim1", "")))
                                  .dimensions(EXTRACT_SUBSTRING("dim1", "d0", 0, 1))
                                  .outputColumns("d0")
                                  .build()
                              )
                              .element(JoinElement.inner("foo.v0 = foo$.d0"))
                              .outputColumns("dim2")
                              .build())
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .outputColumns("d0")
                      .build()
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
    hook.verifyHooked(
        "Oxl2e5uAQXk2u9G1I1fiPQ==",
        "TimeseriesQuery{dataSource='GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}, GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[a0]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}, GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}",
        "StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}",
        "GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}"
    );
  }

  @Test
  public void testExactCountDistinctOfJoinResult2() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT COUNT(*)\n"
                     + "FROM (\n"
                     + "  SELECT DISTINCT dim2\n"
                     + "  FROM druid.foo as X\n"
                     + "  WHERE EXISTS ( SELECT * FROM druid.foo as Y WHERE X.dim2 = Y.dim2  )\n"
                     + ")",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(
                          newJoin()
                              .dataSource("foo", newScan()
                                  .dataSource("foo")
                                  .filters(NOT(SELECTOR("dim2", "")))
                                  .columns("dim2")
                                  .streaming()
                              )
                              .dataSource("foo$", newGroupBy()
                                  .dataSource("foo")
                                  .filters(NOT(SELECTOR("dim2", "")))
                                  .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                  .outputColumns("d0")
                                  .build()
                              )
                              .element(JoinElement.inner("foo.dim2 = foo$.d0"))
                              .outputColumns("dim2")
                              .build()
                      )
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .outputColumns("d0")
                      .build())
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{2L}
    );
    hook.verifyHooked(
        "mR3G9b/ka2b9ZxciLfC9xg==",
        "TimeseriesQuery{dataSource='GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', filter=!(dim2==NULL), columns=[dim2], $hash=true}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=!(dim2==NULL), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[a0]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', filter=!(dim2==NULL), columns=[dim2], $hash=true}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=!(dim2==NULL), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}",
        "StreamQuery{dataSource='foo', filter=!(dim2==NULL), columns=[dim2], $hash=true}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=!(dim2==NULL), outputColumns=[d0]}"
    );
  }

  @Test
  public void testExplainExactCountDistinctOfSemiJoinResult() throws Exception
  {
    String explanation =
        "DruidOuterQueryRel(EXPR$0=[COUNT()])\n"
        + "  DruidOuterQueryRel(group=[{0}])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0])\n"
        + "      DruidQueryRel(table=[druid.foo], scanProject=[$3, SUBSTRING($3, 1, 1)])\n"
        + "      DruidQueryRel(table=[druid.foo], scanFilter=[<>($2, '')], scanProject=[SUBSTRING($2, 1, 1)], group=[{0}])\n";

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "EXPLAIN PLAN WITH TYPE FOR SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + "  )\n"
        + ")",
        null,
        ImmutableList.of(new Object[]{explanation})
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWherePushDown() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 <> ''",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .filters(NOT(SELECTOR("dim2", "")))
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .outputColumns("a0")
                      .build()
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                  CountAggregatorFactory.of("_a1")
              )
              .outputColumns("_a0", "_a1")
              .build(),
        new Object[]{3L, 2L}
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWherePushDown2() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 IS NOT NULL",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .filters(NOT(SELECTOR("dim2", "")))
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .outputColumns("a0")
                      .build()
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                  CountAggregatorFactory.of("_a1")
              )
              .outputColumns("_a0", "_a1")
              .build(),
        new Object[]{3L, 2L}
    );
  }


  @Test
  public void testExactCountDistinctUsingSubqueryWithWhereToOuterFilter() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 1)"
        + "WHERE cnt > 0",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  new TopNQueryBuilder()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                      .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .threshold(1)
                      .outputColumns("d0", "a0")
                      .build()
              )
              .filters(BOUND("a0", "0", null, true, false, null, StringComparators.NUMERIC_NAME))
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                  CountAggregatorFactory.of("_a1")
              )
              .outputColumns("_a0", "_a1")
              .build(),
        new Object[]{3L, 1L}
    );
  }

  @Test
  public void testCompareExactAndApproximateCountDistinctUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  COUNT(*) AS exact_count,\n"
        + "  COUNT(DISTINCT dim1) AS approx_count,\n"
        + "  (CAST(1 AS FLOAT) - COUNT(DISTINCT dim1) / COUNT(*)) * 100 AS error_pct\n"
        + "FROM (SELECT DISTINCT dim1 FROM druid.foo WHERE dim1 <> '')",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .filters(NOT(SELECTOR("dim1", "")))
                      .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
                      .outputColumns("d0")
                      .build()
              )
              .aggregators(
                  CountAggregatorFactory.of("a0"),
                  CARDINALITY("a1", "d0")
              )
              .postAggregators(EXPR_POST_AGG("p0", "((1F - (a1 / a0)) * 100)"))
              .outputColumns("a0", "a1", "p0")
              .build(),
        new Object[]{5L, 5L, 0.0f}
    );
  }

  @Test
  public void testHistogramUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR)",
        newGroupBy()
            .dataSource(
                newGroupBy()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                    .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                    .postAggregators(EXPR_POST_AGG("p0", "CAST(a0, 'STRING')"))
                    .outputColumns("p0")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("p0", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"3", 1L},
        new Object[]{"2", 1L},
        new Object[]{"1", 1L}
    );
  }

  @Test
  public void testHistogramUsingSubqueryWithSort() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR) ORDER BY CAST(thecnt AS VARCHAR) LIMIT 2",
        newGroupBy()
            .dataSource(
                newGroupBy()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                    .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                    .postAggregators(EXPR_POST_AGG("p0", "CAST(a0, 'STRING')"))
                    .outputColumns("p0")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("p0", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(2, OrderByColumnSpec.asc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"1", 1L},
        new Object[]{"2", 1L}
    );
  }

  @Test
  public void testCountDistinctArithmetic() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(DISTINCT dim2),\n"
        + "  CAST(COUNT(DISTINCT dim2) AS FLOAT),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2) + 3,\n"
        + "  CAST(SUM(cnt) AS FLOAT) / CAST(COUNT(DISTINCT dim2) AS FLOAT) + 3\n"
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                  CARDINALITY("a1", "dim2")
              )
              .postAggregators(
                  EXPR_POST_AGG("p0", "CAST(a1, 'FLOAT')"),
                  EXPR_POST_AGG("p1", "(a0 / a1)"),
                  EXPR_POST_AGG("p2", "((a0 / a1) + 3)"),
                  EXPR_POST_AGG("p3", "((CAST(a0, 'FLOAT') / CAST(a1, 'FLOAT')) + 3)")
              )
              .outputColumns("a0", "a1", "p0", "p1", "p2", "p3")
              .build(),
        new Object[]{6L, 3L, 3.0f, 2L, 5L, 5.0f}
    );
  }

  @Test
  public void testCountDistinctOfSubstring() throws Exception
  {
    testQuery(
        "SELECT COUNT(DISTINCT SUBSTRING(dim1, 1, 1)) FROM druid.foo WHERE dim1 <> ''",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(NOT(SELECTOR("dim1", "")))
              .aggregators(CARDINALITY("a0", EXTRACT_SUBSTRING("dim1", null, 0, 1)))
              .outputColumns("a0")
              .build(),
        new Object[]{4L}
    );
  }

  @Test
  public void testCountDistinctOfTrim() throws Exception
  {
    // Test a couple different syntax variants of TRIM.

    testQuery(
        "SELECT COUNT(DISTINCT TRIM(BOTH ' ' FROM dim1)) FROM druid.foo WHERE TRIM(dim1) <> ''",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(NOT(SELECTOR("dim1", "")))
              .virtualColumns(EXPR_VC("a0:v", "btrim(dim1,' ')"))
              .filters(EXPR_FILTER("(btrim(dim1,' ') != '')"))
              .aggregators(CARDINALITY("a0", DefaultDimensionSpec.of("a0:v")))
              .outputColumns("a0")
              .build(),
        new Object[]{5L}
    );
  }

  @Test
  public void testSillyQuarters() throws Exception
  {
    // Like FLOOR(__time TO QUARTER) but silly.

    testQuery(
        "SELECT CAST((EXTRACT(MONTH FROM __time) - 1 ) / 3 + 1 AS INTEGER) AS quarter, COUNT(*)\n"
        + "FROM foo\n"
        + "GROUP BY CAST((EXTRACT(MONTH FROM __time) - 1 ) / 3 + 1 AS INTEGER)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("d0:v", "(((timestamp_extract('MONTH',__time,'UTC') - 1) / 3) + 1)"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1, 6L}
    );
  }

  @Test
  public void testRegexpExtract() throws Exception
  {
    testQuery(
        "SELECT DISTINCT\n"
        + "  REGEXP_EXTRACT(dim1, '^.'),\n"
        + "  REGEXP_EXTRACT(dim1, '^(.)', 1)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_EXTRACT(dim1, '^(.)', 1) <> 'x'",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .filters(
                NOT(SELECTOR("dim1", "x", REGEX_FN("^(.)", 1)))
            )
            .dimensions(
                EXTRACT_REGEX("dim1", "d0", "^.", 0),
                EXTRACT_REGEX("dim1", "d1", "^(.)", 1)
            )
            .outputColumns("d0", "d1")
            .build(),
        new Object[]{"", ""},
        new Object[]{"1", "1"},
        new Object[]{"2", "2"},
        new Object[]{"a", "a"},
        new Object[]{"d", "d"}
    );
  }

  @Test
  public void testGroupBySortPushDown() throws Exception
  {
    testQuery(
        "SELECT dim2, dim1, SUM(cnt) FROM druid.foo GROUP BY dim2, dim1 ORDER BY dim1 LIMIT 4",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(
                DefaultDimensionSpec.of("dim2", "d0"),
                DefaultDimensionSpec.of("dim1", "d1")
            )
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(4, OrderByColumnSpec.asc("d1")))
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{"a", "", 1L},
        new Object[]{"a", "1", 1L},
        new Object[]{"", "10.1", 1L},
        new Object[]{"", "2", 1L}
    );
  }

  @Test
  public void testGroupByLimitPushDownWithHavingOnLong() throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, SUM(cnt) AS thecnt "
        + "FROM druid.foo "
        + "group by dim1, dim2 "
        + "having SUM(cnt) = 1 "
        + "order by dim2 "
        + "limit 4",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(
                DefaultDimensionSpec.of("dim1", "d0"),
                DefaultDimensionSpec.of("dim2", "d1")
            )
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(4, OrderByColumnSpec.asc("d1")))
            .havingSpec(EXPR_HAVING("(a0 == 1)"))
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{"10.1", "", 1L},
        new Object[]{"2", "", 1L},
        new Object[]{"abc", "", 1L},
        new Object[]{"", "a", 1L}
    );
  }

  @Test
  public void testFilterOnTimeFloor() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) = TIMESTAMP '2000-01-01 00:00:00'\n"
        + "OR FLOOR(__time TO MONTH) = TIMESTAMP '2000-02-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000/P2M")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testFilterOnCurrentTimestampWithIntervalArithmetic() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "  __time >= CURRENT_TIMESTAMP + INTERVAL '01:02' HOUR TO MINUTE\n"
        + "  AND __time < TIMESTAMP '2003-02-02 01:00:00' - INTERVAL '1 1' DAY TO HOUR - INTERVAL '1-1' YEAR TO MONTH",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01T01:02/2002")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{5L}
    );
  }

  @Test
  public void testSelectCurrentTimeAndDateLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_DATE + INTERVAL '1' DAY",
        TypedDummyQuery.DUMMY,
        new Object[]{T("2000-01-01T00Z", LOS_ANGELES), D("1999-12-31"), D("2000-01-01")}
    );
  }

  @Test
  public void testFilterOnCurrentTimestampLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-02T08Z/2002-01-01T08Z")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{4L}
    );
  }

  @Test
  public void testFilterOnCurrentTimestampOnView() throws Exception
  {
    testQuery(
        "SELECT * FROM bview",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-02/2002")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{5L}
    );
  }

  @Test
  public void testFilterOnCurrentTimestampLosAngelesOnView() throws Exception
  {
    // Tests that query context still applies to view SQL; note the result is different from
    // "testFilterOnCurrentTimestampOnView" above.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT * FROM bview",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-02T08Z/2002-01-01T08Z")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{4L}
    );
  }

  @Test
  public void testFilterOnNotTimeFloor() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) <> TIMESTAMP '2001-01-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(
                  new Interval(DateTimes.MIN, DateTimes.of("2001-01-01")),
                  new Interval(DateTimes.of("2001-02-01"), DateTimes.MAX)
              ))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testFilterOnTimeFloorComparison() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(new Interval(DateTimes.MIN, DateTimes.of("2000-02-01"))))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testFilterOnTimeFloorComparisonMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:01'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(new Interval(DateTimes.MIN, DateTimes.of("2000-03-01"))))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  @Ignore // https://issues.apache.org/jira/browse/CALCITE-1601
  public void testFilterOnTimeExtract() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(MONTH FROM __time) = 1",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000/P1M")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  @Ignore // https://issues.apache.org/jira/browse/CALCITE-1601
  public void testFilterOnTimeExtractWithMultipleMonths() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(MONTH FROM __time) IN (2, 3, 5)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-02-01/P2M"), Intervals.of("2000-05-01/P1M")))
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testFilterOnTimeFloorMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:01'",
        TypedDummyQuery.DUMMY,
        new Object[] {0L}
    );
  }

  @Test
  public void testGroupByFloor() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_SUBQUERIES, // Sanity check; this simple query should work with subqueries disabled.
        "SELECT floor(CAST(dim1 AS float)), COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float))",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("d0:v", "floor(CAST(dim1, 'FLOAT'))"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{null, 3L},
        new Object[]{1.0f, 1L},
        new Object[]{2.0f, 1L},
        new Object[]{10.0f, 1L}
    );
  }

  @Test
  public void testGroupByFloorWithOrderBy() throws Exception
  {
    testQuery(
        "SELECT floor(CAST(dim1 AS float)) AS fl, COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float)) ORDER BY fl DESC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("d0:v", "floor(CAST(dim1, 'FLOAT'))"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{10.0f, 1L},
        new Object[]{2.0f, 1L},
        new Object[]{1.0f, 1L},
        new Object[]{null, 3L}
    );
  }

  @Test
  public void testGroupByFloorTimeAndOneOtherDimensionWithOrderBy() throws Exception
  {
    testQuery(
        "SELECT floor(__time TO year), dim2, COUNT(*)"
        + " FROM druid.foo"
        + " GROUP BY floor(__time TO year), dim2"
        + " ORDER BY floor(__time TO year), dim2, COUNT(*) DESC",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .granularity(Granularities.YEAR)
            .dimensions(DefaultDimensionSpec.of("dim2", "d1"))
            .virtualColumns(EXPR_VC("d0:v", "timestamp_floor(__time,'P1Y','','UTC')"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .postAggregators(
                EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1Y','','UTC')")
            )
            .limitSpec(
                LimitSpec.of(
                    OrderByColumnSpec.asc("d0"),
                    OrderByColumnSpec.asc("d1"),
                    OrderByColumnSpec.desc("a0")
                )
            )
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{T("2000"), "", 2L},
        new Object[]{T("2000"), "a", 1L},
        new Object[]{T("2001"), "", 1L},
        new Object[]{T("2001"), "a", 1L},
        new Object[]{T("2001"), "abc", 1L}
    );
  }

  @Test
  public void testGroupByStringLength() throws Exception
  {
    testQuery(
        "SELECT CHARACTER_LENGTH(dim1), COUNT(*) FROM druid.foo GROUP BY CHARACTER_LENGTH(dim1)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("d0:v", "strlen(dim1)"))
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{0, 1L},
        new Object[]{1, 2L},
        new Object[]{3, 2L},
        new Object[]{4, 1L}
    );
  }

//  @Test
//  public void testFilterAndGroupByLookup() throws Exception
//  {
//    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
//        null,
//        "lookyloo",
//        false,
//        null,
//        false,
//        true
//    );
//
//    testQuery(
//        "SELECT LOOKUP(dim1, 'lookyloo'), COUNT(*) FROM foo\n"
//        + "WHERE LOOKUP(dim1, 'lookyloo') <> 'xxx'\n"
//        + "GROUP BY LOOKUP(dim1, 'lookyloo')",
//        ImmutableList.of(
//            newGroupBy()
//                        .dataSource(CalciteTests.DATASOURCE1)
//                        .setInterval(QSS(Filtration.eternity()))
//                        .setGranularity(Granularities.ALL)
//                        .setDimFilter(
//                            NOT(SELECTOR(
//                                "dim1",
//                                "xxx",
//                                extractionFn
//                            ))
//                        )
//                        .setDimensions(
//                            DIMS(
//                                new ExtractionDimensionSpec(
//                                    "dim1",
//                                    "d0",
//                                    extractionFn
//                                )
//                            )
//                        )
//                        .setAggregatorSpecs(
//                            AGGS(
//                                new CountAggregatorFactory("a0")
//                            )
//                        )
//                        .setContext(QUERY_CONTEXT_DEFAULT)
//                        .build()
//        ),
//        ImmutableList.of(
//            new Object[]{"", 5L},
//            new Object[]{"xabc", 1L}
//        )
//    );
//  }

//  @Test
//  public void testCountDistinctOfLookup() throws Exception
//  {
//    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
//        null,
//        "lookyloo",
//        false,
//        null,
//        false,
//        true
//    );
//
//    testQuery(
//        "SELECT COUNT(DISTINCT LOOKUP(dim1, 'lookyloo')) FROM foo",
//        ImmutableList.of(
//            Druids.newTimeseriesQueryBuilder()
//                  .dataSource(CalciteTests.DATASOURCE1)
//                  .intervals(QSS(Filtration.eternity()))
//                  .granularity(Granularities.ALL)
//                  .aggregators(
//                      AGGS(
//                          new CardinalityAggregatorFactory(
//                              "a0",
//                              null,
//                              ImmutableList.<DimensionSpec>of(new ExtractionDimensionSpec("dim1", null, extractionFn)),
//                              null,
//                              false,
//                              true
//                          )
//                      )
//                  )
//                  .context(TIMESERIES_CONTEXT_DEFAULT)
//                  .build()
//        ),
//        ImmutableList.of(
//            new Object[]{2L}
//        )
//    );
//  }

  @Test
  public void testTimeseries() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(Granularities.MONTH)
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{3L, T("2000-01-01")},
        new Object[]{3L, T("2001-01-01")}
    );
  }

  @Test
  public void testFilteredTimeAggregators() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00'\n"
        + "                    AND __time <  TIMESTAMP '2000-02-01 00:00:00'),\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2001-01-01 00:00:00'\n"
        + "                    AND __time <  TIMESTAMP '2001-02-01 00:00:00')\n"
        + "FROM foo\n"
        + "WHERE\n"
        + "  __time >= TIMESTAMP '2000-01-01 00:00:00'\n"
        + "  AND __time < TIMESTAMP '2001-02-01 00:00:00'",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01/2001-02-01")))
              .aggregators(
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                      BOUND(
                          "__time",
                          String.valueOf(T("2000-01-01")),
                          String.valueOf(T("2000-02-01")),
                          false,
                          true,
                          null,
                          StringComparators.NUMERIC_NAME
                      )
                  ),
                  new FilteredAggregatorFactory(
                      GenericSumAggregatorFactory.ofLong("a1", "cnt"),
                      BOUND(
                          "__time",
                          String.valueOf(T("2001-01-01")),
                          String.valueOf(T("2001-02-01")),
                          false,
                          true,
                          null,
                          StringComparators.NUMERIC_NAME
                      )
                  )
              )
              .outputColumns("a0", "a1")
              .build(),
        new Object[]{3L, 3L}
    );
  }

  @Test
  public void testTimeseriesLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT FLOOR(__time TO MONTH) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(1), LOS_ANGELES_DTZ))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(
                  EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','America/Los_Angeles')")
              )
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{1L, T("1999-12-01", LOS_ANGELES)},
        new Object[]{2L, T("2000-01-01", LOS_ANGELES)},
        new Object[]{1L, T("2000-12-01", LOS_ANGELES)},
        new Object[]{2L, T("2001-01-01", LOS_ANGELES)}
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloor() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(Granularities.MONTH)
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{3L, T("2000-01-01")},
        new Object[]{3L, T("2001-01-01")}
    );

    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_CEIL(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(Granularities.MONTH)
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_ceil(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{3L, T("2000-02-01")},
        new Object[]{3L, T("2001-02-01")}
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithTimeShift() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(TIME_SHIFT(__time, 'P1D', -1), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC("d0:v", "timestamp_floor(timestamp_shift(__time,'P1D',-1),'P1M','','UTC')")
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("a0", "d0")
            .build(),
        new Object[]{1L, T("1999-12-01")},
        new Object[]{2L, T("2000-01-01")},
        new Object[]{1L, T("2000-12-01")},
        new Object[]{2L, T("2001-01-01")}
    );

    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_CEIL(TIME_SHIFT(__time, 'P1D', -1), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC("d0:v", "timestamp_ceil(timestamp_shift(__time,'P1D',-1),'P1M','','UTC')")
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("a0", "d0")
            .build(),
        new Object[]{1L, T("2000-01-01")},
        new Object[]{2L, T("2000-02-01")},
        new Object[]{1L, T("2001-01-01")},
        new Object[]{2L, T("2001-02-01")}
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithTimestampAdd() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(
                EXPR_VC("d0:v", "timestamp_floor((__time + -86400000),'P1M','','UTC')")
            )
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("a0", "d0")
            .build(),
        new Object[]{1L, T("1999-12-01")},
        new Object[]{2L, T("2000-01-01")},
        new Object[]{1L, T("2000-12-01")},
        new Object[]{2L, T("2001-01-01")}
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithOrigin() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M', TIMESTAMP '1970-01-01 01:02:03') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(
                  new PeriodGranularity(
                      Period.months(1),
                      DateTimes.of("1970-01-01T01:02:03"),
                      DateTimeZone.UTC
                  )
              )
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M',3723000,'UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{1L, T("1999-12-01T01:02:03")},
        new Object[]{2L, T("2000-01-01T01:02:03")},
        new Object[]{1L, T("2000-12-01T01:02:03")},
        new Object[]{2L, T("2001-01-01T01:02:03")}
    );
  }

  @Test
  public void testTimeseriesLosAngelesUsingTimeFloorConnectionUtc() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M', CAST(NULL AS TIMESTAMP), 'America/Los_Angeles') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(1), LOS_ANGELES_DTZ))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(
                  EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','America/Los_Angeles')")
              )
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{1L, T("1999-12-01T08")},
        new Object[]{2L, T("2000-01-01T08")},
        new Object[]{1L, T("2000-12-01T08")},
        new Object[]{2L, T("2001-01-01T08")}
    );
  }

  @Test
  public void testTimeseriesLosAngelesUsingTimeFloorConnectionLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(1), LOS_ANGELES_DTZ))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(
                  EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','America/Los_Angeles')")
              )
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{1L, T("1999-12-01", LOS_ANGELES)},
        new Object[]{2L, T("2000-01-01", LOS_ANGELES)},
        new Object[]{1L, T("2000-12-01", LOS_ANGELES)},
        new Object[]{2L, T("2001-01-01", LOS_ANGELES)}
    );
  }

  @Test
  @Ignore("removed 'skipEmptyBuckets'")
  public void testTimeseriesDontSkipEmptyBuckets() throws Exception
  {
    // Tests that query context parameters are passed through to the underlying query engine.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO HOUR) AS gran, cnt FROM druid.foo\n"
        + "  WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        NO_PARAM,
        "DruidQueryRel(table=[druid.foo], "
        + "scanFilter=[AND(>=($0, 2000-01-01 00:00:00), <($0, 2000-01-02 00:00:00))], "
        + "scanProject=[FLOOR($0, FLAG(HOUR)), $1], "
        + "group=[{0}], EXPR$0=[SUM($1)], sort0=[$0], dir0=[ASC], sortProject=[$1, $0])\n",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000/2000-01-02")))
              .granularity(PeriodGranularity.of(Period.hours(1)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'PT1H','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .context(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS)
              .build(),
        ImmutableList.<Object[]>builder()
            .add(new Object[]{1L, T("2000-01-01")})
            .add(new Object[]{0L, T("2000-01-01T01")})
            .add(new Object[]{0L, T("2000-01-01T02")})
            .add(new Object[]{0L, T("2000-01-01T03")})
            .add(new Object[]{0L, T("2000-01-01T04")})
            .add(new Object[]{0L, T("2000-01-01T05")})
            .add(new Object[]{0L, T("2000-01-01T06")})
            .add(new Object[]{0L, T("2000-01-01T07")})
            .add(new Object[]{0L, T("2000-01-01T08")})
            .add(new Object[]{0L, T("2000-01-01T09")})
            .add(new Object[]{0L, T("2000-01-01T10")})
            .add(new Object[]{0L, T("2000-01-01T11")})
            .add(new Object[]{0L, T("2000-01-01T12")})
            .add(new Object[]{0L, T("2000-01-01T13")})
            .add(new Object[]{0L, T("2000-01-01T14")})
            .add(new Object[]{0L, T("2000-01-01T15")})
            .add(new Object[]{0L, T("2000-01-01T16")})
            .add(new Object[]{0L, T("2000-01-01T17")})
            .add(new Object[]{0L, T("2000-01-01T18")})
            .add(new Object[]{0L, T("2000-01-01T19")})
            .add(new Object[]{0L, T("2000-01-01T20")})
            .add(new Object[]{0L, T("2000-01-01T21")})
            .add(new Object[]{0L, T("2000-01-01T22")})
            .add(new Object[]{0L, T("2000-01-01T23")})
            .build()
    );
  }

  @Test
  public void testTimeseriesUsingCastAsDate() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), dt FROM (\n"
        + "  SELECT CAST(__time AS DATE) AS dt,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY dt\n"
        + "ORDER BY dt",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.days(1)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1D','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{1L, D("2000-01-01")},
        new Object[]{1L, D("2000-01-02")},
        new Object[]{1L, D("2000-01-03")},
        new Object[]{1L, D("2001-01-01")},
        new Object[]{1L, D("2001-01-02")},
        new Object[]{1L, D("2001-01-03")}
    );
  }

  @Test
  public void testTimeseriesUsingFloorPlusCastAsDate() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), dt FROM (\n"
        + "  SELECT CAST(FLOOR(__time TO QUARTER) AS DATE) AS dt,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY dt\n"
        + "ORDER BY dt",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(3)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P3M','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{3L, D("2000-01-01")},
        new Object[]{3L, D("2001-01-01")}
    );
  }

  @Test
  public void testTimeseriesDescending() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt) FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran DESC",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(Granularities.MONTH)
              .descending(true)
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{T("2001-01-01"), 3L},
        new Object[]{T("2000-01-01"), 3L}
    );
  }

  @Test
  public void testRelayAggregators() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT State, minOf(Profit), maxOf(Profit), firstOf(Profit), lastOf(Profit) FROM sales group by 1 limit 3",
        newGroupBy()
            .dataSource("sales")
            .dimensions(DefaultDimensionSpec.of("State", "d0"))
            .aggregators(
                new RelayAggregatorFactory("a0", "Profit", "double", "MIN"),
                new RelayAggregatorFactory("a1", "Profit", "double", "MAX"),
                new RelayAggregatorFactory("a2", "Profit", "double", "TIME_MIN"),
                new RelayAggregatorFactory("a3", "Profit", "double", "TIME_MAX")
            )
            .limitSpec(LimitSpec.of(3))
            .outputColumns("d0", "a0", "a1", "a2", "a3")
            .build(),
        new Object[]{"Alabama", 0.0, 1459.0, 3.0, 46.0},
        new Object[]{"Arizona", -814.0, 211.0, -321.0, 2.0},
        new Object[]{"Arkansas", 1.0, 843.0, 12.0, 15.0}
    );
  }

  @Test
  public void testComplexWindow() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT T, S FROM ("
        + "  SELECT T, sum(S) over W as S, count(S) over W as C FROM ("
        + "    SELECT TIME_FLOOR(__time, 'P6M') as T, sum(Profit) as S FROM sales GROUP BY TIME_FLOOR(__time, 'P6M')"
        + "  ) WINDOW W as (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
        + ") WHERE C = 2",
        newScan()
            .dataSource(
                newTimeseries()
                    .dataSource("sales")
                    .granularity(PeriodGranularity.of(Period.months(6)))
                    .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "Profit"))
                    .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P6M','','UTC')"))
                    .limitSpec(
                        LimitSpec.of(WindowingSpec.expressions("\"w0$o0\" = $COUNT(a0,-1,0)","\"w0$o1\" = $SUM0(a0,-1,0)"))
                                 .withAlias(ImmutableMap.of("d0", "T" ,"a0", "S"))
                    )
                    .outputColumns("T","S","w0$o0","w0$o1")
                    .build()
            )
            .virtualColumns(EXPR_VC("v0", "case((\"w0$o0\" > 0),\"w0$o1\",'')"))
            .filters(SELECTOR("w0$o0", "2"))
            .columns("T","v0")
            .streaming(),
        new Object[]{T("2011-07-01"), 49520.0},
        new Object[]{T("2012-01-01"), 55981.0},
        new Object[]{T("2012-07-01"), 61606.0},
        new Object[]{T("2013-01-01"), 67674.0},
        new Object[]{T("2013-07-01"), 81721.0},
        new Object[]{T("2014-01-01"), 93134.0},
        new Object[]{T("2014-07-01"), 93500.0}
    );

    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT T, S FROM ("
        + "  SELECT T, sum(S) over W as S, count(S) over W as C FROM ("
        + "    SELECT TIME_CEIL(__time, 'P6M') as T, sum(Profit) as S FROM sales GROUP BY TIME_CEIL(__time, 'P6M')"
        + "  ) WINDOW W as (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
        + ") WHERE C = 2",
        newScan()
            .dataSource(
                newTimeseries()
                    .dataSource("sales")
                    .granularity(PeriodGranularity.of(Period.months(6)))
                    .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "Profit"))
                    .postAggregators(EXPR_POST_AGG("d0", "timestamp_ceil(__time,'P6M','','UTC')"))
                    .limitSpec(
                        LimitSpec.of(WindowingSpec.expressions("\"w0$o0\" = $COUNT(a0,-1,0)","\"w0$o1\" = $SUM0(a0,-1,0)"))
                                 .withAlias(ImmutableMap.of("d0", "T" ,"a0", "S"))
                    )
                    .outputColumns("T","S","w0$o0","w0$o1")
                    .build()
            )
            .virtualColumns(EXPR_VC("v0", "case((\"w0$o0\" > 0),\"w0$o1\",'')"))
            .filters(SELECTOR("w0$o0", "2"))
            .columns("T","v0")
            .streaming(),
        new Object[]{T("2012-01-01"), 49520.0},
        new Object[]{T("2012-07-01"), 55981.0},
        new Object[]{T("2013-01-01"), 61606.0},
        new Object[]{T("2013-07-01"), 67674.0},
        new Object[]{T("2014-01-01"), 81721.0},
        new Object[]{T("2014-07-01"), 93134.0},
        new Object[]{T("2015-01-01"), 93500.0}
    );
  }

  @Test
  public void test3823() throws Exception
  {
    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN 1999 PRECEDING AND CURRENT ROW INCREMENT BY 1000)",
        new Object[]{"2011. 4. 1.", 3753.0},
        new Object[]{"2012. 1. 29.", 7585.0},
        new Object[]{"2012. 3. 26.", 7754.0},
        new Object[]{"2012. 9. 29.", 7643.0},
        new Object[]{"2013. 12. 31.", 7622.0},
        new Object[]{"2013. 7. 22.", 7636.0},
        new Object[]{"2014. 10. 29.", 7464.0},
        new Object[]{"2014. 12. 9.", 7611.0},
        new Object[]{"2014. 6. 4.", 7514.0}
    );
    hook.verifyHooked(
        "5zDk6sHY2e2FWb9aBwPT/w==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-1999,0), \"w0$o1\" = $SUM0(v0,-1999,0)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-1999,0), \"w0$o1\" = $SUM0(v0,-1999,0)]}], alias={v0=$1}}}"
    );

    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN 999 PRECEDING AND 1000 FOLLOWING INCREMENT BY 1000)",
        new Object[]{"2011. 4. 1.", 7585.0},
        new Object[]{"2012. 1. 29.", 7754.0},
        new Object[]{"2012. 3. 26.", 7643.0},
        new Object[]{"2012. 9. 29.", 7622.0},
        new Object[]{"2013. 12. 31.", 7636.0},
        new Object[]{"2013. 7. 22.", 7464.0},
        new Object[]{"2014. 10. 29.", 7611.0},
        new Object[]{"2014. 12. 9.", 7514.0},
        new Object[]{"2014. 6. 4.", 7396.0}
    );
    hook.verifyHooked(
        "8DxoLwZQ8GbJo68TKmrndA==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-999,1000), \"w0$o1\" = $SUM0(v0,-999,1000)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-999,1000), \"w0$o1\" = $SUM0(v0,-999,1000)]}], alias={v0=$1}}}"
    );

    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN CURRENT ROW AND 2000 FOLLOWING INCREMENT BY 1000)",
        new Object[]{"2011. 4. 1.", 7757.0},
        new Object[]{"2012. 1. 29.", 7648.0},
        new Object[]{"2012. 3. 26.", 7623.0},
        new Object[]{"2012. 9. 29.", 7637.0},
        new Object[]{"2013. 12. 31.", 7469.0},
        new Object[]{"2013. 7. 22.", 7614.0},
        new Object[]{"2014. 10. 29.", 7517.0},
        new Object[]{"2014. 12. 9.", 7398.0},
        new Object[]{"2014. 6. 4.", 3772.0}
    );
    hook.verifyHooked(
        "8DxoLwZQ8GbJo68TKmrndA==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,0,2000), \"w0$o1\" = $SUM0(v0,0,2000)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,0,2000), \"w0$o1\" = $SUM0(v0,0,2000)]}], alias={v0=$1}}}"
    );

    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW INCREMENT BY 1000)",
        new Object[]{"2011. 4. 1.", 3753.0},
        new Object[]{"2012. 1. 29.", 7585.0},
        new Object[]{"2012. 3. 26.", 11507.0},
        new Object[]{"2012. 9. 29.", 15228.0},
        new Object[]{"2013. 12. 31.", 19129.0},
        new Object[]{"2013. 7. 22.", 22864.0},
        new Object[]{"2014. 10. 29.", 26593.0},
        new Object[]{"2014. 12. 9.", 30475.0},
        new Object[]{"2014. 6. 4.", 34107.0}
    );
    hook.verifyHooked(
        "v6St6UxuI5CGWZXthVHWKQ==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-2147483647,0), \"w0$o1\" = $SUM0(v0,-2147483647,0)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-2147483647,0), \"w0$o1\" = $SUM0(v0,-2147483647,0)]}], alias={v0=$1}}}"
    );

    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING INCREMENT BY 1000)",
        new Object[]{"2011. 4. 1.", 34121.0},
        new Object[]{"2012. 1. 29.", 30291.0},
        new Object[]{"2012. 3. 26.", 26365.0},
        new Object[]{"2012. 9. 29.", 22644.0},
        new Object[]{"2013. 12. 31.", 18747.0},
        new Object[]{"2013. 7. 22.", 15010.0},
        new Object[]{"2014. 10. 29.", 11281.0},
        new Object[]{"2014. 12. 9.", 7398.0},
        new Object[]{"2014. 6. 4.", 3772.0}
    );
    hook.verifyHooked(
        "sSB/9kkHUQB3IcFT2Y+a9Q==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,0,2147483647), \"w0$o1\" = $SUM0(v0,0,2147483647)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,0,2147483647), \"w0$o1\" = $SUM0(v0,0,2147483647)]}], alias={v0=$1}}}"
    );
  }

  @Test
  public void test3835() throws Exception
  {
    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN 1999 PRECEDING AND CURRENT ROW INCREMENT BY 1000 OFFSET 499)",
        new Object[]{"2011. 11. 5.", 1869.0D},
        new Object[]{"2011. 7. 27.", 5655.0D},
        new Object[]{"2012. 11. 7.", 7719.0D},
        new Object[]{"2012. 7. 10.", 7669.0D},
        new Object[]{"2013. 11. 18.", 7550.0D},
        new Object[]{"2013. 4. 7.", 7653.0D},
        new Object[]{"2013. 9. 28.", 7603.0D},
        new Object[]{"2014. 11. 7.", 7570.0D},
        new Object[]{"2014. 4. 28.", 7526.0D},
        new Object[]{"2014. 9. 10.", 7511.0D}
    );
    hook.verifyHooked(
        "5zDk6sHY2e2FWb9aBwPT/w==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-1999,0), \"w0$o1\" = $SUM0(v0,-1999,0)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,-1999,0), \"w0$o1\" = $SUM0(v0,-1999,0)]}], alias={v0=$1}}}"
    );

    testQuery(
        "  SELECT ShipDate, sum(cast(Quantity as double)) over W as S FROM sales"
        + "  WINDOW W as (ORDER BY ShipDate ROWS BETWEEN CURRENT ROW AND 2000 FOLLOWING INCREMENT BY 1000 OFFSET 0)",
        new Object[]{"2011. 1. 11.", 7586.0D},
        new Object[]{"2011. 4. 1.", 7757.0D},
        new Object[]{"2012. 1. 29.", 7646.0D},
        new Object[]{"2012. 3. 26.", 7625.0D},
        new Object[]{"2012. 9. 29.", 7637.0D},
        new Object[]{"2013. 12. 31.", 7467.0D},
        new Object[]{"2013. 7. 22.", 7613.0D},
        new Object[]{"2014. 10. 29.", 7518.0D},
        new Object[]{"2014. 12. 9.", 7396.0D},
        new Object[]{"2014. 6. 4.", 3764.0D}
    );
    hook.verifyHooked(
        "m2wpvHoGQUCK2DH0TOCg9g==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,0,2000), \"w0$o1\" = $SUM0(v0,0,2000)]}], alias={v0=$1}}}', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='case((\"w0$o0\" > 0),\"w0$o1\",'')', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', columns=[ShipDate, v0], virtualColumns=[ExprVirtualColumn{expression='CAST(Quantity, 'DOUBLE')', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='ShipDate', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $COUNT(v0,0,2000), \"w0$o1\" = $SUM0(v0,0,2000)]}], alias={v0=$1}}}"
    );
  }

  @Test
  public void testNewWindowFunctions() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT T, P, M, S, K FROM ("
        + "  SELECT T, P, median(P) over W as M, skewness(P) over W as S, kurtosis(P) over W as K, count(P) over W as C FROM ("
        + "    SELECT TIME_FLOOR(__time, 'P3M') as T, sum(Profit) as P FROM sales GROUP BY TIME_FLOOR(__time, 'P3M')"
        + "  ) WINDOW W as (ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)"
        + ") WHERE C = 4",
        newScan()
            .dataSource(
                newTimeseries()
                    .dataSource("sales")
                    .granularity(PeriodGranularity.of(Period.months(3)))
                    .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "Profit"))
                    .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P3M','','UTC')"))
                    .limitSpec(
                        LimitSpec.of(WindowingSpec.expressions("\"w0$o0\" = $MEDIAN(a0,-3,0)",
                                                               "\"w0$o1\" = $SKEWNESS(a0,-3,0)",
                                                               "\"w0$o2\" = $KURTOSIS(a0,-3,0)",
                                                               "\"w0$o3\" = $COUNT(a0,-3,0)"))
                                 .withAlias(ImmutableMap.of("d0", "T", "a0", "P"))
                    )
                    .outputColumns("T", "P", "w0$o0", "w0$o1", "w0$o2", "w0$o3")
                    .build()
            )
            .filters(SELECTOR("w0$o3", "4"))
            .columns("T", "P", "w0$o0", "w0$o1", "w0$o2")
            .streaming(),
        new Object[]{T("2011-10-01"), 21726.0, 11997.0, 0.3062170421340279, 1.3226140238360753},
        new Object[]{T("2012-01-01"), 9271.0, 11997.0, 1.5967039869308326, 2.7559559816102874},
        new Object[]{T("2012-04-01"), 12191.0, 12492.0, 1.4952958011263764, 2.7622644591004146},
        new Object[]{T("2012-07-01"), 16848.0, 14519.5, 0.401755397139793, -1.501216843045509},
        new Object[]{T("2012-10-01"), 23296.0, 14519.5, 0.6739338133537466, -0.6383966096697862},
        new Object[]{T("2013-01-01"), 11453.0, 14519.5, 1.045305387409049, -0.1885411396699738},
        new Object[]{T("2013-04-01"), 16077.0, 16462.5, 0.5501507194929038, 1.5488704410249594},
        new Object[]{T("2013-07-01"), 16153.0, 16115.0, 0.755644349565603, 1.8195994163140956},
        new Object[]{T("2013-10-01"), 38038.0, 16115.0, 1.7905670243406804, 3.4292297531370752},
        new Object[]{T("2014-01-01"), 21774.0, 18963.5, 1.6367879443194426, 2.519639080416271},
        new Object[]{T("2014-04-01"), 17169.0, 19471.5, 1.6757180527207403, 2.726705834534635},
        new Object[]{T("2014-07-01"), 26899.0, 24336.5, 0.9252130623297218, 0.648594442385228},
        new Object[]{T("2014-10-01"), 27658.0, 24336.5, -0.6871980063130506, -1.986840042055673}
    );
  }

  @Test
  public void testGroupByExtractYear() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  EXTRACT(YEAR FROM __time) AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM __time)\n"
        + "ORDER BY 1",
        newTimeseries()
            .dataSource(CalciteTests.DATASOURCE1)
            .granularity(Granularities.YEAR)
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .postAggregators(EXPR_POST_AGG("d0", "timestamp_extract('YEAR',__time,'UTC')"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{2000L, 3L},
        new Object[]{2001L, 3L}
    );
  }

  @Test
  public void testGroupByFormatYearAndMonth() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  TIME_FORMAT(__time, 'yyyy MM') AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY TIME_FORMAT(__time, 'yyyy MM')\n"
        + "ORDER BY 1",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .virtualColumns(EXPR_VC("d0:v", "timestamp_format(__time,'yyyy MM','UTC')"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"2000 01", 3L},
        new Object[]{"2001 01", 3L}
    );
  }

  @Test
  public void testGroupByExtractFloorTime() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.years(1)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .postAggregators(
                  EXPR_POST_AGG("d0", "timestamp_extract('YEAR',timestamp_floor(__time,'P1Y','','UTC'),'UTC')")
              )
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{2000L, 3L},
        new Object[]{2001L, 3L}
    );
  }

  @Test
  public void testGroupByExtractFloorTimeLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.years(1), LOS_ANGELES_DTZ))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .postAggregators(
                  EXPR_POST_AGG(
                      "d0",
                      "timestamp_extract('YEAR',timestamp_floor(__time,'P1Y','','America/Los_Angeles'),'America/Los_Angeles')"
                  )
              )
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{1999L, 1L},
        new Object[]{2000L, 3L},
        new Object[]{2001L, 2L}
    );
  }

  @Test
  public void testTimeseriesWithLimitNoTopN() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(1)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(1, OrderByColumnSpec.asc("d0")))
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{T("2000-01-01"), 3L}
    );
  }

  @Test
  public void testTimeseriesWithLimit() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "LIMIT 1",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(1)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(1))
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{T("2000-01-01"), 3L}
    );
  }

  @Test
  public void testTimeseriesWithOrderByAndLimit() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .granularity(PeriodGranularity.of(Period.months(1)))
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(1, OrderByColumnSpec.asc("d0")))
              .outputColumns("d0", "a0")
              .build(),
        new Object[]{T("2000-01-01"), 3L}
    );
  }

  @Test
  public void testGroupByTimeAndOtherDimension() throws Exception
  {
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY dim2, gran\n"
        + "ORDER BY dim2, gran",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(
                DefaultDimensionSpec.of("dim2", "d0"),
                DefaultDimensionSpec.of("d1:v", "d1")
            )
            .virtualColumns(EXPR_VC("d1:v", "timestamp_floor(__time,'P1M','','UTC')"))
            .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0"), OrderByColumnSpec.asc("d1")))
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{"", T("2000-01-01"), 2L},
        new Object[]{"", T("2001-01-01"), 1L},
        new Object[]{"a", T("2000-01-01"), 1L},
        new Object[]{"a", T("2001-01-01"), 1L},
        new Object[]{"abc", T("2001-01-01"), 1L}
    );
  }

  @Test
  public void testUsingSubqueryAsPartOfAndFilter() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_SINGLE_NESTING_ONLY, // Sanity check; this query should work with a single level of nesting.
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
        + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')\n"
        + "AND dim1 <> 'xxx'\n"
        + "group by dim1, dim2 ORDER BY dim2",
        newGroupBy()
            .dataSource(
                newJoin()
                    .dataSource("foo$", newGroupBy()
                        .dataSource("foo")
                        .filters(NOT(SELECTOR("dim1", "", null)))
                        .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .outputColumns("d0")
                        .build()
                    )
                    .dataSource("foo", newScan()
                        .dataSource("foo")
                        .filters(AND(NOT(SELECTOR("dim1", "xxx")), NOT(SELECTOR("dim2", ""))))
                        .columns("dim1", "dim2")
                        .streaming()
                    )
                    .element(JoinElement.inner("foo.dim2 = foo$.d0"))
                    .outputColumns("dim1", "dim2")
                    .build()
            )
            .dimensions(
                DefaultDimensionSpec.of("dim1", "d0"),
                DefaultDimensionSpec.of("dim2", "d1")
            )
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d1")))
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{"def", "abc", 1L}
    );
  }

  @Test
  public void testUsingSubqueryAsPartOfOrFilter() throws Exception
  {
    // This query should ideally be plannable, but it's not. The "OR" means it isn't really
    // a semiJoin and so the filter condition doesn't get converted.

    final String theQuery = "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
                            + "WHERE dim1 = 'xxx' OR dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 LIKE '%bc')\n"
                            + "group by dim1, dim2 ORDER BY dim2";

    testQuery(theQuery, new Object[]{"def", "abc", 1L});
    hook.verifyHooked(
        "6XvAvkhSUYUYfCWXwKTswg==",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim1, dim2], orderingSpecs=[OrderByColumnSpec{dimension='dim2', direction=ascending}]}, TimeseriesQuery{dataSource='foo', filter=dim1 LIKE '%bc', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[a0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=dim1 LIKE '%bc', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='_d0'}, DefaultDimensionSpec{dimension='dim2', outputName='_d1'}], filter=((dim1=='xxx' || !(a0=='0')) && (dim1=='xxx' || !(d1==NULL)) && (dim1=='xxx' || !(dim2==NULL))), aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_d1', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _a0]}",
        "StreamQuery{dataSource='foo', columns=[dim1, dim2], orderingSpecs=[OrderByColumnSpec{dimension='dim2', direction=ascending}]}",
        "TimeseriesQuery{dataSource='foo', filter=dim1 LIKE '%bc', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[a0], $hash=true}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=dim1 LIKE '%bc', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
    );
  }

  @Test
  public void testUsingSubqueryAsFilterForbiddenByConfig() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_SUBQUERIES,
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo "
        + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')"
        + "AND dim1 <> 'xxx'"
        + "group by dim1, dim2 ORDER BY dim2",
        new Object[] {"def", "abc", 1L}
    );
    hook.verifyHooked(
        "QIidKtZoM7E5Sq0rOb0Y+g==",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', filter=(!(dim1=='xxx') && !(dim2==NULL)), columns=[dim1, dim2], $hash=true}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}, DefaultDimensionSpec{dimension='dim2', outputName='d1'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='foo', filter=(!(dim1=='xxx') && !(dim2==NULL)), columns=[dim1, dim2], $hash=true}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0]}"
    );
  }

  @Test
  public void testUsingSubqueryAsFilterOnTwoColumns() throws Exception
  {
    String sql = "SELECT __time, cnt, dim1, dim2 FROM druid.foo "
                 + " WHERE (dim1, dim2) IN ("
                 + "   SELECT dim1, dim2 FROM ("
                 + "     SELECT dim1, dim2, COUNT(*)"
                 + "     FROM druid.foo"
                 + "     WHERE dim2 = 'abc'"
                 + "     GROUP BY dim1, dim2"
                 + "     HAVING COUNT(*) = 1"
                 + "   )"
                 + " )";
    Query query = newJoin()
        .dataSource("foo$", newGroupBy()
            .dataSource("foo")
            .filters(SELECTOR("dim2", "abc"))
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"), DefaultDimensionSpec.of("dim2", "d1"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .havingSpec(new ExpressionHavingSpec("(a0 == 1)"))
            .outputColumns("d0", "d1")
            .build()
        )
        .dataSource("foo", newScan()
            .dataSource("foo")
            .filters(SELECTOR("dim2", "abc"))
            .columns("__time", "cnt", "dim1", "dim2")
            .streaming()
        )
        .element(JoinElement.inner("foo.dim1 = foo$.d0 && foo.dim2 = foo$.d1"))
        .outputColumns("__time", "cnt", "dim1", "dim2")
        .build();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        sql,
        query,
        new Object[]{T("2001-01-02"), 1L, "def", "abc"}
    );
    hook.verifyHooked(
        "1IMY7O5oUuNEoXxpeNFmoQ==",
        "StreamQuery{dataSource='foo', filter=dim2=='abc', columns=[__time, cnt, dim1, dim2], $hash=true}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}, DefaultDimensionSpec{dimension='dim2', outputName='d1'}], filter=dim2=='abc', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, d1]}"
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_SEMIJOIN,
        sql,
        query,
        new Object[]{T("2001-01-02"), 1L, "def", "abc"}
    );
    hook.verifyHooked(
        "msqSetksn9qEFgCLFJKvBQ==",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}, DefaultDimensionSpec{dimension='dim2', outputName='d1'}], filter=dim2=='abc', aggregatorSpecs=[CountAggregatorFactory{name='a0'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, d1]}",
        "StreamQuery{dataSource='foo', filter=(dim2=='abc' && InDimsFilter{dimensions=[dim1, dim2], values=[[def, abc]]}), columns=[__time, cnt, dim1, dim2]}"
    );
  }

  @Test
  public void testUsingSubqueryAsFilterWithInnerSort() throws Exception
  {
    // Regression test for https://github.com/druid-io/druid/issues/4208

    String sql = "SELECT dim1, dim2 FROM druid.foo\n"
                 + " WHERE dim2 IN (\n"
                 + "   SELECT dim2\n"
                 + "   FROM druid.foo\n"
                 + "   GROUP BY dim2\n"
                 + " )\n"
                 + "ORDER BY dim2 DESC";
    Query query = newScan()
        .dataSource(
            newJoin()
                .dataSource("foo$", newGroupBy()
                    .dataSource("foo")
                    .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                    .outputColumns("d0")
                    .build()
                )
                .dataSource("foo", newScan()
                    .dataSource("foo")
                    .columns("dim1", "dim2")
                    .streaming()
                )
                .element(JoinElement.inner("foo.dim2 = foo$.d0"))
                .outputColumns("dim1", "dim2")
                .build()
        )
        .columns("dim1", "dim2")
        .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("dim2", StringComparators.LEXICOGRAPHIC_NAME)))
        .streaming();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        sql,
        query,
        new Object[]{"def", "abc"},
        new Object[]{"", "a"},
        new Object[]{"1", "a"},
        new Object[]{"10.1", ""},
        new Object[]{"2", ""},
        new Object[]{"abc", ""}
    );

    hook.verifyHooked(
        "SWT6gLEjDVgtK+OYwdZKXA==",
        "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim1, dim2], $hash=true}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}], timeColumnName=__time}', columns=[dim1, dim2], orderingSpecs=[OrderByColumnSpec{dimension='dim2', direction=descending}]}",
        "StreamQuery{dataSource='foo', columns=[dim1, dim2], $hash=true}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}"
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_SEMIJOIN,
        sql,
        query,
        new Object[]{"def", "abc"},
        new Object[]{"", "a"},
        new Object[]{"1", "a"},
        new Object[]{"10.1", ""},
        new Object[]{"2", ""},
        new Object[]{"abc", ""}
    );

    hook.verifyHooked(
        "M2HZ2qkdJsb4Z798QIeqIQ==",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], outputColumns=[d0]}",
        "StreamQuery{dataSource='StreamQuery{dataSource='foo', filter=InDimFilter{dimension='dim2', values=[, a, abc]}, columns=[dim1, dim2]}', columns=[dim1, dim2], orderingSpecs=[OrderByColumnSpec{dimension='dim2', direction=descending}]}",
        "StreamQuery{dataSource='foo', filter=InDimFilter{dimension='dim2', values=[, a, abc]}, columns=[dim1, dim2]}"
    );
  }

  @Test
  public void testSemiJoinWithOuterTimeExtract() throws Exception
  {
    String sql = "SELECT dim1, EXTRACT(MONTH FROM __time) FROM druid.foo\n"
                 + " WHERE dim2 IN (\n"
                 + "   SELECT dim2\n"
                 + "   FROM druid.foo\n"
                 + "   WHERE dim1 = 'def'\n"
                 + " ) AND dim1 <> ''";
    Query query = newScan()
        .dataSource(
            newJoin()
                .dataSource("foo$", newGroupBy()
                    .dataSource("foo")
                    .filters(SELECTOR("dim1", "def"))
                    .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                    .outputColumns("d0")
                    .build()
                )
                .dataSource("foo", newScan()
                    .dataSource("foo")
                    .filters(NOT(SELECTOR("dim1", "")))
                    .columns("__time", "dim1", "dim2")
                    .streaming()
                )
                .element(JoinElement.inner("foo.dim2 = foo$.d0"))
                .outputColumns("dim1", "__time")
                .build()
        )
        .virtualColumns(
            EXPR_VC("v0", "timestamp_extract('MONTH',__time,'UTC')")
        )
        .columns("dim1", "v0")
        .streaming();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        sql,
        query,
        new Object[]{"def", 1L}
    );
    hook.verifyHooked(
        "FaqlBcwYtAHq3/Hs9dUzgQ==",
        "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', filter=!(dim1==NULL), columns=[__time, dim1, dim2]}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=dim1=='def', outputColumns=[d0], $hash=true}], timeColumnName=__time}', columns=[dim1, v0], virtualColumns=[ExprVirtualColumn{expression='timestamp_extract('MONTH',__time,'UTC')', outputName='v0'}]}",
        "StreamQuery{dataSource='foo', filter=!(dim1==NULL), columns=[__time, dim1, dim2]}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=dim1=='def', outputColumns=[d0], $hash=true}"
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_SEMIJOIN,
        sql,
        query,
        new Object[]{"def", 1L}
    );
    hook.verifyHooked(
        "6XTjz6NEe1/oGYpGOn3sSg==",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=dim1=='def', outputColumns=[d0]}",
        "StreamQuery{dataSource='StreamQuery{dataSource='foo', filter=(!(dim1==NULL) && dim2=='abc'), columns=[dim1, __time]}', columns=[dim1, v0], virtualColumns=[ExprVirtualColumn{expression='timestamp_extract('MONTH',__time,'UTC')', outputName='v0'}]}",
        "StreamQuery{dataSource='foo', filter=(!(dim1==NULL) && dim2=='abc'), columns=[dim1, __time]}"
    );
  }

  @Test
  public void testSemiJoinWithOuterTimeExtractAggregateWithOrderBy() throws Exception
  {
    String sql = "SELECT COUNT(DISTINCT dim1), EXTRACT(MONTH FROM __time) FROM druid.foo\n"
                 + " WHERE dim2 IN (\n"
                 + "   SELECT dim2\n"
                 + "   FROM druid.foo\n"
                 + "   WHERE dim1 = 'def'\n"
                 + " ) AND dim1 <> ''"
                 + "GROUP BY EXTRACT(MONTH FROM __time)\n"
                 + "ORDER BY EXTRACT(MONTH FROM __time)";
    Query query = newTimeseries()
        .dataSource(
            newJoin()
                .dataSource("foo$", newGroupBy()
                    .dataSource("foo")
                    .filters(SELECTOR("dim1", "def"))
                    .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                    .outputColumns("d0")
                    .build()
                )
                .dataSource("foo", newScan()
                    .dataSource("foo")
                    .filters(NOT(SELECTOR("dim1", "")))
                    .columns("__time", "dim1", "dim2")
                    .streaming()
                )
                .element(JoinElement.inner("foo.dim2 = foo$.d0"))
                .outputColumns("__time", "dim1")
                .build()
        )
        .granularity(Granularities.MONTH)
        .aggregators(CARDINALITY("a0", "dim1"))
        .postAggregators(EXPR_POST_AGG("d0", "timestamp_extract('MONTH',__time,'UTC')"))
        .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
        .outputColumns("a0", "d0")
        .build();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        sql,
        query,
        new Object[]{1L, 1L}
    );
    hook.verifyHooked(
        "l7YgOi2DJhfODPL2NDznjQ==",
        "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', filter=!(dim1==NULL), columns=[__time, dim1, dim2]}, GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=dim1=='def', outputColumns=[d0], $hash=true}], timeColumnName=__time}'PeriodGranularity{period=P1M, timeZone=UTC}, limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, aggregatorSpecs=[CardinalityAggregatorFactory{name='a0', fieldNames=[dim1], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[MathPostAggregator{name='d0', expression='timestamp_extract('MONTH',__time,'UTC')', finalize=true}], outputColumns=[a0, d0]}",
        "StreamQuery{dataSource='foo', filter=!(dim1==NULL), columns=[__time, dim1, dim2]}",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=dim1=='def', outputColumns=[d0], $hash=true}"
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_SEMIJOIN,
        sql,
        query,
        new Object[]{1L, 1L}
    );
    hook.verifyHooked(
        "UDpqOzg60MUyDpvx5Yi2+g==",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], filter=dim1=='def', outputColumns=[d0]}",
        "TimeseriesQuery{dataSource='StreamQuery{dataSource='foo', filter=(!(dim1==NULL) && dim2=='abc'), columns=[__time, dim1]}'PeriodGranularity{period=P1M, timeZone=UTC}, limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, aggregatorSpecs=[CardinalityAggregatorFactory{name='a0', fieldNames=[dim1], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[MathPostAggregator{name='d0', expression='timestamp_extract('MONTH',__time,'UTC')', finalize=true}], outputColumns=[a0, d0]}",
        "StreamQuery{dataSource='foo', filter=(!(dim1==NULL) && dim2=='abc'), columns=[__time, dim1]}"
    );
  }

  @Test
  public void testUsingSubqueryWithExtractionFns() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT dim2, COUNT(*) FROM druid.foo "
                     + "WHERE substring(dim2, 1, 1) IN (SELECT substring(dim1, 1, 1) FROM druid.foo WHERE dim1 <> '')"
                     + "group by dim2",
        newGroupBy()
            .dataSource(
                newJoin()
                    .dataSource("foo$", newGroupBy()
                        .dataSource("foo")
                        .filters(NOT(SELECTOR("dim1", "")))
                        .dimensions(EXTRACT_SUBSTRING("dim1", "d0", 0, 1))
                        .outputColumns("d0")
                        .build()
                    )
                    .dataSource("foo", newScan()
                        .dataSource("foo")
                        .virtualColumns(EXPR_VC("v0", "substring(dim2, 0, 1)"))
                        .columns("dim2", "v0")
                        .streaming()
                    )
                    .element(JoinElement.inner("foo.v0 = foo$.d0"))
                    .outputColumns("dim2")
                    .build()
            )
            .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{"a", 2L},
        new Object[]{"abc", 1L}
    );
    hook.verifyHooked(
        "rh86EemQ7lRMlIpaziKA8w==",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}, GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='dim2', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], outputColumns=[d0, a0]}",
        "StreamQuery{dataSource='foo', columns=[dim2, v0], virtualColumns=[ExprVirtualColumn{expression='substring(dim2, 0, 1)', outputName='v0'}]}",
        "GroupByQuery{dataSource='foo', dimensions=[ExtractionDimensionSpec{dimension='dim1', extractionFn=SubstringDimExtractionFn{index=0, end=1}, outputName='d0'}], filter=!(dim1==NULL), outputColumns=[d0], $hash=true}"
    );
  }

  @Test
  public void testUnicodeFilterAndGroupBy() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  dim2,\n"
        + "  COUNT(*)\n"
        + "FROM foo2\n"
        + "WHERE\n"
        + "  dim1 LIKE U&'\u05D3\\05E8%'\n" // First char is actually in the string; second is a SQL U& escape
        + "  OR dim1 = 'друид'\n"
        + "GROUP BY dim1, dim2",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE2)
            .filters(OR(
                new LikeDimFilter("dim1", "דר%", null, null),
                new SelectorDimFilter("dim1", "друид", null)
            ))
            .dimensions(
                DefaultDimensionSpec.of("dim1", "d0"),
                DefaultDimensionSpec.of("dim2", "d1")
            )
            .aggregators(CountAggregatorFactory.of("a0"))
            .outputColumns("d0", "d1", "a0")
            .build(),
        new Object[]{"друид", "ru", 1L},
        new Object[]{"דרואיד", "he", 1L}
    );
  }

  @Test
  public void testProjectAfterSort() throws Exception
  {
    testQuery(
        "select dim1 from (select dim1, dim2, count(*) cnt from druid.foo group by dim1, dim2) order by cnt",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(
                DefaultDimensionSpec.of("dim1", "d0"),
                DefaultDimensionSpec.of("dim2", "d1")
            )
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{""},
        new Object[]{"1"},
        new Object[]{"10.1"},
        new Object[]{"2"},
        new Object[]{"abc"},
        new Object[]{"def"}
    );
  }

  @Test
  public void testProjectAfterSort2() throws Exception
  {
    testQuery(
        "select s / cnt, dim1, dim2, s from (select dim1, dim2, count(*) cnt, sum(m2) s from druid.foo group by dim1, dim2) order by cnt",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(
                DefaultDimensionSpec.of("dim1", "d0"),
                DefaultDimensionSpec.of("dim2", "d1")
            )
            .aggregators(
                CountAggregatorFactory.of("a0"),
                GenericSumAggregatorFactory.ofDouble("a1", "m2")
            )
            .postAggregators(EXPR_POST_AGG("s0", "(a1 / a0)"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
            .outputColumns("s0", "d0", "d1", "a1", "a0")
            .build(),
        new Object[]{1.0, "", "a", 1.0},
        new Object[]{4.0, "1", "a", 4.0},
        new Object[]{2.0, "10.1", "", 2.0},
        new Object[]{3.0, "2", "", 3.0},
        new Object[]{6.0, "abc", "", 6.0},
        new Object[]{5.0, "def", "abc", 5.0}
    );
  }

  /**
   * In Calcite 1.17, this test worked, but after upgrading to Calcite 1.21, this query fails with:
   *  org.apache.calcite.sql.validate.SqlValidatorException: Column 'dim1' is ambiguous
   */
  @Test
  @Ignore
  public void testProjectAfterSort3() throws Exception
  {
    testQuery(
        "select dim1 from (select dim1, dim1, count(*) cnt from druid.foo group by dim1, dim1 order by cnt)",
        newGroupBy()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimensions(DefaultDimensionSpec.of("dim1", "d0"))
            .aggregators(CountAggregatorFactory.of("a0"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{""},
        new Object[]{"1"},
        new Object[]{"10.1"},
        new Object[]{"2"},
        new Object[]{"abc"},
        new Object[]{"def"}
    );
  }

  @Test
  public void testSortProjectAfterNestedGroupBy() throws Exception
  {
    testQuery(
        "SELECT "
        + "  cnt "
        + "FROM ("
        + "  SELECT "
        + "    __time, "
        + "    dim1, "
        + "    COUNT(m2) AS cnt "
        + "  FROM ("
        + "    SELECT "
        + "        __time, "
        + "        m2, "
        + "        dim1 "
        + "    FROM druid.foo "
        + "    GROUP BY __time, m2, dim1 "
        + "  ) "
        + "  GROUP BY __time, dim1 "
        + ")"
        + "ORDER BY cnt",
        newGroupBy()
            .dataSource(
                newGroupBy()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .dimensions(
                        DefaultDimensionSpec.of("__time", "d0"),
                        DefaultDimensionSpec.of("m2", "d1"),
                        DefaultDimensionSpec.of("dim1", "d2")
                    )
                    .outputColumns("d0", "d2", "d1")
                    .build()
            )
            .dimensions(
                DefaultDimensionSpec.of("d0", "_d0"),
                DefaultDimensionSpec.of("d2", "_d1")
            )
            .aggregators(CountAggregatorFactory.of("a0", "d1"))
            .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
            .outputColumns("a0")
            .build(),
        new Object[]{1L},
        new Object[]{1L},
        new Object[]{1L},
        new Object[]{1L},
        new Object[]{1L},
        new Object[]{1L}
    );
  }

  @Test
  public void testPostAggWithTimeseries() throws Exception
  {
    testQuery(
        "SELECT "
        + "  FLOOR(__time TO YEAR), "
        + "  SUM(m1), "
        + "  SUM(m1) + SUM(m2) "
        + "FROM "
        + "  druid.foo "
        + "WHERE "
        + "  dim2 = 'a' "
        + "GROUP BY FLOOR(__time TO YEAR) "
        + "ORDER BY FLOOR(__time TO YEAR) desc",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .filters(SELECTOR("dim2", "a"))
              .granularity(Granularities.YEAR)
              .aggregators(
                  GenericSumAggregatorFactory.ofDouble("a0", "m1"),
                  GenericSumAggregatorFactory.ofDouble("a1", "m2")
              )
              .postAggregators(
                  EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1Y','','UTC')"),
                  EXPR_POST_AGG("p0", "(a0 + a1)")
                  )
              .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
              .outputColumns("d0", "a0", "p0")
              .descending(true)
              .build(),
        new Object[]{978307200000L, 4.0, 8.0},
        new Object[]{946684800000L, 1.0, 2.0}
    );
  }

  @Test
  @Ignore("Does not allow topN on metric")
  public void testPostAggWithTopN() throws Exception
  {
    testQuery(
        "SELECT AVG(m2), SUM(m1) + SUM(m2) FROM druid.foo WHERE dim2 = 'a' GROUP BY m1 ORDER BY m1 LIMIT 5",
        new TopNQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .dimension(DefaultDimensionSpec.of("m1", "d0"))
            .filters("dim2", "a")
            .aggregators(
                GenericSumAggregatorFactory.ofDouble("a0:sum", "m2"),
                CountAggregatorFactory.of("a0:count"),
                GenericSumAggregatorFactory.ofDouble("a1", "m1"),
                GenericSumAggregatorFactory.ofDouble("a2", "m2")
            )
            .postAggregators(
                new ArithmeticPostAggregator(
                    "a0",
                    "quotient",
                    ImmutableList.of(
                        new FieldAccessPostAggregator(null, "a0:sum"),
                        new FieldAccessPostAggregator(null, "a0:count")
                    )
                ),
                EXPR_POST_AGG("p0", "(\"a1\" + \"a2\")")
            )
            .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC_NAME))
            .threshold(5)
            .outputColumns("d0", "a0")
            .build(),
        new Object[]{1.0, 2.0},
        new Object[]{4.0, 8.0}
    );
  }

  @Test
  public void testConcat() throws Exception
  {
    testQuery(
        "SELECT CONCAT(dim1, '-', dim1, '_', dim1) as dimX FROM foo",
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "concat(dim1,'-',dim1,'_',dim1)"))
            .columns("v0")
            .streaming(),
        new Object[]{"-_"},
        new Object[]{"10.1-10.1_10.1"},
        new Object[]{"2-2_2"},
        new Object[]{"1-1_1"},
        new Object[]{"def-def_def"},
        new Object[]{"abc-abc_abc"}
    );

    testQuery(
        "SELECT CONCAT(dim1, CONCAT(dim2,'x'), m2, 9999, dim1) as dimX FROM foo",
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "concat(dim1,concat(dim2,'x'),m2,9999,dim1)"))
            .columns("v0")
            .streaming(),
        new Object[]{"ax1.09999"},
        new Object[]{"10.1x2.0999910.1"},
        new Object[]{"2x3.099992"},
        new Object[]{"1ax4.099991"},
        new Object[]{"defabcx5.09999def"},
        new Object[]{"abcx6.09999abc"}
    );
  }

  @Test
  public void testTextcat() throws Exception
  {
    testQuery(
        "SELECT textcat(dim1, dim1) as dimX FROM foo",
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "concat(dim1,dim1)"))
            .columns("v0")
            .streaming(),
        new Object[]{""},
        new Object[]{"10.110.1"},
        new Object[]{"22"},
        new Object[]{"11"},
        new Object[]{"defdef"},
        new Object[]{"abcabc"}
    );

    testQuery(
        "SELECT textcat(dim1, CAST(m2 as VARCHAR)) as dimX FROM foo",
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "concat(dim1,CAST(m2, 'STRING'))"))
            .columns("v0")
            .streaming(),
        new Object[]{"1.0"},
        new Object[]{"10.12.0"},
        new Object[]{"23.0"},
        new Object[]{"14.0"},
        new Object[]{"def5.0"},
        new Object[]{"abc6.0"}
    );
  }

  @Test
  public void testRequireTimeConditionPositive() throws Exception
  {
    // simple timeseries
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT __time as t, floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "WHERE t >= '2000-01-01' and t < '2002-01-01'"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(QSS(Intervals.of("2000-01-01/2002-01-01")))
              .granularity(Granularities.MONTH)
              .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
              .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(__time,'P1M','','UTC')"))
              .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
              .outputColumns("a0", "d0")
              .build(),
        new Object[]{3L, T("2000-01-01")},
        new Object[]{3L, T("2001-01-01")}
    );

    final QuerySegmentSpec interval = QSS(Intervals.utc(DateTimes.of("2000-01-01").getMillis(), JodaUtils.MAX_INSTANT));

    // nested groupby only requires time condition for inner most query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo WHERE __time >= '2000-01-01' GROUP BY dim2)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .intervals(interval)
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                      .outputColumns("a0")
                      .build()
              )
              .aggregators(
                  GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                  CountAggregatorFactory.of("_a1")
              )
              .outputColumns("_a0", "_a1")
              .build(),
        new Object[]{6L, 3L}
    );

    // semi-join requires time condition on both left and right query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= '2000-01-01' AND SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo\n"
        + "  WHERE dim1 <> '' AND __time >= '2000-01-01'\n"
        + ")",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newJoin()
                      .dataSource("foo$", newGroupBy()
                          .dataSource("foo")
                          .intervals(interval)
                          .filters(NOT(SELECTOR("dim1", "")))
                          .dimensions(EXTRACT_SUBSTRING("dim1", "d0", 0, 1))
                          .outputColumns("d0")
                          .build()
                      )
                      .dataSource("foo", newScan()
                          .dataSource("foo")
                          .intervals(interval)
                          .virtualColumns(EXPR_VC("v0", "substring(dim2, 0, 1)"))
                          .columns("v0")
                          .streaming()
                      )
                      .element(JoinElement.inner("foo.v0 = foo$.d0"))
                      .outputColumns()
                      .build()
              )
              .aggregators(CountAggregatorFactory.of("a0"))
              .outputColumns("a0")
              .build(),
        new Object[]{3L}
    );
  }

  @Test
  public void testJoin() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT foo.m1 X, foo2.dim2 Y FROM foo join foo2 on foo.__time = foo2.__time",
        newJoin()
            .dataSource("foo", newScan()
                .dataSource("foo")
                .columns("__time", "m1")
                .streaming()
            )
            .dataSource("foo2", newScan()
                .dataSource("foo2")
                .columns("__time", "dim2")
                .streaming()
            )
            .element(JoinElement.inner("foo.__time = foo2.__time"))
            .outputColumns("m1", "dim2")
            .build(),
        new Object[]{1.0, "en"},
        new Object[]{1.0, "ru"},
        new Object[]{1.0, "he"}
    );
    hook.verifyHooked(
        "oITj51xyDRG1gvHv+TEb7w==",
        "StreamQuery{dataSource='foo', columns=[__time, m1]}",
        "StreamQuery{dataSource='foo2', columns=[__time, dim2], $hash=true}"
    );
  }

  @Test
  public void testJoinWithTimes() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT foo.__time,foo2.__time FROM foo join foo2 on foo.__time = foo2.__time limit 3",
        newScan()
            .dataSource(
                newJoin()
                    .dataSource("foo", newScan()
                        .dataSource("foo")
                        .columns("__time")
                        .streaming()
                    )
                    .dataSource("foo2", newScan()
                        .dataSource("foo2")
                        .columns("__time")
                        .streaming()
                    )
                    .element(JoinElement.inner("foo.__time = foo2.__time"))
                    .build()
            )
            .columns("__time", "__time0")
            .limit(3)
            .streaming(),
        new Object[]{946684800000L, 946684800000L},
        new Object[]{946684800000L, 946684800000L},
        new Object[]{946684800000L, 946684800000L}
    );
    hook.verifyHooked(
        "S/Frn7RmZLxzoYF8KyFLpg==",
        "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[__time]}, StreamQuery{dataSource='foo2', columns=[__time], $hash=true}], timeColumnName=__time}', columns=[__time, __time0], limitSpec=LimitSpec{columns=[], limit=3}}",
        "StreamQuery{dataSource='foo', columns=[__time]}",
        "StreamQuery{dataSource='foo2', columns=[__time], $hash=true}"
    );
  }

  @Test
  public void testGbyOnJoin() throws Exception
  {
    testQuery(
        "SELECT sum(foo.m1) X, foo2.dim2 Y FROM foo join foo2 on foo.__time = foo2.__time group by foo2.dim2 limit 3",
        "DruidOuterQueryRel(group=[{0}], X=[SUM($1)], aggregateProject=[$1, $0], fetch=[3])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[3, 1])\n"
        + "    DruidQueryRel(table=[druid.foo], scanProject=[$0, $4])\n"
        + "    DruidQueryRel(table=[druid.foo2], scanProject=[$0, $3])\n",
        new TopNQueryBuilder()
            .dataSource(
                newJoin()
                    .dataSource("foo", newScan()
                        .dataSource("foo")
                        .columns("__time", "m1")
                        .streaming())
                    .dataSource("foo2", newScan()
                        .dataSource("foo2")
                        .columns("__time", "dim2")
                        .streaming()
                    )
                    .element(JoinElement.inner("foo.__time = foo2.__time"))
                    .outputColumns("dim2", "m1")
                    .build()
            )
            .dimension("dim2", "d0")
            .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
            .metric(new DimensionTopNMetricSpec(null, null))
            .outputColumns("a0", "d0")
            .threshold(3)
            .build(),
        new Object[]{1.0, "en"},
        new Object[]{1.0, "he"},
        new Object[]{1.0, "ru"}
    );
    hook.verifyHooked(
        "JaDdXGLCfaCemK06RKStXw==",
        "TopNQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='foo', columns=[__time, m1]}, StreamQuery{dataSource='foo2', columns=[__time, dim2], $hash=true}], timeColumnName=__time}', dimensionSpec=DefaultDimensionSpec{dimension='dim2', outputName='d0'}, virtualColumns=[], topNMetricSpec=DimensionTopNMetricSpec{previousStop='null', ordering=lexicographic}, threshold=3, querySegmentSpec=null, filter=null, granularity='AllGranularity', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='m1', inputType='double'}], postAggregatorSpecs=[], outputColumns=[a0, d0]}",
        "StreamQuery{dataSource='foo', columns=[__time, m1]}",
        "StreamQuery{dataSource='foo2', columns=[__time, dim2], $hash=true}"
    );
  }

  @Test
  public void testExpressionFunctions() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT distinct time_format(bucketStart(__time, 'MONTH'), 'yyyy-MM-dd HH:mm:SS') FROM foo",
        newGroupBy()
            .dataSource("foo")
            .dimensions(DefaultDimensionSpec.of("d0:v", "d0"))
            .virtualColumns(
                EXPR_VC("d0:v", "timestamp_format(BUCKETSTART(__time,'MONTH'),'yyyy-MM-dd HH:mm:SS','UTC')")
            )
            .outputColumns("d0")
            .build(),
        new Object[]{"2000-01-01 00:00:00"},
        new Object[]{"2001-01-01 00:00:00"}
    );
  }

  @Test
  public void testWindowFunction() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT market, quality, index, $sum(index) over (partition by market order by quality desc, index asc) FROM mmapped WHERE __time < '2011-01-15'",
        newScan()
              .dataSource("mmapped")
              .intervals(Intervals.utc(JodaUtils.MIN_INSTANT, DateTimes.of("2011-01-15").getMillis()))
              .columns("index", "market", "quality")
              .limitSpec(
                  LimitSpec.of(new WindowingSpec(
                      Arrays.asList("market"),
                      Arrays.asList(OrderByColumnSpec.desc("quality"), OrderByColumnSpec.asc("index")),
                      "\"w0$o0\" = $SUM(index,-2147483647,0)"
                  ))
              )
              .outputColumns("market", "quality", "index", "w0$o0")
              .streaming(),
        new Object[]{"spot", "travel", 100.0, 100.0},
        new Object[]{"spot", "travel", 106.23693084716797, 206.23693084716797},
        new Object[]{"spot", "travel", 112.25995635986328, 318.49688720703125},
        new Object[]{"spot", "technology", 100.0, 418.49688720703125},
        new Object[]{"spot", "technology", 111.35667419433594, 529.8535614013672},
        new Object[]{"spot", "technology", 114.97421264648438, 644.8277740478516},
        new Object[]{"spot", "premium", 100.0, 744.8277740478516},
        new Object[]{"spot", "premium", 104.61178588867188, 849.4395599365234},
        new Object[]{"spot", "premium", 108.8630142211914, 958.3025741577148},
        new Object[]{"spot", "news", 100.0, 1058.3025741577148},
        new Object[]{"spot", "news", 101.3807601928711, 1159.683334350586},
        new Object[]{"spot", "news", 102.8516845703125, 1262.5350189208984},
        new Object[]{"spot", "mezzanine", 97.90306854248047, 1360.438087463379},
        new Object[]{"spot", "mezzanine", 100.0, 1460.438087463379},
        new Object[]{"spot", "mezzanine", 104.46576690673828, 1564.9038543701172},
        new Object[]{"spot", "health", 94.00043487548828, 1658.9042892456055},
        new Object[]{"spot", "health", 100.0, 1758.9042892456055},
        new Object[]{"spot", "health", 114.94740295410156, 1873.851692199707},
        new Object[]{"spot", "entertainment", 100.0, 1973.851692199707},
        new Object[]{"spot", "entertainment", 109.57347106933594, 2083.425163269043},
        new Object[]{"spot", "entertainment", 110.08729553222656, 2193.5124588012695},
        new Object[]{"spot", "business", 100.0, 2293.5124588012695},
        new Object[]{"spot", "business", 102.67041015625, 2396.1828689575195},
        new Object[]{"spot", "business", 103.62940216064453, 2499.812271118164},
        new Object[]{"spot", "automotive", 86.45037078857422, 2586.2626419067383},
        new Object[]{"spot", "automotive", 94.87471008300781, 2681.137351989746},
        new Object[]{"spot", "automotive", 100.0, 2781.137351989746},
        new Object[]{"total_market", "premium", 1000.0, 1000.0},
        new Object[]{"total_market", "premium", 1073.4765625, 2073.4765625},
        new Object[]{"total_market", "premium", 1689.0128173828125, 3762.4893798828125},
        new Object[]{"total_market", "mezzanine", 1000.0, 4762.4893798828125},
        new Object[]{"total_market", "mezzanine", 1040.945556640625, 5803.4349365234375},
        new Object[]{"total_market", "mezzanine", 1049.1419677734375, 6852.576904296875},
        new Object[]{"upfront", "premium", 800.0, 800.0},
        new Object[]{"upfront", "premium", 869.6437377929688, 1669.6437377929688},
        new Object[]{"upfront", "premium", 1564.61767578125, 3234.2614135742188},
        new Object[]{"upfront", "mezzanine", 800.0, 4034.2614135742188},
        new Object[]{"upfront", "mezzanine", 826.0601806640625, 4860.321594238281},
        new Object[]{"upfront", "mezzanine", 1006.402099609375, 5866.723693847656}
    );
  }

  @Test
  public void testEarliestAggregators() throws Exception
  {
    testQuery(
        "SELECT "
        + "EARLIEST(cnt), EARLIEST(m1), EARLIEST(dim1), "
        + "EARLIEST(cnt + 1), EARLIEST(m1 + 1), EARLIEST(dim1 || CAST(cnt AS VARCHAR)) "
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .virtualColumns(
                  EXPR_VC("a3:v", "(cnt + 1)"),
                  EXPR_VC("a4:v", "(m1 + 1)"),
                  EXPR_VC("a5:v", "concat(dim1,CAST(cnt, 'STRING'))")
              )
              .aggregators(
                  new RelayAggregatorFactory("a0", "cnt", "long", "TIME_MIN"),
                  new RelayAggregatorFactory("a1", "m1", "double", "TIME_MIN"),
                  new RelayAggregatorFactory("a2", "dim1", "dimension.string", "TIME_MIN"),
                  new RelayAggregatorFactory("a3", "a3:v", "long", "TIME_MIN"),
                  new RelayAggregatorFactory("a4", "a4:v", "double", "TIME_MIN"),
                  new RelayAggregatorFactory("a5", "a5:v", "string", "TIME_MIN")
              )
              .outputColumns("a0", "a1", "a2", "a3", "a4", "a5")
              .build(),
        new Object[]{1L, 1.0d, "10.1", 2L, 2.0d, "1"}
    );
  }

  @Test
  public void testLatestAggregators() throws Exception
  {
    testQuery(
        "SELECT "
        + "LATEST(cnt), LATEST(m1), LATEST(dim1), "
        + "LATEST(cnt + 1), LATEST(m1 + 1), LATEST(dim1 || CAST(cnt AS VARCHAR)) "
        + "FROM druid.foo",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .virtualColumns(
                  EXPR_VC("a3:v", "(cnt + 1)"),
                  EXPR_VC("a4:v", "(m1 + 1)"),
                  EXPR_VC("a5:v", "concat(dim1,CAST(cnt, 'STRING'))")
              )
              .aggregators(
                  new RelayAggregatorFactory("a0", "cnt", "long", "TIME_MAX"),
                  new RelayAggregatorFactory("a1", "m1", "double", "TIME_MAX"),
                  new RelayAggregatorFactory("a2", "dim1", "dimension.string", "TIME_MAX"),
                  new RelayAggregatorFactory("a3", "a3:v", "long", "TIME_MAX"),
                  new RelayAggregatorFactory("a4", "a4:v", "double", "TIME_MAX"),
                  new RelayAggregatorFactory("a5", "a5:v", "string", "TIME_MAX")
              )
              .outputColumns("a0", "a1", "a2", "a3", "a4", "a5")
              .build(),
        new Object[]{1L, 6.0d, "abc", 2L, 7.0d, "abc1"}
    );
  }

  @Test
  public void testLatestInSubquery() throws Exception
  {
    RelayAggregatorFactory factory = new RelayAggregatorFactory("a0:a", "m1", "double", "TIME_MAX");
    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, LATEST(m1) AS val FROM foo GROUP BY dim2)",
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  newGroupBy()
                      .dataSource(CalciteTests.DATASOURCE1)
                      .dimensions(DefaultDimensionSpec.of("dim2", "d0"))
                      .aggregators(factory)
                      .postAggregators(AggregatorFactory.asFinalizer("a0", factory))
                      .outputColumns("a0")
                      .build()
              )
              .aggregators(GenericSumAggregatorFactory.ofDouble("_a0", "a0"))
              .outputColumns("_a0")
              .build(),
        new Object[]{15.0d}
    );
  }

  @Test
  public void test3849() throws Exception
  {
    testQuery(
        "SELECT placementish, SUM(index) FROM ("
        + "SELECT __time, placementish, index FROM \"mmapped-split\" limit 10000) GROUP BY placementish",
        new Object[]{"a", 12270.807106018066D},
        new Object[]{"preferred", 503332.5071372986D},
        new Object[]{"b", 10279.01725769043D},
        new Object[]{"e", 12086.472755432129D},
        new Object[]{"h", 10348.278709411621D},
        new Object[]{"m", 217725.42022705078D},
        new Object[]{"n", 10362.731010437012D},
        new Object[]{"p", 210865.67966461182D},
        new Object[]{"t", 19394.10040664673D}
    );
    hook.verifyHooked(
        "t3wFyhirTf2JD4PxhZd1pg==",
        "GroupByQuery{dataSource='StreamQuery{dataSource='mmapped-split', columns=[placementish, index], limitSpec=LimitSpec{columns=[], limit=10000}}', dimensions=[DefaultDimensionSpec{dimension='placementish', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='index', inputType='double'}], outputColumns=[d0, a0]}",
        "StreamQuery{dataSource='mmapped-split', columns=[placementish, index], limitSpec=LimitSpec{columns=[], limit=10000}}"
    );
  }

  @Test
  public void test3880() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "WITH X AS (SELECT placementish, placement, index FROM \"mmapped-split\" LIMIT 10) "
        + "SELECT A.placementish, SUM(A.index) FROM X A INNER JOIN X B ON A.placement = B.placement "
        + "GROUP BY A.placementish "
        + "LIMIT 10",
        new Object[]{"a", 1000.0D},
        new Object[]{"preferred", 19000.0D},
        new Object[]{"b", 1000.0D},
        new Object[]{"e", 1000.0D},
        new Object[]{"h", 1000.0D},
        new Object[]{"m", 11000.0D},
        new Object[]{"n", 1000.0D},
        new Object[]{"p", 1000.0D},
        new Object[]{"t", 2000.0D}
    );
    hook.verifyHooked(
        "0Mkk145g6lpwCR46F4Z4Qg==",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='mmapped-split', columns=[placementish, placement, index], limitSpec=LimitSpec{columns=[], limit=10}, $hash=true}, StreamQuery{dataSource='mmapped-split', columns=[placement], limitSpec=LimitSpec{columns=[], limit=10}}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='placementish', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='index', inputType='double'}], limitSpec=LimitSpec{columns=[], limit=10}, outputColumns=[d0, a0]}",
        "StreamQuery{dataSource='mmapped-split', columns=[placementish, placement, index], limitSpec=LimitSpec{columns=[], limit=10}, $hash=true}",
        "StreamQuery{dataSource='mmapped-split', columns=[placement], limitSpec=LimitSpec{columns=[], limit=10}}"
    );
  }

  @Test
  public void test4013() throws Exception
  {
    testQuery(
        "SELECT datetime(__time), Profit, delta(Profit) over () FROM sales WHERE __time < '2011-01-10'",
        new Object[]{DT("2011-01-04"), 6.0D, 0.0D},
        new Object[]{DT("2011-01-05"), 4.0D, -2.0D},
        new Object[]{DT("2011-01-05"), -5.0D, -9.0D},
        new Object[]{DT("2011-01-05"), -65.0D, -60.0D},
        new Object[]{DT("2011-01-06"), 5.0D, 70.0D},
        new Object[]{DT("2011-01-07"), 746.0D, 741.0D},
        new Object[]{DT("2011-01-07"), 5.0D, -741.0D},
        new Object[]{DT("2011-01-07"), 0.0D, -5.0D},
        new Object[]{DT("2011-01-07"), 274.0D, 274.0D},
        new Object[]{DT("2011-01-07"), 1.0D, -273.0D},
        new Object[]{DT("2011-01-07"), 3.0D, 2.0D},
        new Object[]{DT("2011-01-07"), 9.0D, 6.0D},
        new Object[]{DT("2011-01-07"), 114.0D, 105.0D},
        new Object[]{DT("2011-01-07"), 204.0D, 90.0D},
        new Object[]{DT("2011-01-08"), -54.0D, -258.0D},
        new Object[]{DT("2011-01-08"), -18.0D, 36.0D}
    );
    hook.verifyHooked(
        "C7IlX3FpuNwG7K448h3oAg==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[-146136543-09-08T08:23:32.096Z/2011-01-10T00:00:00.000Z]}, columns=[Profit, __time], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $DELTA(Profit,-2147483647,2147483647)]}]}}', columns=[v0, Profit, w0$o0], virtualColumns=[ExprVirtualColumn{expression='DATETIME(__time)', outputName='v0'}]}",
        "StreamQuery{dataSource='sales', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[-146136543-09-08T08:23:32.096Z/2011-01-10T00:00:00.000Z]}, columns=[Profit, __time], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $DELTA(Profit,-2147483647,2147483647)]}]}}"
    );

    // cannot use expression for over clause.. I don't know why
    testQuery(
        "SELECT T, Profit, Profit - PV as DELTA FROM (" +
        "  SELECT datetime(__time) as T, Profit, $prev(Profit) over () as PV FROM sales WHERE __time < '2011-01-10' )",
        new Object[]{DT("2011-01-04"), 6.0D, null},
        new Object[]{DT("2011-01-05"), 4.0D, -2.0D},
        new Object[]{DT("2011-01-05"), -5.0D, -9.0D},
        new Object[]{DT("2011-01-05"), -65.0D, -60.0D},
        new Object[]{DT("2011-01-06"), 5.0D, 70.0D},
        new Object[]{DT("2011-01-07"), 746.0D, 741.0D},
        new Object[]{DT("2011-01-07"), 5.0D, -741.0D},
        new Object[]{DT("2011-01-07"), 0.0D, -5.0D},
        new Object[]{DT("2011-01-07"), 274.0D, 274.0D},
        new Object[]{DT("2011-01-07"), 1.0D, -273.0D},
        new Object[]{DT("2011-01-07"), 3.0D, 2.0D},
        new Object[]{DT("2011-01-07"), 9.0D, 6.0D},
        new Object[]{DT("2011-01-07"), 114.0D, 105.0D},
        new Object[]{DT("2011-01-07"), 204.0D, 90.0D},
        new Object[]{DT("2011-01-08"), -54.0D, -258.0D},
        new Object[]{DT("2011-01-08"), -18.0D, 36.0D}
    );
    hook.verifyHooked(
        "MFbMpK9WnWSXwnoJY9Mrkg==",
        "StreamQuery{dataSource='StreamQuery{dataSource='sales', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[-146136543-09-08T08:23:32.096Z/2011-01-10T00:00:00.000Z]}, columns=[Profit, __time], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $PREV(Profit,-2147483647,2147483647)]}]}}', columns=[v0, Profit, v1], virtualColumns=[ExprVirtualColumn{expression='DATETIME(__time)', outputName='v0'}, ExprVirtualColumn{expression='(Profit - \"w0$o0\")', outputName='v1'}]}",
        "StreamQuery{dataSource='sales', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[-146136543-09-08T08:23:32.096Z/2011-01-10T00:00:00.000Z]}, columns=[Profit, __time], limitSpec=LimitSpec{columns=[], limit=-1, windowingSpecs=[WindowingSpec{skipSorting=true, expressions=[\"w0$o0\" = $PREV(Profit,-2147483647,2147483647)]}]}}"
    );
  }

  @Test
  public void testX() throws Exception
  {
    testQuery(
        "SELECT textcat(dim1, dim1) as dimX FROM foo WHERE dim1 = ? ",
        Arrays.<Object>asList("abc"),
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "concat(dim1,dim1)"))
            .filters(SELECTOR("dim1", "abc"))
            .columns("v0")
            .streaming(),
        new Object[]{"abcabc"}
    );
  }
}
