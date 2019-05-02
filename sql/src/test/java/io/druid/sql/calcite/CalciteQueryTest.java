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

package io.druid.sql.calcite;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.PeriodGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinType;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContexts;
import io.druid.query.QueryDataSource;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HLLCV1;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.SubstringDimExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.query.select.PagingSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.util.CalciteTestBase;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import io.druid.sql.calcite.view.InProcessViewManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CalciteQueryTest extends CalciteTestBase
{
  private static final Logger log = new Logger(CalciteQueryTest.class);

  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();
  private static final PlannerConfig PLANNER_CONFIG_NO_TOPN = new PlannerConfig()
  {
    @Override
    public int getMaxTopNLimit()
    {
      return 0;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_NO_HLL = new PlannerConfig()
  {
    @Override
    public boolean isUseApproximateCountDistinct()
    {
      return false;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_FALLBACK = new PlannerConfig()
  {
    @Override
    public boolean isUseFallback()
    {
      return true;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_SINGLE_NESTING_ONLY = new PlannerConfig()
  {
    @Override
    public int getMaxQueryCount()
    {
      return 2;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_NO_SUBQUERIES = new PlannerConfig()
  {
    @Override
    public int getMaxQueryCount()
    {
      return 1;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_JOIN_ENABLED = new PlannerConfig()
  {
    @Override
    public boolean isJoinEnabled()
    {
      return true;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_REQUIRE_TIME_CONDITION = new PlannerConfig()
  {
    @Override
    public boolean isRequireTimeCondition()
    {
      return true;
    }
  };

  private static final String LOS_ANGELES = "America/Los_Angeles";

  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      GroupByQuery.SORT_ON_TIME, false
  );

  private static final Map<String, Object> QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      "skipEmptyBuckets", false,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      GroupByQuery.SORT_ON_TIME, false
  );

  private static final Map<String, Object> QUERY_CONTEXT_NO_TOPN = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, "false",
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      GroupByQuery.SORT_ON_TIME, false
  );

  private static final Map<String, Object> QUERY_CONTEXT_LOS_ANGELES = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      GroupByQuery.SORT_ON_TIME, false
  );

  // Matches QUERY_CONTEXT_DEFAULT
  public static final Map<String, Object> TIMESERIES_CONTEXT_DEFAULT = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      "skipEmptyBuckets", true,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      GroupByQuery.SORT_ON_TIME, false
  );

  // Matches QUERY_CONTEXT_LOS_ANGELES
  public static final Map<String, Object> TIMESERIES_CONTEXT_LOS_ANGELES = Maps.newHashMap(TIMESERIES_CONTEXT_DEFAULT);

  static {
    TIMESERIES_CONTEXT_LOS_ANGELES.put(PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES);
  }

  private static final PagingSpec FIRST_PAGING_SPEC = new PagingSpec(null, 1000, true);

  private static final String MASKED = "<<<<<<MASK>>>>>>";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker = null;

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());
  }

  @After
  public void tearDown() throws Exception
  {
    walker = null;
  }

  @Test
  public void testShowTables() throws Exception
  {
    testQuery(
        "SHOW TABLES",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"foo"},
            new Object[]{"foo2"},
            new Object[]{"forbiddenDatasource"},
            new Object[]{"aview"},
            new Object[]{"bview"}
        )
    );
    testQuery(
        "SHOW TABLES LIKE 'foo%'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"foo"},
            new Object[]{"foo2"}
        )
    );
  }

  @Test
  public void testDescTable() throws Exception
  {
    testQuery(
        "DESC foo",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", "NO"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"m1", "DOUBLE", "NO"},
            new Object[]{"m2", "DOUBLE", "NO"},
            new Object[]{"unique_dim1", "OTHER", "YES"}
        )
    );
    testQuery(
        "DESCRIBE foo2 '%1'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"m1", "DOUBLE", "NO"},
            new Object[]{"unique_dim1", "OTHER", "YES"}
        )
    );
  }

  @Test
  public void testSelectConstantExpression() throws Exception
  {
    testQuery(
        "SELECT 1 + 1",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{2}
        )
    );
  }

  @Test
  public void testInsertInto() throws Exception
  {
    testQuery(
        "INSERT INTO DIRECTORY '/__temporary' AS 'CSV' SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{true, 1, MASKED, 7L}
        )
    );
  }

  @Test
  public void testInsertIntoWithHeader() throws Exception
  {
    testQuery(
        "INSERT INTO DIRECTORY '/__temporary' AS 'CSV' WITH ('withHeader' => 'true') SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{true, 1, MASKED, 15L}
        )
    );
  }

  @Test
  public void testSelectConstantExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("dim1", "v0"))
                .virtualColumns(EXPR_VC("v0", "2"))
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{2, ""}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"foo", "xfoo", "foo", " foo", "foo", "foo", "foo", "foo ", "foox", " foo", "xfoo", 6L}
        )
    );
  }

  @Test
  public void testExplainSelectConstantExpression() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN FOR SELECT 1 + 1",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"BindableValues(tuples=[[{ 2 }]])\n"}
        )
    );
  }

  @Test
  public void testInformationSchemaSchemata() throws Exception
  {
    testQuery(
        "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"druid"},
            new Object[]{"INFORMATION_SCHEMA"}
        )
    );
  }

  @Test
  public void testInformationSchemaTables() throws Exception
  {
    testQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"druid", "foo", "TABLE"},
            new Object[]{"druid", "foo2", "TABLE"},
            new Object[]{"druid", "forbiddenDatasource", "TABLE"},
            new Object[]{"druid", "aview", "VIEW"},
            new Object[]{"druid", "bview", "VIEW"},
            new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE"}
        )
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"druid", CalciteTests.DATASOURCE1, "TABLE"},
            new Object[]{"druid", CalciteTests.DATASOURCE2, "TABLE"},
            new Object[]{"druid", CalciteTests.FORBIDDEN_DATASOURCE, "TABLE"},
            new Object[]{"druid", "aview", "VIEW"},
            new Object[]{"druid", "bview", "VIEW"},
            new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE"}
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
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", "NO"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"m1", "DOUBLE", "NO"},
            new Object[]{"m2", "DOUBLE", "NO"},
            new Object[]{"unique_dim1", "OTHER", "YES"}
        )
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
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", "NO"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"m1", "DOUBLE", "NO"},
            new Object[]{"m2", "DOUBLE", "NO"},
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
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"dim1_firstchar", "VARCHAR", "YES"}
        )
    );
  }

  @Test
  public void testExplainInformationSchemaColumns() throws Exception
  {
    final String explanation =
        "BindableProject(COLUMN_NAME=[$3], DATA_TYPE=[$7])\n"
        + "  BindableFilter(condition=[AND(=($1, 'druid'), =($2, 'foo'))])\n"
        + "    BindableTableScan(table=[[INFORMATION_SCHEMA, COLUMNS]])\n";

    testQuery(
        "EXPLAIN PLAN FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation}
        )
    );
  }

  @Test
  public void testSelectStar() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo",
        ImmutableList.<Query>of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1d, 1.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-02"), 1L, "10.1", "", 2d, 2.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-03"), 1L, "2", "", 3d, 3.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-01"), 1L, "1", "a", 4d, 4.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5d, 5.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-03"), 1L, "abc", "", 6d, 6.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectStarOnForbiddenTable() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM druid.forbiddenDatasource",
        ImmutableList.<Query>of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .columns(Arrays.asList("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "forbidden", "abcd", 9999.0d, 0.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testUnqualifiedTableName() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testExplainSelectStar() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN FOR SELECT * FROM druid.foo",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                "DruidQueryRel(query=[{\"queryType\":\"select.stream\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"descending\":false,\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"m1\",\"m2\",\"unique_dim1\"],\"limit\":-1,\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{__time:long, cnt:long, dim1:dimension.string, dim2:dimension.string, m1:double, m2:double, unique_dim1:hyperUnique}])\n"
            }
        )
    );
  }

  @Test
  public void testSelectStarWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
                .limit(2)
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1.0d, 1.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-02"), 1L, "10.1", "", 2.0d, 2.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectWithProjection() throws Exception
  {
    testQuery(
        "SELECT SUBSTRING(dim2, 1, 1) FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("v0"))
                .virtualColumns(EXPR_VC("v0", "substring(\"dim2\", 0, 1)"))
                .limit(2)
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testSelectStarWithLimitTimeDescending() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo ORDER BY __time DESC LIMIT 2",
        ImmutableList.of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .columns(ImmutableList.of("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
                  .descending(true)
                  .orderBy(OrderByColumnSpec.desc("__time"))
                  .limit(2)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2001-01-03"), 1L, "abc", "", 6d, 6d, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5d, 5d, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectStarWithoutLimitTimeAscending() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo ORDER BY __time",
        ImmutableList.of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .columns(ImmutableList.of("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
                  .orderBy(OrderByColumnSpec.asc("__time"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1d, 1.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-02"), 1L, "10.1", "", 2d, 2.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-03"), 1L, "2", "", 3d, 3.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-01"), 1L, "1", "a", 4d, 4.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5d, 5.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-03"), 1L, "abc", "", 6d, 6.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectSingleColumnTwice() throws Exception
  {
    testQuery(
        "SELECT dim2 x, dim2 y FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("dim2", "dim2"))
                .limit(2)
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"a", "a"},
            new Object[]{"", ""}
        )
    );
  }

  @Test
  public void testSelectSingleColumnWithLimitDescending() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2",
        ImmutableList.of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .columns(ImmutableList.of("__time", "dim1"))
                  .descending(true)
                  .orderBy(OrderByColumnSpec.desc("__time"))
                  .limit(2)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .streaming()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testGroupBySingleColumnDescendingNoTopN() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                .setGranularity(Granularities.ALL)
                .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"abc"},
            new Object[]{"2"},
            new Object[]{"10.1"},
            new Object[]{"1"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testSelfJoinWithFallback() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ''",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("dim1"))
                .filters(NOT(SELECTOR("dim1", "", null)))
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming(),
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("dim1", "dim2"))
                .filters(NOT(SELECTOR("dim2", "", null)))
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"abc", "def", "abc"}
        )
    );
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ''",
        ImmutableList.of(
            new Druids.JoinQueryBuilder()
                .dataSource(
                    "foo",
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .columns(Arrays.asList("dim1"))
                        .filters(NOT(SELECTOR("dim1", "", null)))
                        .context(QUERY_CONTEXT_DEFAULT)
                        .streaming()
                )
                .dataSource(
                    "foo$",
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .columns(Arrays.asList("dim1", "dim2"))
                        .filters(NOT(SELECTOR("dim2", "", null)))
                        .context(QUERY_CONTEXT_DEFAULT)
                        .streaming()
                )
                .element(JoinElement.inner("foo.dim1 = foo$.dim2"))
                .asArray(true)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "def", "abc"}
        )
    );
  }

  @Test
  public void testExplainSelfJoinWithFallback() throws Exception
  {
    final String explanation =
        "BindableJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "  DruidQueryRel(query=[{\"queryType\":\"select.stream\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"descending\":false,\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"\"}},\"columns\":[\"dim1\"],\"limit\":-1,\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{dim1:string}])\n"
        + "  DruidQueryRel(query=[{\"queryType\":\"select.stream\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"descending\":false,\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim2\",\"value\":\"\"}},\"columns\":[\"dim1\",\"dim2\"],\"limit\":-1,\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{dim1:string, dim2:string}])\n";

    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "EXPLAIN PLAN FOR\n"
        + "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ''",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation}
        )
    );
  }

  @Test
  public void testGroupByLong() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("cnt", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByOrdinal() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("cnt", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  @Ignore // Disabled since GROUP BY alias can confuse the validator; see DruidConformance::isGroupByAlias
  public void testGroupByAndOrderByAlias() throws Exception
  {
    testQuery(
        "SELECT cnt AS theCnt, COUNT(*) FROM druid.foo GROUP BY theCnt ORDER BY theCnt ASC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("cnt", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.NUMERIC_NAME)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.MONTH)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 3L},
            new Object[]{T("2001-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByAndOrderByOrdinalOfAlias() throws Exception
  {
    testQuery(
        "SELECT cnt as theCnt, COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 1 ASC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("cnt", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByFloat() throws Exception
  {
    testQuery(
        "SELECT m1, COUNT(*) FROM druid.foo GROUP BY m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("m1", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0d, 1L},
            new Object[]{2.0d, 1L},
            new Object[]{3.0d, 1L},
            new Object[]{4.0d, 1L},
            new Object[]{5.0d, 1L},
            new Object[]{6.0d, 1L}
        )
    );
  }

  @Test
  public void testGroupByDouble() throws Exception
  {
    testQuery(
        "SELECT m2, COUNT(*) FROM druid.foo GROUP BY m2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("m2", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0d, 1L},
            new Object[]{2.0d, 1L},
            new Object[]{3.0d, 1L},
            new Object[]{4.0d, 1L},
            new Object[]{5.0d, 1L},
            new Object[]{6.0d, 1L}
        )
    );
  }

  @Test
  public void testFilterOnFloat() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m1 = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .filters(SELECTOR("m1", "1.0", null))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testFilterOnDouble() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m2 = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .filters(SELECTOR("m2", "1.0", null))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testHavingOnGrandTotal() throws Exception
  {
    testQuery(
        "SELECT SUM(m1) AS m1_sum FROM foo HAVING m1_sum = 21",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(CalciteTests.DATASOURCE1)
                  .setGranularity(Granularities.ALL)
                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                  .setHavingSpec(EXPR_HAVING("(\"a0\" == 21)"))
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{21d}
        )
    );
  }

  @Test
  public void testHavingOnDoubleSum() throws Exception
  {
    testQuery(
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING SUM(m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                        .setHavingSpec(EXPR_HAVING("(\"a0\" > 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0d},
            new Object[]{"10.1", 2.0d},
            new Object[]{"2", 3.0d},
            new Object[]{"abc", 6.0d},
            new Object[]{"def", 5.0d}
        )
    );
  }

  @Test
  public void testHavingOnApproximateCountDistinct() throws Exception
  {
    testQuery(
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                        .setAggregatorSpecs(
                            new CardinalityAggregatorFactory(
                                "a0",
                                null,
                                ImmutableList.of(
                                    DefaultDimensionSpec.of("m1", "m1")
                                ),
                                null,
                                null,
                                false,
                                true
                            )
                        )
                        .setHavingSpec(EXPR_HAVING("(\"a0\" > 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  public void testHavingOnExactCountDistinct() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                DefaultDimensionSpec.of("dim2", "d0"),
                                                DefaultDimensionSpec.of("m1", "d1")
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("d0", "_d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setHavingSpec(EXPR_HAVING("(\"a0\" > 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  public void testHavingOnFloatSum() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "SELECT dim1, CAST(SUM(m1) AS FLOAT) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING CAST(SUM(m1) AS FLOAT) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                        .setPostAggregatorSpecs(EXPR_POST_AGG("p0", "CAST(\"a0\", 'FLOAT')"))
                        .setHavingSpec(EXPR_HAVING("(CAST(\"a0\", 'FLOAT') > 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0f},
            new Object[]{"10.1", 2.0f},
            new Object[]{"2", 3.0f},
            new Object[]{"abc", 6.0f},
            new Object[]{"def", 5.0f}
        )
    );

    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING CAST(SUM(m1) AS FLOAT) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                        .setHavingSpec(EXPR_HAVING("(CAST(\"a0\", 'FLOAT') > 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0d},
            new Object[]{"10.1", 2.0d},
            new Object[]{"2", 3.0d},
            new Object[]{"abc", 6.0d},
            new Object[]{"def", 5.0d}
        )
    );
  }

  @Test
  public void testColumnComparison() throws Exception
  {
    // we compares double to string as two strings not like apache druid.. so empty result returned
    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - 1 = dim1 GROUP BY dim1, m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(EXPR_FILTER("((\"m1\" - 1) == \"dim1\")"))
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("m1", "d1")
                        )
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
//            new Object[]{"", 1.0f, 1L},
//            new Object[]{"2", 3.0f, 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(
                            new FilteredAggregatorFactory(
                                CountAggregatorFactory.of("a0"),
                                NOT(SELECTOR("dim2", "a", null))
                            ),
                            CountAggregatorFactory.of("a1")
                        )
                        .setPostAggregatorSpecs(EXPR_POST_AGG("p0", "(\"a0\" / \"a1\")"))
                        .setHavingSpec(EXPR_HAVING("((\"a0\" / \"a1\") == 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setPostAggregatorSpecs(EXPR_POST_AGG("p0", "substring(\"d0\", 1, -1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", ""},
            new Object[]{"1", ""},
            new Object[]{"10.1", "0.1"},
            new Object[]{"2", ""},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setPostAggregatorSpecs(
                            EXPR_POST_AGG("p0", "substring(\"d0\", 1, -1)"),
                            EXPR_POST_AGG("p1", "strlen(\"d0\")")
                        )
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("p1"), OrderByColumnSpec.asc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "0.1"},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"},
            new Object[]{"1", ""},
            new Object[]{"2", ""},
            new Object[]{"", ""}
        )
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
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim1", "d0"))
                .postAggregators(
                    ImmutableList.of(
                        EXPR_POST_AGG("p0", "substring(\"d0\", 1, -1)")
                    )
                )
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", ""},
            new Object[]{"1", ""},
            new Object[]{"10.1", "0.1"},
            new Object[]{"2", ""},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"}
        )
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
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim1", "d0"))
                .postAggregators(ImmutableList.of(
                    EXPR_POST_AGG("p0", "substring(\"d0\", 1, -1)"),
                    EXPR_POST_AGG("p1", "strlen(\"d0\")")
                ))
                .metric(new NumericTopNMetricSpec("p1"))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "0.1"},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"},
            new Object[]{"1", ""},
            new Object[]{"2", ""},
            new Object[]{"", ""}
        )
    );
  }


  @Test
  public void testUnionAll() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            UnionAllQuery.union(
                Arrays.asList(
                    Druids.newTimeseriesQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .aggregators(CountAggregatorFactory.of("a0"))
                          .context(TIMESERIES_CONTEXT_DEFAULT)
                          .build(),
                    Druids.newTimeseriesQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                          .context(TIMESERIES_CONTEXT_DEFAULT)
                          .build(),
                    Druids.newTimeseriesQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .aggregators(CountAggregatorFactory.of("a0"))
                          .context(TIMESERIES_CONTEXT_DEFAULT)
                          .build()
                )
            )
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{6L}, new Object[]{6L})
    );
  }

  @Test
  public void testUnionAllWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM ("
        + "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo"
        + ") LIMIT 2",
        ImmutableList.of(
            UnionAllQuery.union(
                Arrays.asList(
                    Druids.newTimeseriesQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .aggregators(CountAggregatorFactory.of("a0"))
                          .limit(2)
                          .build(),
                    Druids.newTimeseriesQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                          .limit(2)
                          .build(),
                    Druids.newTimeseriesQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .aggregators(CountAggregatorFactory.of("a0"))
                          .limit(2)
                          .build()
                ),
                2
            )
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{6L})
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{21.0})
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                  .postAggregators(EXPR_POST_AGG("p0", "(\"a0\" / 10)"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2.1})
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(CalciteTests.DATASOURCE1)
                  .setGranularity(Granularities.ALL)
                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                  .setHavingSpec(EXPR_HAVING("(\"a0\" == 21)"))
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{21.0})
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            EXPR_VC(
                                "d0:v",
                                "case("
                                + "(CAST(timestamp_extract('DAY',\"__time\",'UTC'), 'DOUBLE') == \"m1\"),"
                                + "'match-m1',"
                                + "(timestamp_extract('DAY',\"__time\",'UTC') == \"cnt\"),"
                                + "'match-cnt',"
                                + "(timestamp_extract('DAY',\"__time\",'UTC') == 0),"
                                + "'zero',"
                                + "'')"
                            )
                        )
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2L},
            new Object[]{"match-cnt", 1L},
            new Object[]{"match-m1", 3L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            EXPR_VC(
                                "d0:v",
                                "case(((\"m1\" > 1) && (\"m1\" < 5) && (\"cnt\" == 1)),'x','')"
                            )
                        )
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"x", 3L}
        )
    );
  }

  @Test
  public void testNullEmptyStringEquality() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE NULLIF(dim2, 'a') IS NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(EXPR_FILTER("case((\"dim2\" == 'a'),1,isNull(\"dim2\"))"))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // Matches everything but "abc"
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testCoalesceColumns() throws Exception
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    testQuery(
        "SELECT COALESCE(dim2, dim1), COUNT(*) FROM druid.foo GROUP BY COALESCE(dim2, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            EXPR_VC(
                                "d0:v",
                                "case(isNotNull(\"dim2\"),\"dim2\",\"dim1\")"
                            )
                        )
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testColumnIsNull() throws Exception
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 IS NULL\n",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(SELECTOR("dim2", null, null))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
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

    for (final String query : queries) {
      assertQueryIsUnplannable(query);
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

    for (final String query : queries) {
      assertQueryIsUnplannable(PLANNER_CONFIG_NO_HLL, query);
    }
  }

  private void assertQueryIsUnplannable(final String sql)
  {
    assertQueryIsUnplannable(PLANNER_CONFIG_DEFAULT, sql);
  }

  private void assertQueryIsUnplannable(final PlannerConfig plannerConfig, final String sql)
  {
    Exception e = null;
    try {
      testQuery(plannerConfig, sql, ImmutableList.of(), ImmutableList.of());
    }
    catch (Exception e1) {
      e = e1;
    }

    if (e == null) {
      // now makes queries lazily
      Assert.fail(sql);
    }
  }

  @Test
  public void testSelectStarWithDimFilter() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo WHERE dim1 > 'd' OR dim2 = 'a'",
        ImmutableList.<Query>of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1"))
                .filters(
                    OR(
                        BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC_NAME),
                        SELECTOR("dim2", "a", null)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1.0d, 1.0d, HLLCV1.class.getName()},
            new Object[]{T("2001-01-01"), 1L, "1", "a", 4.0d, 4.0d, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5.0d, 5.0d, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testGroupByNothingWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{0L, null}
        )
    );
  }

  @Test
  public void testGroupByOneColumnWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0 GROUP BY dim1",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothing() throws Exception
  {
    // This query should actually return [0, null] rather than an empty result set, but it doesn't.
    // This test just "documents" the current behavior.

    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(SELECTOR("dim1", "foobar", null))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      CountAggregatorFactory.of("a0"),
                      GenericMaxAggregatorFactory.ofLong("a1", "cnt")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothingWithGroupByLiteral() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar' GROUP BY 'dummy'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(SELECTOR("dim1", "foobar", null))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      CountAggregatorFactory.of("a0"),
                      GenericMaxAggregatorFactory.ofLong("a1", "cnt")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testCountNonNullColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(cnt) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountNullableColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(dim2) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a0"),
                          NOT(SELECTOR("dim2", "", null))
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountNullableExpression() throws Exception
  {
    testQuery(
        "SELECT COUNT(CASE WHEN dim2 = 'abc' THEN 'yes' WHEN dim2 = 'def' THEN 'yes' END) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a0"),
                          EXPR_FILTER(
                              "isNotNull(case((\"dim2\" == 'abc'),'yes',(\"dim2\" == 'def'),'yes',''))"
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStar() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarOnCommonTableExpression() throws Exception
  {
    testQuery(
        "WITH beep (dim1_firstchar) AS (SELECT SUBSTRING(dim1, 1, 1) FROM foo WHERE dim2 = 'a')\n"
        + "SELECT COUNT(*) FROM beep WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(AND(
                      SELECTOR("dim2", "a", null),
                      NOT(SELECTOR("dim1", "z", new SubstringDimExtractionFn(0, 1)))
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarOnView() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.aview WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(AND(
                      SELECTOR("dim2", "a", null),
                      NOT(SELECTOR("dim1", "z", new SubstringDimExtractionFn(0, 1)))
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testExplainCountStarOnView() throws Exception
  {
    final String explanation =
        "DruidQueryRel(query=["
        + "{\"queryType\":\"timeseries\","
        + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
        + "\"descending\":false,"
        + "\"filter\":{\"type\":\"and\",\"fields\":[{\"type\":\"selector\",\"dimension\":\"dim2\",\"value\":\"a\"},{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"z\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}}]},"
        + "\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{a0:long}])\n";

    testQuery(
        "EXPLAIN PLAN FOR SELECT COUNT(*) FROM aview WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(
                      OR(
                          BOUND("cnt", "3", null, false, false, null, StringComparators.NUMERIC_NAME),
                          SELECTOR("cnt", "1", null)
                      )
                  )
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnFloatLiterals() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt > 1.1 and cnt < 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(
                      BOUND("cnt", "1.1", "100000001.0", true, true, null, StringComparators.NUMERIC_NAME)
                  )
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(
                      SELECTOR("cnt", "1.0", null)
                  )
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(
                      SELECTOR("cnt", "100000001.0", null)
                  )
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0 or cnt = 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(IN("cnt", "1.0", "100000001.0"))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnTwoPoints() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1 OR cnt = 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(IN("cnt", "1", "2"))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testFilterOnStringAsNumber() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE "
        + "dim1 = 10 OR "
        + "(floor(CAST(dim1 AS float)) = 10.00 and CAST(dim1 AS float) > 9 and CAST(dim1 AS float) <= 10.5)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setDimFilter(
                            OR(
                                SELECTOR("dim1", "10", null),
                                AND(
                                    EXPR_FILTER("(CAST(floor(CAST(\"dim1\", 'FLOAT')), 'DOUBLE') == 10.00)"),
                                    BOUND("dim1", "9", "10.5", true, false, null, StringComparators.NUMERIC_NAME)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testSimpleAggregations() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), COUNT(cnt), COUNT(dim1), AVG(cnt), SUM(cnt), SUM(cnt) + MIN(cnt) + MAX(cnt) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      CountAggregatorFactory.of("a0"),
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a1"),
                          NOT(SELECTOR("dim1", "", null))
                      ),
                      GenericSumAggregatorFactory.ofLong("a2:sum", "cnt"),
                      CountAggregatorFactory.of("a2:count"),
                      GenericSumAggregatorFactory.ofLong("a3", "cnt"),
                      GenericMinAggregatorFactory.ofLong("a4", "cnt"),
                      GenericMaxAggregatorFactory.ofLong("a5", "cnt")
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new ArithmeticPostAggregator(
                              "a2",
                              "quotient",
                              ImmutableList.of(
                                  new FieldAccessPostAggregator(null, "a2:sum"),
                                  new FieldAccessPostAggregator(null, "a2:count")
                              )
                          ),
                          EXPR_POST_AGG("p0", "((\"a3\" + \"a4\") + \"a5\")")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 6L, 5L, 1L, 6L, 8L}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationDefault() throws Exception
  {
    // By default this query uses topN.

    testQuery(
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim1", "d0"))
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("p0")))
                .aggregators(
                    GenericMinAggregatorFactory.ofDouble("a0", "m1"),
                    GenericMaxAggregatorFactory.ofDouble("a1", "m1")
                )
                .postAggregators(EXPR_POST_AGG("p0", "(\"a0\" + \"a1\")"))
                .threshold(3)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0d},
            new Object[]{"10.1", 4.0d},
            new Object[]{"2", 6.0d}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNConfig() throws Exception
  {
    // Use PlannerConfig to disable topN, so this query becomes a groupBy.

    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(
                            GenericMinAggregatorFactory.ofDouble("a0", "m1"),
                            GenericMaxAggregatorFactory.ofDouble("a1", "m1")
                        )
                        .setPostAggregatorSpecs(EXPR_POST_AGG("p0", "(\"a0\" + \"a1\")"))
                        .setLimitSpec(LimitSpec.of(3, OrderByColumnSpec.asc("p0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0d},
            new Object[]{"10.1", 4.0d},
            new Object[]{"2", 6.0d}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(
                            GenericMinAggregatorFactory.ofDouble("a0", "m1"),
                            GenericMaxAggregatorFactory.ofDouble("a1", "m1")
                        )
                        .setPostAggregatorSpecs(EXPR_POST_AGG("p0", "(\"a0\" + \"a1\")"))
                        .setLimitSpec(LimitSpec.of(3, OrderByColumnSpec.asc("p0")))
                        .setContext(QUERY_CONTEXT_NO_TOPN)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0d},
            new Object[]{"10.1", 4.0d},
            new Object[]{"2", 6.0d}
        )
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
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new FilteredAggregatorFactory(
                          GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                          SELECTOR("dim1", "abc", null)
                      ),
                      new FilteredAggregatorFactory(
                          GenericSumAggregatorFactory.ofLong("a1", "cnt"),
                          NOT(SELECTOR("dim1", "abc", null))
                      ),
                      new FilteredAggregatorFactory(
                          GenericSumAggregatorFactory.ofLong("a2", "cnt"),
                          SELECTOR("dim1", "a", new SubstringDimExtractionFn(0, 1))
                      ),
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a3"),
                          AND(
                              NOT(SELECTOR("dim2", "", null)),
                              NOT(SELECTOR("dim1", "1", null))
                          )
                      ),
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a4"),
                          NOT(SELECTOR("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a5"),
                          NOT(SELECTOR("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          GenericSumAggregatorFactory.ofLong("a6", "cnt"),
                          SELECTOR("dim2", "a", null)
                      ),
                      new FilteredAggregatorFactory(
                          GenericSumAggregatorFactory.ofLong("a7", "cnt"),
                          AND(
                              SELECTOR("dim2", "a", null),
                              NOT(SELECTOR("dim1", "1", null))
                          )
                      ),
                      new FilteredAggregatorFactory(
                          GenericSumAggregatorFactory.ofLong("a8", "cnt"),
                          NOT(SELECTOR("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          GenericMaxAggregatorFactory.ofLong("a9", "cnt"),
                          NOT(SELECTOR("dim1", "1", null))
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 5L, 1L, 2L, 5L, 5L, 2L, 1L, 5L, 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("cnt", "d0"))
                        .setAggregatorSpecs(
                            new FilteredAggregatorFactory(
                                CountAggregatorFactory.of("a0"),
                                NOT(SELECTOR("dim1", "1", null))
                            ),
                            GenericSumAggregatorFactory.ofLong("a1", "cnt")
                        )
                        .setPostAggregatorSpecs(EXPR_POST_AGG("p0", "(\"a0\" + \"a1\")"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 11L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 5L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.expr("a0", "(\"cnt\" * 3)", ValueDesc.LONG),
                      GenericSumAggregatorFactory.ofLong("a1", "cnt"),
                      GenericSumAggregatorFactory.ofDouble("a2", "m1"),
                      GenericSumAggregatorFactory.expr("a3", "strlen(CAST((\"cnt\" * 10), 'STRING'))", ValueDesc.LONG),
                      GenericMaxAggregatorFactory.expr("a4", "(strlen(\"dim2\") + log(\"m1\"))", ValueDesc.DOUBLE)
                  )
                  .postAggregators(
                      EXPR_POST_AGG("p0", "log((\"a1\" + \"a2\"))"),
                      EXPR_POST_AGG("p1", "(\"a1\" % 4)")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{18L, 3.295836866004329, 2, 12L, 3f + (Math.log(5.0))}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            EXPR_VC("d0:v", "(floor((\"m1\" / 2)) * 2)")
                        )
                        .setDimFilter(EXPR_FILTER("((floor((\"m1\" / 2)) * 2) > -1)"))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{6.0d, 1L},
            new Object[]{4.0d, 2L},
            new Object[]{2.0d, 2L},
            new Object[]{0.0d, 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(EXPR_VC("d0:v", "((CAST(\"m1\", 'LONG') / 2) * 2)"))
                        .setDimFilter(EXPR_FILTER("(((CAST(\"m1\", 'LONG') / 2) * 2) > -1)"))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 1L},
            new Object[]{4L, 2L},
            new Object[]{2L, 2L},
            new Object[]{0L, 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(EXPR_VC("d0:v", "(floor((CAST(\"dim1\", 'FLOAT') / 2)) * 2)"))
                        .setDimFilter(EXPR_FILTER("((floor((CAST(\"dim1\", 'FLOAT') / 2)) * 2) > -1)"))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{0.0f, 4L}
        )
    );
  }

  @Test
  public void testInFilter() throws Exception
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setDimFilter(IN("dim1", "abc", "def", "ghi"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setDimFilter(IN("dim1", elements, null))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testCountStarWithDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(
                      AND(
                          SELECTOR("dim2", "a", null),
                          OR(
                              BOUND("dim1", "a", null, true, false, null, StringComparators.LEXICOGRAPHIC_NAME),
                              NOT(SELECTOR("dim1", null, null))
                          )
                      )
                  )
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithNotOfDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and not (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.of(),
        ImmutableList.of(new Object[]{0L})
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOnMetric() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE 2.5 < m1 AND m1 < 3.5",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(BOUND("m1", "2.5", "3.5", true, true, null, StringComparators.NUMERIC_NAME))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOr() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') OR dim1 = 'ab'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(BOUND("dim1", "a", "b", false, true, null, StringComparators.LEXICOGRAPHIC_NAME))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyAnd() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') and dim1 = 'abc'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(SELECTOR("dim1", "abc", null))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithFilterOnCastedString() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE CAST(dim1 AS bigint) = 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .filters(NUMERIC_SELECTOR("dim1", "2", null))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterUsingStringLiterals() throws Exception
  {
    // Strings are implicitly cast to timestamps.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= '2000-01-01 00:00:00' AND __time < '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000/2001"), Intervals.of("2010/2011")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeMillisecondFilters() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time = TIMESTAMP '2000-01-01 00:00:00.111'\n"
        + "OR (__time >= TIMESTAMP '2000-01-01 00:00:00.888' AND __time < TIMESTAMP '2000-01-02 00:00:00.222')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      QSS(
                          Intervals.of("2000-01-01T00:00:00.111/2000-01-01T00:00:00.112"),
                          Intervals.of("2000-01-01T00:00:00.888/2000-01-02T00:00:00.222")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }


  @Test
  public void testCountStarWithSinglePointInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE __time = TIMESTAMP '2000-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01/2000-01-01T00:00:00.001")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTwoPointsInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "__time = TIMESTAMP '2000-01-01 00:00:00' OR __time = TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1' DAY",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      QSS(
                          Intervals.of("2000-01-01/2000-01-01T00:00:00.001"),
                          Intervals.of("2000-01-02/2000-01-02T00:00:00.001")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000/2001"), Intervals.of("2002-05-01/2003-05-01")))
                  .granularity(Granularities.ALL)
                  .filters(
                      AND(
                          SELECTOR("dim2", "a", null),
                          OR(
                              TIME_BOUND("2000/2001"),
                              AND(
                                  SELECTOR("dim1", "abc", null),
                                  TIME_BOUND("2002-05-01/2003-05-01")
                              )
                          )
                      )
                  )
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(
                      OR(
                          NOT(SELECTOR("dim2", "a", null)),
                          AND(
                              NOT(TIME_BOUND("2000/2001")),
                              NOT(AND(
                                  SELECTOR("dim1", "abc", null),
                                  TIME_BOUND("2002-05-01/2003-05-01")
                              ))
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      QSS(
                          new Interval(DateTimes.MIN, DateTimes.of("2000")),
                          Intervals.of("2001/2003"),
                          new Interval(DateTimes.of("2004"), DateTimes.MAX)
                      )
                  )
                  .filters(NOT(SELECTOR("dim1", "xxx", null)))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeAndDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "and __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01/2001-01-01")))
                  .filters(NOT(SELECTOR("dim2", "a", null)))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeOrDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "or __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(
                      OR(
                          NOT(SELECTOR("dim2", "a", null)),
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
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpoch() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM TIMESTAMP '1970-01-02 00:00:00') * 1000",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpochFromDate() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM DATE '1970-01-01') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM DATE '1970-01-02') * 1000",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingTimestampToMillis() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-01 00:00:00') "
        + "AND cnt < TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-02 00:00:00')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testSumOfString() throws Exception
  {
    // Perhaps should be 13, but dim1 has "1", "2" and "10.1"; and CAST('10.1' AS INTEGER) = 0 since parsing is strict.

    testQuery(
        "SELECT SUM(CAST(dim1 AS INTEGER)) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(GenericSumAggregatorFactory.expr("a0", "CAST(\"dim1\", 'LONG')", ValueDesc.LONG))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{13L}
        )
    );
  }

  @Test
  public void testSumOfExtractionFn() throws Exception
  {
    // Perhaps should be 13, but dim1 has "1", "2" and "10.1"; and CAST('10.1' AS INTEGER) = 0 since parsing is strict.

    testQuery(
        "SELECT SUM(CAST(SUBSTRING(dim1, 1, 10) AS INTEGER)) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.expr("a0", "CAST(substring(\"dim1\", 0, 10), 'LONG')", ValueDesc.LONG)
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{13L}
        )
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
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setGranularity(Granularities.ALL)
                .setDimFilter(
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
                .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                .virtualColumns(EXPR_VC("d0:v", "timestamp_floor(\"cnt\",'P1Y','','UTC')"))
                .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{T("1970-01-01"), 6L}
        )
    );
  }

  @Test
  public void testSelectDistinctWithCascadeExtractionFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE substring(substring(dim1, 2), 1, 1) = 'e' OR dim2 = 'a'",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setDimFilter(
                            OR(
                                SELECTOR(
                                    "dim1",
                                    "e",
                                    CASCADE(
                                        new SubstringDimExtractionFn(1, null),
                                        new SubstringDimExtractionFn(0, 1)
                                    )
                                ),
                                SELECTOR("dim2", "a", null)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithStrlenFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo "
        + "WHERE CHARACTER_LENGTH(dim1) = 3 OR CAST(CHARACTER_LENGTH(dim1) AS varchar) = 3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setDimFilter(
                            OR(
                                EXPR_FILTER("(strlen(\"dim1\") == 3)"),
                                EXPR_FILTER("(CAST(strlen(\"dim1\"), 'STRING') == 3)")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithLimit() throws Exception
  {
    // Should use topN even if approximate topNs are off, because this query is exact.

    testQuery(
        "SELECT DISTINCT dim2 FROM druid.foo LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery2() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 5) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery3() throws Exception
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery4() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 DESC LIMIT 5) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                .metric(new InvertedTopNMetricSpec(
                    new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME)
                ))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"abc"},
            new Object[]{"a"}
        )
    );
  }

  @Test
  public void testCountDistinct() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(distinct dim2), COUNT(distinct unique_dim1) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                      new CardinalityAggregatorFactory(
                          "a1",
                          null,
                          DIMS(DefaultDimensionSpec.of("dim2", null)),
                          null,
                          null,
                          false,
                          true
                      ),
                      new HyperUniquesAggregatorFactory("a2", "unique_dim1", null, true)
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 6L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new FilteredAggregatorFactory(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              ImmutableList.of(DefaultDimensionSpec.of("m1", "m1")),
                              null,
                              null,
                              false,
                              true
                          ),
                          BOUND("m1", "4", null, false, false, null, StringComparators.NUMERIC_NAME)
                      ),
                      new FilteredAggregatorFactory(
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              ImmutableList.of(DefaultDimensionSpec.of("dim1", "dim1")),
                              null,
                              null,
                              false,
                              true
                          ),
                          BOUND("m1", "4", null, false, false, null, StringComparators.NUMERIC_NAME)
                      ),
                      new FilteredAggregatorFactory(
                          new HyperUniquesAggregatorFactory("a2", "unique_dim1", null, true),
                          BOUND("m1", "4", null, false, false, null, StringComparators.NUMERIC_NAME)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 3L, 3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setGranularity(Granularities.ALL)
                                  .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new FilteredAggregatorFactory(
                          CountAggregatorFactory.of("a0"), NOT(SELECTOR("d0", "", null))
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testApproxCountDistinctWhenHllDisabled() throws Exception
  {
    // When HLL is disabled, APPROX_COUNT_DISTINCT is still approximate.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT APPROX_COUNT_DISTINCT(dim2) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          DIMS(DefaultDimensionSpec.of("dim2", null)),
                          null,
                          null,
                          false,
                          true
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testExactCountDistinctWithGroupingAndOtherAggregators() throws Exception
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, SUM(cnt), COUNT(distinct dim1) FROM druid.foo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            DefaultDimensionSpec.of("dim1", "d0"),
                                            DefaultDimensionSpec.of("dim2", "d1")
                                        )
                                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("d1", "_d0"))
                        .setAggregatorSpecs(
                            GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                            new FilteredAggregatorFactory(
                                CountAggregatorFactory.of("_a1"),
                                NOT(SELECTOR("d0", "", null))
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L, 1L},
            new Object[]{"", 3L, 3L},
            new Object[]{"abc", 1L, 1L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      EXPR_VC("a4:v", "concat(substring(\"dim2\", 0, 1),'x')")
                  )
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                      new CardinalityAggregatorFactory(
                          "a1",
                          null,
                          DIMS(DefaultDimensionSpec.of("dim2", "dim2")),
                          null,
                          null,
                          false,
                          true
                      ),
                      new FilteredAggregatorFactory(
                          new CardinalityAggregatorFactory(
                              "a2",
                              null,
                              DIMS(DefaultDimensionSpec.of("dim2", "dim2")),
                              null,
                              null,
                              false,
                              true
                          ),
                          NOT(SELECTOR("dim2", "", null))
                      ),
                      new CardinalityAggregatorFactory(
                          "a3",
                          null,
                          DIMS(
                              new ExtractionDimensionSpec(
                                  "dim2",
                                  "dim2",
                                  new SubstringDimExtractionFn(0, 1)
                              )
                          ),
                          null,
                          null,
                          false,
                          true
                      ),
                      new CardinalityAggregatorFactory(
                          "a4",
                          null,
                          DIMS(DefaultDimensionSpec.of("a4:v", "a4:v")),
                          null,
                          null,
                          false,
                          true
                      ),
                      new HyperUniquesAggregatorFactory("a5", "unique_dim1", null, true)
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 2L, 2L, 2L, 6L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            DefaultDimensionSpec.of("m2", "d0"),
                                            DefaultDimensionSpec.of("dim1", "d1")
                                        )
                                        .setDimFilter(SELECTOR("m1", "5.0", null))
                                        .setAggregatorSpecs(GenericMaxAggregatorFactory.ofLong("a0", "__time"))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("_d0:v", "_d0"),
                            DefaultDimensionSpec.of("d1", "_d1")
                        )
                        .setVirtualColumns(EXPR_VC("_d0:v", "timestamp_floor(\"a0\",'PT1H','','UTC')"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("_a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{978393600000L, "def", 1L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(
                                      GroupByQuery.builder()
                                                  .setDataSource(CalciteTests.DATASOURCE1)
                                                  .setGranularity(Granularities.ALL)
                                                  .setDimensions(
                                                      DefaultDimensionSpec.of("dim1", "d0"),
                                                      DefaultDimensionSpec.of("dim2", "d1")
                                                  )
                                                  .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                                  .build()
                                  )
                                  .setGranularity(Granularities.ALL)
                                  .setDimensions(DefaultDimensionSpec.of("d1", "_d0"))
                                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("_a0", "a0"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("a0", "_a0"),
                      CountAggregatorFactory.of("a1")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L}
        )
    );
  }

  @Test
  public void testExplainDoubleNestedGroupBy() throws Exception
  {
    final String explanation =
        "DruidOuterQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"__subquery__\"},\"descending\":false,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"sum\",\"name\":\"a0\",\"fieldName\":\"cnt\",\"inputType\":\"long\"},{\"type\":\"count\",\"name\":\"a1\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{a0:long, a1:long}])\n"
        + "  DruidOuterQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"__subquery__\"},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\"}],\"aggregations\":[{\"type\":\"sum\",\"name\":\"a0\",\"fieldName\":\"cnt\",\"inputType\":\"long\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"},\"descending\":false}], signature=[{a0:long}])\n"
        + "    DruidQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim1\",\"outputName\":\"d0\"},{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d1\"}],\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"},\"descending\":false}], signature=[{d0:string, d1:string, a0:long}])\n";

    testQuery(
        "EXPLAIN PLAN FOR SELECT SUM(cnt), COUNT(*) FROM (\n"
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
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      new QueryDataSource(
                          GroupByQuery.builder()
                                      .setDataSource(CalciteTests.DATASOURCE1)
                                      .setGranularity(Granularities.ALL)
                                      .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                      .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                      .build()
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                      CountAggregatorFactory.of("_a1")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L}
        )
    );
  }

  @Test
  public void testMinMaxAvgDailyCountWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM ("
        + "  SELECT max(cnt), min(cnt), avg(cnt), TIME_EXTRACT(max(t), 'EPOCH') last_time, count(1) num_days FROM (\n"
        + "      SELECT TIME_FLOOR(__time, 'P1D') AS t, count(1) cnt\n"
        + "      FROM \"foo\"\n"
        + "      GROUP BY 1\n"
        + "  )"
        + ") LIMIT 1\n",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(
                      Druids.newTimeseriesQueryBuilder()
                            .setDataSource(CalciteTests.DATASOURCE1)
                            .setGranularity(Granularities.DAY)
                            .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                            .setPostAggregatorSpecs(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1D','','UTC')"))
                            .setContext(TIMESERIES_CONTEXT_DEFAULT)
                            .build()
                  )
                  .setGranularity(Granularities.ALL)
                  .setAggregatorSpecs(
                      GenericMaxAggregatorFactory.ofLong("_a0", "a0"),
                      GenericMinAggregatorFactory.ofLong("_a1", "a0"),
                      GenericSumAggregatorFactory.ofLong("_a2:sum", "a0"),
                      CountAggregatorFactory.of("_a2:count"),
                      GenericMaxAggregatorFactory.ofLong("_a3", "d0"),
                      CountAggregatorFactory.of("_a4")
                  )
                  .setPostAggregatorSpecs(
                      new ArithmeticPostAggregator(
                          "_a2",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, "_a2:sum"),
                              new FieldAccessPostAggregator(null, "_a2:count")
                          )
                      ),
                      EXPR_POST_AGG("s0", "timestamp_extract('EPOCH',\"_a3\",'UTC')")
                  )
                  .setLimit(1)
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{1L, 1L, 1L, 978480000L, 6L})
    );
  }

  @Test
  public void testAvgDailyCountDistinct() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  AVG(u)\n"
        + "FROM (SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT(cnt) AS u FROM druid.foo GROUP BY 1)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(
                      Druids.newTimeseriesQueryBuilder()
                            .setDataSource(CalciteTests.DATASOURCE1)
                            .setGranularity(Granularities.DAY)
                            .setAggregatorSpecs(
                                new CardinalityAggregatorFactory(
                                    "a0:a",
                                    null,
                                    DIMS(DefaultDimensionSpec.of("cnt", "cnt")),
                                    null,
                                    null,
                                    false,
                                    true
                                )
                            )
                            .setPostAggregatorSpecs(
                                new HyperUniqueFinalizingPostAggregator("a0", "a0:a", true),
                                EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1D','','UTC')")
                            )
                            .setContext(TIMESERIES_CONTEXT_DEFAULT)
                            .build()
                  )
                  .setGranularity(Granularities.ALL)
                  .setAggregatorSpecs(
                      GenericSumAggregatorFactory.ofLong("_a0:sum", "a0"),
                      CountAggregatorFactory.of("_a0:count")
                  )
                  .setPostAggregatorSpecs(
                      new ArithmeticPostAggregator(
                          "_a0",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, "_a0:sum"),
                              new FieldAccessPostAggregator(null, "_a0:count")
                          )
                      )
                  )
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }

  @Test
  public void testTopNFilterJoin() throws Exception
  {
    // Filters on top N values of some dimension by using an inner join.
    testQuery(
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
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                .metric(new NumericTopNMetricSpec("a0"))
                .threshold(2)
                .context(QUERY_CONTEXT_DEFAULT)
                .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(IN("dim2", "", "a"))
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L}
        )
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
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                .metric(new NumericTopNMetricSpec("a0"))
                .threshold(2)
                .context(QUERY_CONTEXT_DEFAULT)
                .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(IN("dim2", "", "a"))
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L}
        )
    );
  }

  @Test
  public void testRemovableLeftJoin() throws Exception
  {
    // LEFT JOIN where the right-hand side can be ignored.

    testQuery(
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testExactCountDistinctOfSemiJoinResult() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + "  )\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(new ExtractionDimensionSpec("dim1", "d0", new SubstringDimExtractionFn(0, 1)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setGranularity(Granularities.ALL)
                                  .setDimFilter(
                                      IN(
                                          "dim2",
                                          ImmutableList.of("1", "2", "a", "d"),
                                          new SubstringDimExtractionFn(0, 1)
                                      )
                                  )
                                  .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testExactCountDistinctOfJoinResult() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + "  )\n"
        + ")",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(GroupByQuery.builder().setDataSource(
                      new Druids.JoinQueryBuilder()
                          .dataSource(
                              "foo",
                              new Druids.SelectQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .columns("dim2", "v0")
                                  .virtualColumns(EXPR_VC("v0", "substring(\"dim2\", 0, 1)"))
                                  .streaming()
                          )
                          .dataSource(
                              "foo$",
                              new GroupByQuery.Builder()
                                  .setDataSource("foo")
                                  .filters(NOT(SELECTOR("dim1", "", null)))
                                  .granularity(Granularities.ALL)
                                  .setDimensions(new ExtractionDimensionSpec("dim1", "d0", new SubstringDimExtractionFn(0, 1)))
                                  .build()
                          )
                          .element(JoinElement.inner("foo.v0 = foo$.d0"))
                          .build())
                      .setGranularity(Granularities.ALL)
                      .setDimensions(DefaultDimensionSpec.of("dim2", "_d0"))
                      .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testExactCountDistinctOfJoinResult2() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo as X\n"
        + "  WHERE EXISTS ( SELECT * FROM druid.foo as Y WHERE X.dim2 = Y.dim2  )\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim2", "", null)))
                        .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setGranularity(Granularities.ALL)
                                  .setDimFilter(IN("dim2", "a", "abc"))
                                  .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testExplainExactCountDistinctOfSemiJoinResult() throws Exception
  {
    final String explanation =
        "DruidOuterQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"__subquery__\"},\"descending\":false,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{a0:long}])\n"
        + "  DruidSemiJoinRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"},\"descending\":false}], leftExpressions=[[SUBSTRING($3, 1, 1)]], rightKeys=[[0]])\n"
        + "    DruidQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"\"}},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"dim1\",\"outputName\":\"d0\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"},\"descending\":false}], signature=[{d0:string}])\n";

    testQuery(
        "EXPLAIN PLAN FOR SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + "  )\n"
        + ")",
        ImmutableList.of(),
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setDimFilter(NOT(SELECTOR("dim2", "", null)))
                                  .setGranularity(Granularities.ALL)
                                  .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                      CountAggregatorFactory.of("_a1")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 2L}
        )
    );

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 IS NOT NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setDimFilter(NOT(SELECTOR("dim2", "", null)))
                                  .setGranularity(Granularities.ALL)
                                  .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                      CountAggregatorFactory.of("_a1")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 2L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      new TopNQueryBuilder()
                          .dataSource(CalciteTests.DATASOURCE1)
                          .granularity(Granularities.ALL)
                          .dimension(DefaultDimensionSpec.of("dim2", "d0"))
                          .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC_NAME))
                          .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                          .threshold(1)
                          .context(QUERY_CONTEXT_DEFAULT)
                          .build()
                  )
                  .filters(BOUND("a0", "0", null, true, false, null, StringComparators.NUMERIC_NAME))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                      CountAggregatorFactory.of("_a1")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 1L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setGranularity(Granularities.ALL)
                                  .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                                  .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                  .build()
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(
                      CountAggregatorFactory.of("a0"),
                      new CardinalityAggregatorFactory(
                          "a1",
                          null,
                          DIMS(DefaultDimensionSpec.of("d0", null)),
                          null,
                          null,
                          false,
                          true
                      )
                  )
                  .postAggregators(EXPR_POST_AGG("p0", "((1 - (\"a1\" / \"a0\")) * 100)"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L, 5L, 0.0f}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("a0", "_d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("_a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"3", 1L},
            new Object[]{"2", 1L},
            new Object[]{"1", 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                        .setAggregatorSpecs(
                                            GenericSumAggregatorFactory.ofLong("a0", "cnt")
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("a0", "_d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("_a0"))
                        .setLimitSpec(
                            LimitSpec.of(
                                2, OrderByColumnSpec.asc("_d0", StringComparators.LEXICOGRAPHIC_NAME)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.ALL)
                  .aggregators(
                      GenericSumAggregatorFactory.ofLong("a0", "cnt"),
                      new CardinalityAggregatorFactory(
                          "a1",
                          null,
                          DIMS(DefaultDimensionSpec.of("dim2", null)),
                          null,
                          null,
                          false,
                          true
                      )
                  )
                  .postAggregators(
                      EXPR_POST_AGG("p0", "CAST(\"a1\", 'FLOAT')"),
                      EXPR_POST_AGG("p1", "(\"a0\" / \"a1\")"),
                      EXPR_POST_AGG("p2", "((\"a0\" / \"a1\") + 3)"),
                      EXPR_POST_AGG("p3", "((CAST(\"a0\", 'FLOAT') / CAST(\"a1\", 'FLOAT')) + 3)")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 3.0f, 2L, 5L, 5.0f}
        )
    );
  }

  @Test
  public void testCountDistinctOfSubstring() throws Exception
  {
    testQuery(
        "SELECT COUNT(DISTINCT SUBSTRING(dim1, 1, 1)) FROM druid.foo WHERE dim1 <> ''",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(NOT(SELECTOR("dim1", "", null)))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          DIMS(new ExtractionDimensionSpec("dim1", null, new SubstringDimExtractionFn(0, 1))),
                          null,
                          null,
                          false,
                          true
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testCountDistinctOfTrim() throws Exception
  {
    // Test a couple different syntax variants of TRIM.

    testQuery(
        "SELECT COUNT(DISTINCT TRIM(BOTH ' ' FROM dim1)) FROM druid.foo WHERE TRIM(dim1) <> ''",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(NOT(SELECTOR("dim1", "", null)))
                  .granularity(Granularities.ALL)
                  .virtualColumns(EXPR_VC("a0:v", "btrim(\"dim1\",' ')"))
                  .filters(EXPR_FILTER("(btrim(\"dim1\",' ') != '')"))
                  .aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          DIMS(DefaultDimensionSpec.of("a0:v", "a0:v")),
                          null,
                          null,
                          false,
                          true
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(EXPR_VC(
                            "d0:v",
                            "(((timestamp_extract('MONTH',\"__time\",'UTC') - 1) / 3) + 1)"
                        ))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1, 6L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            NOT(SELECTOR(
                                "dim1",
                                "x",
                                new RegexDimExtractionFn("^(.)", 1, true, null)
                            ))
                        )
                        .setDimensions(
                            new ExtractionDimensionSpec(
                                "dim1",
                                "d0",
                                new RegexDimExtractionFn("^.", 0, true, null)
                            ),
                            new ExtractionDimensionSpec(
                                "dim1",
                                "d1",
                                new RegexDimExtractionFn("^(.)", 1, true, null)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", ""},
            new Object[]{"1", "1"},
            new Object[]{"2", "2"},
            new Object[]{"a", "a"},
            new Object[]{"d", "d"}
        )
    );
  }

  @Test
  public void testGroupBySortPushDown() throws Exception
  {
    testQuery(
        "SELECT dim2, dim1, SUM(cnt) FROM druid.foo GROUP BY dim2, dim1 ORDER BY dim1 LIMIT 4",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("dim2", "d0"),
                            DefaultDimensionSpec.of("dim1", "d1")
                        )
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(4, OrderByColumnSpec.asc("d1")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "", 1L},
            new Object[]{"a", "1", 1L},
            new Object[]{"", "10.1", 1L},
            new Object[]{"", "2", 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("dim2", "d1")
                        )
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(4, OrderByColumnSpec.asc("d1")))
                        .setHavingSpec(EXPR_HAVING("(\"a0\" == 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "", 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"abc", "", 1L},
            new Object[]{"", "a", 1L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000/P2M")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01T01:02/2002")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testSelectCurrentTimeAndDateLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_DATE + INTERVAL '1' DAY",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{T("2000-01-01T00Z", LOS_ANGELES), D("1999-12-31"), D("2000-01-01")}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-02T00Z/2002-01-01T08Z")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampOnView() throws Exception
  {
    testQuery(
        "SELECT * FROM bview",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-02/2002")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-02T00Z/2002-01-01T08Z")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnNotTimeFloor() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) <> TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(
                      new Interval(DateTimes.MIN, DateTimes.of("2001-01-01")),
                      new Interval(DateTimes.of("2001-02-01"), DateTimes.MAX)
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorComparison() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval(DateTimes.MIN, DateTimes.of("2000-02-01"))))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorComparisonMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval(DateTimes.MIN, DateTimes.of("2000-03-01"))))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000/P1M")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-02-01/P2M"), Intervals.of("2000-05-01/P1M")))
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS())
                  .granularity(Granularities.ALL)
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByFloor() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_SUBQUERIES, // Sanity check; this simple query should work with subqueries disabled.
        "SELECT floor(CAST(dim1 AS float)), COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(EXPR_VC("d0:v", "floor(CAST(\"dim1\", 'FLOAT'))"))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{0.0f, 3L},
            new Object[]{1.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{10.0f, 1L}
        )
    );
  }

  @Test
  public void testGroupByFloorWithOrderBy() throws Exception
  {
    testQuery(
        "SELECT floor(CAST(dim1 AS float)) AS fl, COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float)) ORDER BY fl DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(EXPR_VC("d0:v", "floor(CAST(\"dim1\", 'FLOAT'))"))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{1.0f, 1L},
            new Object[]{0.0f, 3L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.YEAR)
                        .setDimensions(DefaultDimensionSpec.of("dim2", "d1"))
                        .setVirtualColumns(EXPR_VC("d0:v", "timestamp_floor(\"__time\",'P1Y','','UTC')"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setPostAggregatorSpecs(
                            EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1Y','','UTC')")
                        )
                        .setLimitSpec(
                            LimitSpec.of(
                                OrderByColumnSpec.asc("d0"),
                                OrderByColumnSpec.asc("d1"),
                                OrderByColumnSpec.desc("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000"), "", 2L},
            new Object[]{T("2000"), "a", 1L},
            new Object[]{T("2001"), "", 1L},
            new Object[]{T("2001"), "a", 1L},
            new Object[]{T("2001"), "abc", 1L}
        )
    );
  }

  @Test
  public void testGroupByStringLength() throws Exception
  {
    testQuery(
        "SELECT CHARACTER_LENGTH(dim1), COUNT(*) FROM druid.foo GROUP BY CHARACTER_LENGTH(dim1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(EXPR_VC("d0:v", "strlen(\"dim1\")"))
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{0, 1L},
            new Object[]{1, 2L},
            new Object[]{3, 2L},
            new Object[]{4, 1L}
        )
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
//            GroupByQuery.builder()
//                        .setDataSource(CalciteTests.DATASOURCE1)
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.MONTH)
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, T("2000-01-01")},
            new Object[]{3L, T("2001-01-01")}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01/2001-02-01")))
                  .granularity(Granularities.ALL)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimeZone.forID(LOS_ANGELES)))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(
                      EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','America/Los_Angeles')")
                  )
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, T("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, T("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, T("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, T("2001-01-01", LOS_ANGELES)}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.MONTH)
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, T("2000-01-01")},
            new Object[]{3L, T("2001-01-01")}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            EXPR_VC(
                                "d0:v",
                                "timestamp_floor(timestamp_shift(\"__time\",'P1D',-1),'P1M','','UTC')"
                            )
                        )
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, T("1999-12-01")},
            new Object[]{2L, T("2000-01-01")},
            new Object[]{1L, T("2000-12-01")},
            new Object[]{2L, T("2001-01-01")}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            EXPR_VC(
                                "d0:v",
                                "timestamp_floor((\"__time\" + -86400000),'P1M','','UTC')"
                            )
                        )
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, T("1999-12-01")},
            new Object[]{2L, T("2000-01-01")},
            new Object[]{1L, T("2000-12-01")},
            new Object[]{2L, T("2001-01-01")}
        )
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
        ImmutableList.of(
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
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M',3723000,'UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, T("1999-12-01T01:02:03")},
            new Object[]{2L, T("2000-01-01T01:02:03")},
            new Object[]{1L, T("2000-12-01T01:02:03")},
            new Object[]{2L, T("2001-01-01T01:02:03")}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimeZone.forID(LOS_ANGELES)))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(
                      EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','America/Los_Angeles')")
                  )
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, T("1999-12-01T08")},
            new Object[]{2L, T("2000-01-01T08")},
            new Object[]{1L, T("2000-12-01T08")},
            new Object[]{2L, T("2001-01-01T08")}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimeZone.forID(LOS_ANGELES)))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(
                      EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','America/Los_Angeles')")
                  )
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, T("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, T("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, T("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, T("2001-01-01", LOS_ANGELES)}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000/2000-01-02")))
                  .granularity(new PeriodGranularity(Period.hours(1), null, DateTimeZone.UTC))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'PT1H','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS)
                  .build()
        ),
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1D','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, D("2000-01-01")},
            new Object[]{1L, D("2000-01-02")},
            new Object[]{1L, D("2000-01-03")},
            new Object[]{1L, D("2001-01-01")},
            new Object[]{1L, D("2001-01-02")},
            new Object[]{1L, D("2001-01-03")}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.months(3), null, DateTimeZone.UTC))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P3M','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, D("2000-01-01")},
            new Object[]{3L, D("2001-01-01")}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(Granularities.MONTH)
                  .descending(true)
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2001-01-01"), 3L},
            new Object[]{T("2000-01-01"), 3L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setVirtualColumns(EXPR_VC("d0:v", "timestamp_extract('YEAR',\"__time\",'UTC')"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                        .setVirtualColumns(EXPR_VC("d0:v", "timestamp_format(\"__time\",'yyyy MM','UTC')"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2000 01", 3L},
            new Object[]{"2001 01", 3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(CalciteTests.DATASOURCE1)
                  .setGranularity(new PeriodGranularity(Period.parse("P1Y"), null, DateTimeZone.UTC))
                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG(
                      "d0",
                      "timestamp_extract('YEAR',timestamp_floor(\"__time\",'P1Y','','UTC'),'UTC')"
                  ))
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(CalciteTests.DATASOURCE1)
                  .setGranularity(new PeriodGranularity(
                      Period.parse("P1Y"),
                      null,
                      DateTimeZone.forID("America/Los_Angeles")
                  ))
                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .setPostAggregatorSpecs(
                      EXPR_POST_AGG(
                          "d0",
                          "timestamp_extract('YEAR',timestamp_floor(\"__time\",'P1Y','','America/Los_Angeles'),'America/Los_Angeles')"
                      )
                  )
                  .setContext(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1999L, 1L},
            new Object[]{2000L, 3L},
            new Object[]{2001L, 2L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(CalciteTests.DATASOURCE1)
                  .setGranularity(new PeriodGranularity(Period.parse("P1M"), null, DateTimeZone.UTC))
                  .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .setLimitSpec(LimitSpec.of(1, OrderByColumnSpec.asc("d0")))
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.parse("P1M"), null, DateTimeZone.UTC))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .limitSpec(LimitSpec.of(1))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 3L}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .granularity(new PeriodGranularity(Period.parse("P1M"), null, DateTimeZone.UTC))
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .addPostAggregator(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .limitSpec(LimitSpec.of(1, OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 3L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("dim2", "d0"),
                            DefaultDimensionSpec.of("d1:v", "d1")
                        )
                        .setVirtualColumns(EXPR_VC("d1:v", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                        .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0"), OrderByColumnSpec.asc("d1")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", T("2000-01-01"), 2L},
            new Object[]{"", T("2001-01-01"), 1L},
            new Object[]{"a", T("2000-01-01"), 1L},
            new Object[]{"a", T("2001-01-01"), 1L},
            new Object[]{"abc", T("2001-01-01"), 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            AND(
                                NOT(SELECTOR("dim1", "xxx", null)),
                                IN("dim2", "1", "10.1", "2", "abc", "def")
                            )
                        )
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("dim2", "d1")
                        )
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d1")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", 1L}
        )
    );
  }

  @Test
  public void testUsingSubqueryAsPartOfOrFilter() throws Exception
  {
    // This query should ideally be plannable without fallback, but it's not. The "OR" means it isn't really
    // a semiJoin and so the filter condition doesn't get converted.

    final String explanation =
        "BindableSort(sort0=[$1], dir0=[ASC])\n"
        + "  BindableAggregate(group=[{0, 1}], EXPR$2=[COUNT()])\n"
        + "    BindableFilter(condition=[OR(=($0, 'xxx'), CAST(AND(IS NOT NULL($4), <>($2, 0), IS NOT NULL($1))):BOOLEAN)])\n"
        + "      BindableJoin(condition=[=($1, $3)], joinType=[left])\n"
        + "        BindableJoin(condition=[true], joinType=[inner])\n"
        + "          DruidQueryRel(query=[{\"queryType\":\"select.stream\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"descending\":false,\"columns\":[\"dim1\",\"dim2\"],\"limit\":-1,\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{dim1:string, dim2:string}])\n"
        + "          DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"descending\":false,\"filter\":{\"type\":\"like\",\"dimension\":\"dim1\",\"pattern\":\"%bc\",\"escape\":null,\"extractionFn\":null},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"}}], signature=[{a0:long}])\n"
        + "        DruidQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"filter\":{\"type\":\"like\",\"dimension\":\"dim1\",\"pattern\":\"%bc\",\"escape\":null,\"extractionFn\":null},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim1\",\"outputName\":\"d0\"},{\"type\":\"default\",\"dimension\":\"d1:v\",\"outputName\":\"d1\"}],\"virtualColumns\":[{\"type\":\"expr\",\"expression\":\"1\",\"outputName\":\"d1:v\"}],\"limitSpec\":{\"type\":\"noop\"},\"context\":{\"defaultTimeout\":300000,\"groupby.sort.on.time\":false,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\"},\"descending\":false}], signature=[{d0:string, d1:long}])\n";
    final String theQuery = "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
                            + "WHERE dim1 = 'xxx' OR dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 LIKE '%bc')\n"
                            + "group by dim1, dim2 ORDER BY dim2";

    assertQueryIsUnplannable(theQuery);

    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "EXPLAIN PLAN FOR " + theQuery,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{explanation})
    );
  }

  @Test
  public void testUsingSubqueryAsFilterForbiddenByConfig() throws Exception
  {
    assertQueryIsUnplannable(
        PLANNER_CONFIG_NO_SUBQUERIES,
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo "
        + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')"
        + "AND dim1 <> 'xxx'"
        + "group by dim1, dim2 ORDER BY dim2"
    );
  }

  @Test
  public void testUsingSubqueryAsFilterOnTwoColumns() throws Exception
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2 FROM druid.foo "
        + " WHERE (dim1, dim2) IN ("
        + "   SELECT dim1, dim2 FROM ("
        + "     SELECT dim1, dim2, COUNT(*)"
        + "     FROM druid.foo"
        + "     WHERE dim2 = 'abc'"
        + "     GROUP BY dim1, dim2"
        + "     HAVING COUNT(*) = 1"
        + "   )"
        + " )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(SELECTOR("dim2", "abc", null))
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("dim2", "d1")
                        )
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setHavingSpec(EXPR_HAVING("(\"a0\" == 1)"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("__time", "cnt", "dim1", "dim2"))
                .filters(OR(
                    SELECTOR("dim1", "def", null),
                    AND(
                        SELECTOR("dim1", "def", null),
                        SELECTOR("dim2", "abc", null)
                    )
                ))
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{T("2001-01-02"), 1L, "def", "abc"}
        )
    );
  }

  @Test
  public void testUsingSubqueryAsFilterWithInnerSort() throws Exception
  {
    // Regression test for https://github.com/druid-io/druid/issues/4208

    testQuery(
        "SELECT dim1, dim2 FROM druid.foo\n"
        + " WHERE dim2 IN (\n"
        + "   SELECT dim2\n"
        + "   FROM druid.foo\n"
        + "   GROUP BY dim2\n"
        + "   ORDER BY dim2 DESC\n"
        + " )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0", StringComparators.LEXICOGRAPHIC_NAME)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            new Druids.SelectQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .columns(Arrays.asList("dim1", "dim2"))
                .filters(IN("dim2", "", "a", "abc"))
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"", "a"},
            new Object[]{"10.1", ""},
            new Object[]{"2", ""},
            new Object[]{"1", "a"},
            new Object[]{"def", "abc"},
            new Object[]{"abc", ""}
        )
    );
  }

  @Test
  public void testSemiJoinWithOuterTimeExtract() throws Exception
  {
    testQuery(
        "SELECT dim1, EXTRACT(MONTH FROM __time) FROM druid.foo\n"
        + " WHERE dim2 IN (\n"
        + "   SELECT dim2\n"
        + "   FROM druid.foo\n"
        + "   WHERE dim1 = 'def'\n"
        + " ) AND dim1 <> ''",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                        .setDimFilter(SELECTOR("dim1", "def", null))
                        .build(),
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(
                    EXPR_VC("v0", "timestamp_extract('MONTH',\"__time\",'UTC')")
                )
                .filters(
                    AND(
                        NOT(SELECTOR("dim1", "", null)),
                        SELECTOR("dim2", "abc", null)
                    )
                )
                .columns("dim1", "v0")
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testSemiJoinWithOuterTimeExtractAggregateWithOrderBy() throws Exception
  {
    testQuery(
        "SELECT COUNT(DISTINCT dim1), EXTRACT(MONTH FROM __time) FROM druid.foo\n"
        + " WHERE dim2 IN (\n"
        + "   SELECT dim2\n"
        + "   FROM druid.foo\n"
        + "   WHERE dim1 = 'def'\n"
        + " ) AND dim1 <> ''"
        + "GROUP BY EXTRACT(MONTH FROM __time)\n"
        + "ORDER BY EXTRACT(MONTH FROM __time)",
        ImmutableList.of(
            GroupByQuery
                .builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setGranularity(Granularities.ALL)
                .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                .setDimFilter(SELECTOR("dim1", "def", null))
                .build(),
            GroupByQuery
                .builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setVirtualColumns(EXPR_VC("d0:v", "timestamp_extract('MONTH',\"__time\",'UTC')"))
                .setDimFilter(
                    AND(
                        NOT(SELECTOR("dim1", "", null)),
                        SELECTOR("dim2", "abc", null)
                    )
                )
                .setDimensions(DefaultDimensionSpec.of("d0:v", "d0"))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    new CardinalityAggregatorFactory(
                        "a0",
                        null,
                        ImmutableList.of(DefaultDimensionSpec.of("dim1")),
                        null,
                        null,
                        false,
                        true
                    )
                )
                .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1L}
        )
    );
  }

  @Test
  public void testUsingSubqueryWithExtractionFns() throws Exception
  {
    testQuery(
        "SELECT dim2, COUNT(*) FROM druid.foo "
        + "WHERE substring(dim2, 1, 1) IN (SELECT substring(dim1, 1, 1) FROM druid.foo WHERE dim1 <> '')"
        + "group by dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(
                            new ExtractionDimensionSpec("dim1", "d0", new SubstringDimExtractionFn(0, 1))
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(IN(
                            "dim2",
                            ImmutableList.of("1", "2", "a", "d"),
                            new SubstringDimExtractionFn(0, 1)
                        ))
                        .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L}
        )
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE2)
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(OR(
                            new LikeDimFilter("dim1", "דר%", null, null),
                            new SelectorDimFilter("dim1", "друид", null)
                        ))
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("dim2", "d1")
                        )
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"друид", "ru", 1L},
            new Object[]{"דרואיד", "he", 1L}
        )
    );
  }

  @Test
  public void testProjectAfterSort() throws Exception
  {
    testQuery(
        "select dim1 from (select dim1, dim2, count(*) cnt from druid.foo group by dim1, dim2 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("dim2", "d1")
                        )
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testProjectAfterSort2() throws Exception
  {
    testQuery(
        "select s / cnt, dim1, dim2, s from (select dim1, dim2, count(*) cnt, sum(m2) s from druid.foo group by dim1, dim2 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("dim1", "d0"),
                            DefaultDimensionSpec.of("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            CountAggregatorFactory.of("a0"),
                            GenericSumAggregatorFactory.ofDouble("a1", "m2")
                        )
                        .setPostAggregatorSpecs(EXPR_POST_AGG("s0", "(\"a1\" / \"a0\")"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, "", "a", 1.0},
            new Object[]{4.0, "1", "a", 4.0},
            new Object[]{2.0, "10.1", "", 2.0},
            new Object[]{3.0, "2", "", 3.0},
            new Object[]{6.0, "abc", "", 6.0},
            new Object[]{5.0, "def", "abc", 5.0}
        )
    );
  }

  @Test
  public void testProjectAfterSort3() throws Exception
  {
    testQuery(
        "select dim1 from (select dim1, dim1, count(*) cnt from druid.foo group by dim1, dim1 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setDimensions(DefaultDimensionSpec.of("dim1", "d0"))
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
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
        + "  ORDER BY cnt"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            DefaultDimensionSpec.of("__time", "d0"),
                                            DefaultDimensionSpec.of("dim1", "d1"),
                                            DefaultDimensionSpec.of("m2", "d2")
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            DefaultDimensionSpec.of("d0", "_d0"),
                            DefaultDimensionSpec.of("d1", "_d1")
                        )
                        .setAggregatorSpecs(CountAggregatorFactory.of("a0"))
                        .setLimitSpec(LimitSpec.of(OrderByColumnSpec.asc("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L}
        )
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
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .filters(SELECTOR("dim2", "a", null))
                  .granularity(Granularities.YEAR)
                  .aggregators(
                      GenericSumAggregatorFactory.ofDouble("a0", "m1"),
                      GenericSumAggregatorFactory.ofDouble("a1", "m2")
                  )
                  .postAggregators(
                      EXPR_POST_AGG("p0", "(\"a0\" + \"a1\")"),
                      EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1Y','','UTC')")
                  )
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.desc("d0")))
                  .descending(true)
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{978307200000L, 4.0, 8.0},
            new Object[]{946684800000L, 1.0, 2.0}
        )
    );
  }

  @Test
  @Ignore("Does not allow topN on metric")
  public void testPostAggWithTopN() throws Exception
  {
    testQuery(
        "SELECT AVG(m2), SUM(m1) + SUM(m2) FROM druid.foo WHERE dim2 = 'a' GROUP BY m1 ORDER BY m1 LIMIT 5",
        Collections.singletonList(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .granularity(Granularities.ALL)
                .dimension(DefaultDimensionSpec.of("m1", "d0"))
                .filters("dim2", "a")
                .aggregators(
                    GenericSumAggregatorFactory.ofDouble("a0:sum", "m2"),
                    CountAggregatorFactory.of("a0:count"),
                    GenericSumAggregatorFactory.ofDouble("a1", "m1"),
                    GenericSumAggregatorFactory.ofDouble("a2", "m2")
                )
                .postAggregators(
                    ImmutableList.of(
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
                )
                .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC_NAME))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, 2.0},
            new Object[]{4.0, 8.0}
        )
    );
  }

  @Test
  public void testConcat() throws Exception
  {
    testQuery(
        "SELECT CONCAT(dim1, '-', dim1, '_', dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(EXPR_VC("v0", "concat(\"dim1\",'-',\"dim1\",'_',\"dim1\")"))
                .columns("v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"-_"},
            new Object[]{"10.1-10.1_10.1"},
            new Object[]{"2-2_2"},
            new Object[]{"1-1_1"},
            new Object[]{"def-def_def"},
            new Object[]{"abc-abc_abc"}
        )
    );

    testQuery(
        "SELECT CONCAT(dim1, CONCAT(dim2,'x'), m2, 9999, dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(EXPR_VC("v0", "concat(\"dim1\",concat(\"dim2\",'x'),\"m2\",9999,\"dim1\")"))
                .columns("v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"ax1.09999"},
            new Object[]{"10.1x2.0999910.1"},
            new Object[]{"2x3.099992"},
            new Object[]{"1ax4.099991"},
            new Object[]{"defabcx5.09999def"},
            new Object[]{"abcx6.09999abc"}
        )
    );
  }

  @Test
  public void testTextcat() throws Exception
  {
    testQuery(
        "SELECT textcat(dim1, dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(EXPR_VC("v0", "concat(\"dim1\",\"dim1\")"))
                .columns("v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"10.110.1"},
            new Object[]{"22"},
            new Object[]{"11"},
            new Object[]{"defdef"},
            new Object[]{"abcabc"}
        )
    );

    testQuery(
        "SELECT textcat(dim1, CAST(m2 as VARCHAR)) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(EXPR_VC("v0", "concat(\"dim1\",CAST(\"m2\", 'STRING'))"))
                .columns("v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .streaming()
        ),
        ImmutableList.of(
            new Object[]{"1.0"},
            new Object[]{"10.12.0"},
            new Object[]{"23.0"},
            new Object[]{"14.0"},
            new Object[]{"def5.0"},
            new Object[]{"abc6.0"}
        )
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
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.of("2000-01-01/2002-01-01")))
                  .granularity(Granularities.MONTH)
                  .aggregators(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                  .postAggregators(EXPR_POST_AGG("d0", "timestamp_floor(\"__time\",'P1M','','UTC')"))
                  .limitSpec(LimitSpec.of(OrderByColumnSpec.asc("d0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, T("2000-01-01")},
            new Object[]{3L, T("2001-01-01")}
        )
    );

    // nested groupby only requires time condition for inner most query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo WHERE __time >= '2000-01-01' GROUP BY dim2)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .setDataSource(
                      new QueryDataSource(
                          GroupByQuery.builder()
                                      .setDataSource(CalciteTests.DATASOURCE1)
                                      .setInterval(QSS(Intervals.utc(
                                          DateTimes.of("2000-01-01").getMillis(),
                                          JodaUtils.MAX_INSTANT
                                      )))
                                      .setGranularity(Granularities.ALL)
                                      .setDimensions(DefaultDimensionSpec.of("dim2", "d0"))
                                      .setAggregatorSpecs(GenericSumAggregatorFactory.ofLong("a0", "cnt"))
                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                      .build()
                      )
                  )
                  .setGranularity(Granularities.ALL)
                  .setAggregatorSpecs(
                      GenericSumAggregatorFactory.ofLong("_a0", "a0"),
                      CountAggregatorFactory.of("_a1")
                  )
                  .setContext(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L}
        )
    );

    // semi-join requires time condition on both left and right query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= '2000-01-01' AND SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo\n"
        + "  WHERE dim1 <> '' AND __time >= '2000-01-01'\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Intervals.utc(DateTimes.of("2000-01-01").getMillis(), JodaUtils.MAX_INSTANT)))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(new ExtractionDimensionSpec("dim1", "d0", new SubstringDimExtractionFn(0, 1)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Intervals.utc(DateTimes.of("2000-01-01").getMillis(), JodaUtils.MAX_INSTANT)))
                  .granularity(Granularities.ALL)
                  .filters(IN(
                      "dim2",
                      ImmutableList.of("1", "2", "a", "d"),
                      new SubstringDimExtractionFn(0, 1)
                  ))
                  .aggregators(CountAggregatorFactory.of("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testJoin() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT foo.m1 X, foo2.dim2 Y FROM foo join foo2 on foo.__time = foo2.__time limit 3",
        ImmutableList.of(
            Druids.newSelectQueryBuilder()
                  .dataSource(
                      QueryDataSource.of(
                          Druids.newJoinQueryBuilder()
                                .dataSource("foo", QueryDataSource.of(
                                    Druids.newSelectQueryBuilder()
                                          .dataSource("foo")
                                          .columns("__time", "m1")
                                          .streaming())
                                )
                                .dataSource("foo2", QueryDataSource.of(
                                    Druids.newSelectQueryBuilder()
                                          .dataSource("foo2")
                                          .columns("__time", "dim2")
                                          .streaming())
                                )
                                .element(JoinElement.of(JoinType.INNER, "foo2.__time = foo.__time"))
                                .build()
                      )
                  )
                  .columns("dim2", "m1")
                  .limit(3)
                  .streaming()
        )
        , ImmutableList.of(
            new Object[]{1.0, "en"},
            new Object[]{1.0, "ru"},
            new Object[]{1.0, "he"}
        )
    );
  }

  @Test
  public void testGbyOnJoin() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT sum(foo.m1) X, foo2.dim2 Y FROM foo join foo2 on foo.__time = foo2.__time group by foo2.dim2 limit 3",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .dataSource(
                    QueryDataSource.of(
                        Druids.newJoinQueryBuilder()
                              .dataSource("foo", QueryDataSource.of(
                                  Druids.newSelectQueryBuilder()
                                        .dataSource("foo")
                                        .columns("__time", "m1")
                                        .streaming())
                              )
                              .dataSource("foo2", QueryDataSource.of(
                                  Druids.newSelectQueryBuilder()
                                        .dataSource("foo2")
                                        .columns("__time", "dim2")
                                        .streaming())
                              )
                              .element(JoinElement.of(JoinType.INNER, "foo2.__time = foo.__time"))
                              .build()
                    )
                )
                .granularity(Granularities.ALL)
                .addDimension(DefaultDimensionSpec.of("dim2", "d0"))
                .addAggregator(GenericSumAggregatorFactory.ofDouble("a0", "m1"))
                .limit(3)
                .build()
        )
        , ImmutableList.of(
            new Object[]{1.0, "en"},
            new Object[]{1.0, "ru"},
            new Object[]{1.0, "he"}
        )
    );
  }

  private void testQuery(
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        sql,
        expectedQueries,
        expectedResults
    );
  }

  private void testQuery(
      final PlannerConfig plannerConfig,
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    testQuery(plannerConfig, QUERY_CONTEXT_DEFAULT, sql, expectedQueries, expectedResults);
  }

  private void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    log.info("SQL: %s", sql);
    queryLogHook.clearRecordedQueries();
    final List<Object[]> plannerResults = getResults(plannerConfig, queryContext, sql);
    verifyResults(sql, expectedQueries, expectedResults, plannerResults);
  }

  private List<Object[]> getResults(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql
  ) throws Exception
  {
    final InProcessViewManager viewManager = new InProcessViewManager();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(walker, plannerConfig, viewManager);
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();

    final PlannerFactory plannerFactory = new PlannerFactory(
        druidSchema,
        walker,
        operatorTable,
        plannerConfig,
        new QueryConfig(),
        CalciteTests.getJsonMapper()
    );

    viewManager.createView(
        plannerFactory,
        "aview",
        "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'"
    );

    viewManager.createView(
        plannerFactory,
        "bview",
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
    );

    try (DruidPlanner planner = plannerFactory.createPlanner(queryContext)) {
      final PlannerResult plan = planner.plan(sql, null, null);
      List<Object[]> results = Sequences.toList(plan.run(), Lists.newArrayList());
      log.info("result schema " + plan.rowType());
      return results;
    }
  }

  private static final Map<String, Object> REMOVE_ID = Maps.newHashMap();

  static {
    REMOVE_ID.put(BaseQuery.QUERYID, null);
  }

  private void verifyResults(
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults,
      final List<Object[]> results
  )
  {
    log.info("results..");
    for (int i = 0; i < results.size(); i++) {
      log.info("#%d: %s", i, Arrays.toString(results.get(i)));
    }
    log.info("expected..");
    for (int i = 0; i < expectedResults.size(); i++) {
      log.info("#%d: %s", i, Arrays.toString(expectedResults.get(i)));
    }

    Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResults.size(), results.size());
    for (int i = 0; i < results.size(); i++) {
      final Object[] expected = expectedResults.get(i);
      final Object[] actual = results.get(i);
      final int masked = Arrays.asList(expected).indexOf(MASKED);
      if (masked >= 0) {
        expected[masked] = actual[masked] = null;
      }
      Assert.assertArrayEquals(
          StringUtils.format("result #%d: %s", i + 1, sql),
          expected,
          actual
      );
    }

    if (expectedQueries != null) {
      final List<Query> recordedQueries = queryLogHook.getRecordedQueries();

      Assert.assertEquals(
          StringUtils.format("query count: %s", sql),
          expectedQueries.size(),
          recordedQueries.size()
      );
      for (int i = 0; i < expectedQueries.size(); i++) {
        Assert.assertEquals(
            StringUtils.format("query #%d: %s", i + 1, sql),
            expectedQueries.get(i),
            recordedQueries.get(i).withOverriddenContext(REMOVE_ID)
        );
      }
    }
  }

  // Generate timestamps for expected results
  private static long T(final String timeString)
  {
    return Calcites.jodaToCalciteTimestamp(DateTimes.of(timeString), DateTimeZone.UTC);
  }

  // Generate timestamps for expected results
  private static long T(final String timeString, final String timeZoneString)
  {
    final DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    return Calcites.jodaToCalciteTimestamp(new DateTime(timeString, timeZone), timeZone);
  }

  // Generate day numbers for expected results
  private static int D(final String dayString)
  {
    return (int) (Intervals.utc(T("1970"), T(dayString)).toDurationMillis() / (86400L * 1000L));
  }

  private static QuerySegmentSpec QSS(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  private static AndDimFilter AND(DimFilter... filters)
  {
    return new AndDimFilter(Arrays.asList(filters));
  }

  private static OrDimFilter OR(DimFilter... filters)
  {
    return new OrDimFilter(Arrays.asList(filters));
  }

  private static NotDimFilter NOT(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  private static InDimFilter IN(String dimension, String... values)
  {
    return new InDimFilter(dimension, Arrays.asList(values), null);
  }

  private static InDimFilter IN(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  private static SelectorDimFilter SELECTOR(final String fieldName, final String value, final ExtractionFn extractionFn)
  {
    return new SelectorDimFilter(fieldName, value, extractionFn);
  }

  private static DimFilter EXPR_FILTER(final String expression)
  {
    return new MathExprFilter(expression);
  }

  private static DimFilter NUMERIC_SELECTOR(
      final String fieldName,
      final String value,
      final ExtractionFn extractionFn
  )
  {
    // We use Bound filters for numeric equality to achieve "10.0" = "10"
    return BOUND(fieldName, value, value, false, false, extractionFn, StringComparators.NUMERIC_NAME);
  }

  private static BoundDimFilter BOUND(
      final String fieldName,
      final String lower,
      final String upper,
      final boolean lowerStrict,
      final boolean upperStrict,
      final ExtractionFn extractionFn,
      final String comparator
  )
  {
    return new BoundDimFilter(fieldName, lower, upper, lowerStrict, upperStrict, comparator, extractionFn);
  }

  private static BoundDimFilter TIME_BOUND(final Object intervalObj)
  {
    final Interval interval = new Interval(intervalObj, ISOChronology.getInstanceUTC());
    return new BoundDimFilter(
        Column.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        false,
        true,
        StringComparators.NUMERIC_NAME,
        null
    );
  }

  private static CascadeExtractionFn CASCADE(final ExtractionFn... fns)
  {
    return new CascadeExtractionFn(fns);
  }

  private static List<DimensionSpec> DIMS(final DimensionSpec... dimensionSpecs)
  {
    return Arrays.asList(dimensionSpecs);
  }

  private static HavingSpec EXPR_HAVING(final String expression)
  {
    return new ExpressionHavingSpec(expression);
  }

  private static VirtualColumn EXPR_VC(
      final String name,
      final String expression
  )
  {
    return new ExprVirtualColumn(expression, name);
  }

  private static MathPostAggregator EXPR_POST_AGG(final String name, final String expression)
  {
    return new MathPostAggregator(name, expression);
  }

  private static Druids.SelectQueryBuilder newScanQueryBuilder()
  {
    return new Druids.SelectQueryBuilder();
  }
}
