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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.server.QueryManager;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.ForbiddenException;
import io.druid.sql.SqlLifecycleFactory;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import io.druid.sql.calcite.util.CalciteTestBase;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.sql.calcite.view.InProcessViewManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Rule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class CalciteQueryTestHelper extends CalciteTestBase
{
  protected static final Logger log = new Logger(CalciteQueryTestHelper.class);

  protected static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();

  protected static final PlannerConfig PLANNER_CONFIG_NO_TOPN = new PlannerConfig()
  {
    @Override
    public int getMaxTopNLimit() { return 0;}
  };

  protected static final PlannerConfig PLANNER_CONFIG_NO_HLL = new PlannerConfig()
  {
    @Override
    public boolean isUseApproximateCountDistinct() { return false;}
  };

  protected static final PlannerConfig PLANNER_CONFIG_SINGLE_NESTING_ONLY = new PlannerConfig()
  {
    @Override
    public int getMaxQueryCount() { return 2;}
  };

  protected static final PlannerConfig PLANNER_CONFIG_NO_SUBQUERIES = new PlannerConfig()
  {
    @Override
    public int getMaxQueryCount() { return 1;}
  };

  protected static final PlannerConfig PLANNER_CONFIG_JOIN_ENABLED = new PlannerConfig()
  {
    @Override
    public boolean isJoinEnabled() { return true;}

    @Override
    public int getMaxSemiJoinRowsInMemory() { return -1;}
  };

  protected static final PlannerConfig PLANNER_CONFIG_REQUIRE_TIME_CONDITION = new PlannerConfig()
  {
    @Override
    public boolean isRequireTimeCondition() { return true;}
  };

  protected static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.<String, Object>of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      GroupByQuery.SORT_ON_TIME, false
  );

  protected static final String MASKED = "<<<<<<MASK>>>>>>";

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  protected abstract TestQuerySegmentWalker walker();

  protected void assertForbidden(String sql, AuthenticationResult authenticationResult)
  {
    assertForbidden(PLANNER_CONFIG_DEFAULT, sql, authenticationResult);
  }

  protected void assertForbidden(PlannerConfig plannerConfig, String sql, AuthenticationResult authenticationResult)
  {
    try {
      testQuery(
          plannerConfig,
          QUERY_CONTEXT_DEFAULT,
          sql,
          authenticationResult,
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Expected ForbiddenException");
    }
    catch (Exception e) {
      Assert.assertTrue(
          String.format("Expected ForbiddenException but %s", e), e instanceof ForbiddenException
      );
    }
  }

  protected void testQuery(String sql, String expectedQuery, Object[]... expectedResults) throws Exception
  {
    testQuery(sql, TestHelper.JSON_MAPPER.readValue(expectedQuery, Query.class), expectedResults);
  }

  protected void testQuery(String sql, Query expectedQuery, Object[]... expectedResults) throws Exception
  {
    testQuery(sql, Arrays.asList(expectedQuery), Arrays.asList(expectedResults));
  }

  protected void testQuery(
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

  protected void testQuery(PlannerConfig plannerConfig, String sql, String expectedQuery, Object[]... expectedResults)
      throws Exception
  {
    testQuery(plannerConfig, sql, TestHelper.JSON_MAPPER.readValue(expectedQuery, Query.class), expectedResults);
  }

  protected void testQuery(PlannerConfig plannerConfig, String sql, Query expectedQuery, Object[]... expectedResults)
      throws Exception
  {
    testQuery(plannerConfig, sql, Arrays.asList(expectedQuery), Arrays.asList(expectedResults));
  }

  protected void testQuery(
      final PlannerConfig plannerConfig,
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    testQuery(
        plannerConfig,
        QUERY_CONTEXT_DEFAULT,
        sql,
        expectedQueries,
        expectedResults
    );
  }

  protected void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    testQuery(
        plannerConfig,
        queryContext,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        expectedResults
    );
  }

  protected void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    log.info("SQL: %s", sql);
    queryLogHook.clearRecordedQueries();
    final List<Object[]> plannerResults = getResults(plannerConfig, queryContext, sql, authenticationResult);
    verifyResults(sql, expectedQueries, expectedResults, plannerResults);
  }

  public List<Object[]> getResults(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final AuthenticationResult authenticationResult
  ) throws Exception
  {
    final TestQuerySegmentWalker walker = walker();
    final InProcessViewManager viewManager = new InProcessViewManager(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
    final DruidSchema druidSchema = CalciteTests.createMockSchema(walker, plannerConfig, viewManager);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker);
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();

    final PlannerFactory plannerFactory = new PlannerFactory(
        druidSchema,
        systemSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker),
        walker,
        new QueryManager(),
        operatorTable,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        plannerConfig,
        new QueryConfig(),
        CalciteTests.getJsonMapper()
    );
    final SqlLifecycleFactory sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(plannerFactory);

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

    try (DruidPlanner planner = plannerFactory.createPlanner(queryContext, authenticationResult)) {
      final PlannerResult plan = planner.plan(sql, null);
      List<Object[]> results = Sequences.toList(plan.run(), Lists.newArrayList());
      log.info("result schema " + plan.rowType());
      return results;
    }
  }

  protected void verifyResults(
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

    int compareTo = Math.min(expectedResults.size(), results.size());
    for (int i = 0; i < compareTo; i++) {
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
    Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResults.size(), results.size());

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
            recordedQueries.get(i)
        );
      }
    }
  }

  // Generate timestamps for expected results
  protected static long T(final String timeString)
  {
    return Calcites.jodaToCalciteTimestamp(DateTimes.of(timeString), DateTimeZone.UTC);
  }

  // Generate timestamps for expected results
  protected static long T(final String timeString, final String timeZoneString)
  {
    final DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    return Calcites.jodaToCalciteTimestamp(new DateTime(timeString, timeZone), timeZone);
  }

  // Generate day numbers for expected results
  protected static int D(final String dayString)
  {
    return (int) (Intervals.utc(T("1970"), T(dayString)).toDurationMillis() / (86400L * 1000L));
  }

  protected static QuerySegmentSpec QSS(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  protected static AndDimFilter AND(DimFilter... filters)
  {
    return new AndDimFilter(Arrays.asList(filters));
  }

  protected static OrDimFilter OR(DimFilter... filters)
  {
    return new OrDimFilter(Arrays.asList(filters));
  }

  protected static NotDimFilter NOT(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  protected static InDimFilter IN(String dimension, String... values)
  {
    return new InDimFilter(dimension, Arrays.asList(values), null);
  }

  protected static InDimFilter IN(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  protected static SelectorDimFilter SELECTOR(final String fieldName, final String value, final ExtractionFn extractionFn)
  {
    return new SelectorDimFilter(fieldName, value, extractionFn);
  }

  protected static DimFilter EXPR_FILTER(final String expression)
  {
    return new MathExprFilter(expression);
  }

  protected static BoundDimFilter BOUND(
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

  protected static BoundDimFilter TIME_BOUND(final Object intervalObj)
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

  protected static CascadeExtractionFn CASCADE(final ExtractionFn... fns)
  {
    return new CascadeExtractionFn(Arrays.asList(fns));
  }

  protected static List<DimensionSpec> DIMS(final DimensionSpec... dimensionSpecs)
  {
    return Arrays.asList(dimensionSpecs);
  }

  protected static HavingSpec EXPR_HAVING(final String expression)
  {
    return new ExpressionHavingSpec(expression);
  }

  protected static VirtualColumn EXPR_VC(final String name, final String expression)
  {
    return new ExprVirtualColumn(expression, name);
  }

  protected static MathPostAggregator EXPR_POST_AGG(final String name, final String expression)
  {
    return new MathPostAggregator(name, expression);
  }

  protected static Druids.SelectQueryBuilder newScan()
  {
    return new Druids.SelectQueryBuilder();
  }

  protected static GroupByQuery.Builder newGroupBy()
  {
    return GroupByQuery.builder();
  }
}
