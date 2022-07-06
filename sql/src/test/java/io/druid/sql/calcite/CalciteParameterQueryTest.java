package io.druid.sql.calcite;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import io.druid.common.guava.Files;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * from apache druid, CalciteParameterQueryTest
 */
public class CalciteParameterQueryTest extends CalciteQueryTestHelper
{
  private static MiscQueryHook hook;
  private static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    hook = new MiscQueryHook();
    walker = CalciteTests.createMockWalker(Files.createTempDir()).withQueryHook(hook);
    walker.getQueryConfig().getJoin().setSemiJoinThreshold(-1);
    walker.getQueryConfig().getJoin().setBroadcastJoinThreshold(-1);
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
  public void testSelectConstantParamGetsConstant() throws Exception
  {
    testQuery(
        "SELECT 1 + ?",
        Arrays.asList(1),
        new Object[]{2}
    );
  }

  @Test
  public void testParamsGetOptimizedIntoConstant() throws Exception
  {
    testQuery(
        "SELECT 1 + ?, dim1 FROM foo LIMIT ?",
        Arrays.asList(1, 1),
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "(1 + 1)"))
            .columns("v0", "dim1")
            .limit(1)
            .streaming(),
        new Object[]{2, ""}
    );
  }

  @Test
  public void testParametersInSelectAndFilter() throws Exception
  {
    testQuery(
        "SELECT exp(count(*)) + ?, sum(m2) FROM druid.foo WHERE dim2 = ?",
        Arrays.asList(10, ""),
        newTimeseries()
            .dataSource(CalciteTests.DATASOURCE1)
            .filters(EXPR_FILTER("(dim2 == '')"))
            .aggregators(new CountAggregatorFactory("a0"), new GenericSumAggregatorFactory("a1", "m2", ValueDesc.DOUBLE))
            .postAggregators(
                EXPR_POST_AGG("p0", "(exp(a0) + 10)")
            )
            .outputColumns("p0", "a1")
            .build(),
        new Object[]{30.085536923187668, 11.0}
    );
  }

  @Test
  public void testParamsInInformationSchema() throws Exception
  {
    // Not including COUNT DISTINCT, since it isn't supported by BindableAggregate, and so it can't work.
    testQuery(
        "SELECT\n"
        + "  COUNT(JDBC_TYPE),\n"
        + "  SUM(JDBC_TYPE),\n"
        + "  AVG(JDBC_TYPE),\n"
        + "  MIN(JDBC_TYPE),\n"
        + "  MAX(JDBC_TYPE)\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        Arrays.asList("druid", "foo"),
        new Object[]{7L, 1239L, 177L, -5L, 1111L}
    );
  }

  @Test
  public void testParamsInSelectExpressionAndLimit() throws Exception
  {
    testQuery(
        "SELECT SUBSTRING(dim2, ?, ?) FROM druid.foo LIMIT ?",
        Arrays.asList(1, 1, 2),
        newScan()
            .dataSource(CalciteTests.DATASOURCE1)
            .virtualColumns(EXPR_VC("v0", "substring(dim2, 0, 1)"))
            .columns("v0")
            .limit(2)
            .streaming(),
        new Object[]{"a"},
        new Object[]{""}
    );
  }

  @Test
  public void testParamsTuckedInACast() throws Exception
  {
    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - CAST(? as INT) = cast(dim1 as DOUBLE) GROUP BY dim1, m1",
        Arrays.asList(1),
        newGroupBy().setDataSource(CalciteTests.DATASOURCE1)
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
  public void testParametersInStrangePlaces() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  COUNT(*) FILTER(WHERE dim2 <> ?)/COUNT(*) as ratio\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "HAVING COUNT(*) FILTER(WHERE dim2 <> ?)/COUNT(*) = ?",
        Arrays.asList("a", "a", 1),
        new Object[]{"10.1", 1L},
        new Object[]{"2", 1L},
        new Object[]{"abc", 1L},
        new Object[]{"def", 1L}
    );
    hook.verifyHooked(
        "0uSWXkiMoUwKrd5XyL28dQ==",
        "GroupByQuery{dataSource='foo', dimensions=[DefaultDimensionSpec{dimension='dim1', outputName='d0'}], aggregatorSpecs=["
        + "FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=MathExprFilter{expression='(dim2 != 'a')'}}, "
        + "CountAggregatorFactory{name='a1'}, "
        + "FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a2'}, filter=MathExprFilter{expression='(dim2 != 'a')'}}], "
        + "postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], "
        + "havingSpec=ExpressionHavingSpec{expression='((a2 / a1) == 1)'}, "
        + "outputColumns=[d0, p0]}"
    );
  }
}
