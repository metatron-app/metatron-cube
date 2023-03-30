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

import io.druid.data.Pair;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

public class OtherTest extends CalciteQueryTestHelper
{
  protected static final MiscQueryHook hook = new MiscQueryHook();
  private static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker = TestHelper.profileWalker.duplicate().withQueryHook(hook);
    walker.addIndex("b", "b", IntStream.range(0, 2));
  }

  @Override
  protected <T extends Throwable> Pair<String, List<Object[]>> failed(T ex) throws T
  {
    hook.printHooked();
    throw ex;
  }

  @Before
  public void before()
  {
    hook.clear();
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void testSelect() throws Exception
  {
    testQuery(
        "SELECT unwrap(st11_cat) FROM profile",
        new Object[]{Arrays.asList(3, 7, 18)},
        new Object[]{null},
        new Object[]{Arrays.asList(3, 9, 14)},
        new Object[]{Arrays.asList(6, 14, 15, 16, 17)},
        new Object[]{Arrays.asList(1, 4, 14, 18)}
    );
    testQuery(
        "SELECT unwrap(st11_cat) FROM profile where st11_cat IS NOT NULL",
        new Object[]{Arrays.asList(3, 7, 18)},
        new Object[]{Arrays.asList(3, 9, 14)},
        new Object[]{Arrays.asList(6, 14, 15, 16, 17)},
        new Object[]{Arrays.asList(1, 4, 14, 18)}
    );
    testQuery(
        "SELECT st11_cat[3], st11_cat[18] FROM profile",
        new Object[]{true, true},
        new Object[]{null, null},
        new Object[]{true, false},
        new Object[]{false, false},
        new Object[]{false, true}
    );
    testQuery(
        "SELECT svc_mgmt_num, age FROM profile where st11_cat[3]",
        new Object[]{"s:1", "23"},
        new Object[]{"s:3", "44"}
    );
    testQuery(
        "SELECT cardinality(st11_cat), unwrap(st11_cat) FROM profile",
        new Object[]{3L, Arrays.asList(3, 7, 18)},
        new Object[]{0L, null},
        new Object[]{3L, Arrays.asList(3, 9, 14)},
        new Object[]{5L, Arrays.asList(6, 14, 15, 16, 17)},
        new Object[]{4L, Arrays.asList(1, 4, 14, 18)}
    );
    testQuery(
        "SELECT cardinality(st11_cat), unwrap(st11_cat) FROM profile WHERE _bs(st11_cat) = '3'",
        new Object[]{3L, Arrays.asList(3, 7, 18)},
        new Object[]{3L, Arrays.asList(3, 9, 14)}
    );
    testQuery(
        "SELECT cardinality(st11_cat), unwrap(st11_cat) FROM profile WHERE _bs(st11_cat) IN ('3', '6', '9')",
        new Object[]{3L, Arrays.asList(3, 7, 18)},
        new Object[]{3L, Arrays.asList(3, 9, 14)},
        new Object[]{5L, Arrays.asList(6, 14, 15, 16, 17)}
    );
  }

  @Test
  public void testGroupBy() throws Exception
  {
    testQuery(
        "SELECT _bs(st11_cat), count(*) as c FROM profile group by _bs(st11_cat) order by c desc limit 5",
        new Object[]{"14", 3L},
        new Object[]{"18", 2L},
        new Object[]{"3", 2L},
        new Object[]{"15", 1L},
        new Object[]{"9", 1L}
    );
  }

  private static BitSet newBitSet(int... ixs)
  {
    final BitSet set = new BitSet();
    for (int ix : ixs) {
      set.set(ix);
    }
    return set;
  }

  @Test
  public void testX() throws Exception
  {
    Object[][] expected = {
        {"2023-02-05T00:10:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T00:20:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T00:30:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T00:40:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T00:50:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T01:00:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T01:10:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T01:20:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T01:30:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T01:40:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T01:50:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T02:00:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T02:10:00.000Z", "FDC_1.0", 4.0D, 4.0D},
        {"2023-02-05T02:20:00.000Z", "FDC_1.0", 4.0D, 4.0D}
    };

    String sql1 =
        "SELECT event_time, modelType, MAX(score) AS score, MAX(smoothingScore) AS smoothingScore FROM" +
        "(" +
        "  SELECT" +
        "    TIMESTAMP_FORMAT(TIME_CEIL(__time, 'PT10M'), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', 'UTC') AS event_time," +
        "    AVG(score) AS score," +
        "    AVG(smoothingScore) AS smoothingScore," +
        "    modelType" +
        "  FROM b" +
        "  WHERE __time >= '2023-02-02 23:16:37' AND __time < '2023-02-05 02:16:37' AND assetId = 'CVD_8G_PM_CH'" +
        "  GROUP BY modelType, __time" +
        ") AS x " +
        "GROUP BY event_time, modelType ORDER BY 1";
    testQuery(sql1, expected);
    hook.verifyHooked(
        "9r2jbaAKCIovoCPcezSiiQ==",
        "GroupByQuery{dataSource='GroupByQuery{dataSource='b', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[2023-02-02T23:16:37.000Z/2023-02-05T02:16:37.000Z]}, dimensions=[DefaultDimensionSpec{dimension='modelType', outputName='d0'}, DefaultDimensionSpec{dimension='__time', outputName='d1'}], filter=assetId=='CVD_8G_PM_CH', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='score', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='score'}, GenericSumAggregatorFactory{name='a1:sum', fieldName='smoothingScore', inputType='double'}, CountAggregatorFactory{name='a1:count', fieldName='smoothingScore'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, ArithmeticPostAggregator{name='a1', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a1:sum'}, FieldAccessPostAggregator{fieldName='a1:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='TIMESTAMP_FORMAT(timestamp_ceil(d1,'PT10M','','UTC'),'yyyy-MM-dd\\u0027T\\u0027HH:mm:ss.SSS\\u0027Z\\u0027','UTC')', finalize=true}], outputColumns=[p0, d0, a0, a1]}', dimensions=[DefaultDimensionSpec{dimension='p0', outputName='_d0'}, DefaultDimensionSpec{dimension='d0', outputName='_d1'}], aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}, GenericMaxAggregatorFactory{name='_a1', fieldName='a1', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _a0, _a1]}",
        "GroupByQuery{dataSource='b', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[2023-02-02T23:16:37.000Z/2023-02-05T02:16:37.000Z]}, dimensions=[DefaultDimensionSpec{dimension='modelType', outputName='d0'}, DefaultDimensionSpec{dimension='__time', outputName='d1'}], filter=assetId=='CVD_8G_PM_CH', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='score', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='score'}, GenericSumAggregatorFactory{name='a1:sum', fieldName='smoothingScore', inputType='double'}, CountAggregatorFactory{name='a1:count', fieldName='smoothingScore'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, ArithmeticPostAggregator{name='a1', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a1:sum'}, FieldAccessPostAggregator{fieldName='a1:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='TIMESTAMP_FORMAT(timestamp_ceil(d1,'PT10M','','UTC'),'yyyy-MM-dd\\u0027T\\u0027HH:mm:ss.SSS\\u0027Z\\u0027','UTC')', finalize=true}], outputColumns=[p0, d0, a0, a1]}"
    );

    String sql2 =
        "SELECT event_time, modelType, MAX(score) AS score, MAX(smoothingScore) AS smoothingScore FROM" +
        "(" +
        "  SELECT" +
        "    TIMESTAMP_FORMAT(TIME_CEIL(__time, 'PT10M'), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', 'UTC') AS event_time," +
        "    modelType, " +
        "    AVG(score) AS score," +
        "    AVG(smoothingScore) AS smoothingScore" +
        "  FROM b" +
        "  WHERE __time >= '2023-02-02 23:16:37' AND __time < '2023-02-05 02:16:37' AND assetId = 'CVD_8G_PM_CH'" +
        "  GROUP BY 1, 2" +
        ") AS x " +
        "GROUP BY 1, 2 ORDER BY 1";
    testQuery(sql2, expected);
    hook.verifyHooked(
        "nvS6sDScwnDNBRX17j7caw==",
        "GroupByQuery{dataSource='b', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[2023-02-02T23:16:37.000Z/2023-02-05T02:16:37.000Z]}, dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}, DefaultDimensionSpec{dimension='modelType', outputName='d1'}], filter=assetId=='CVD_8G_PM_CH', virtualColumns=[ExprVirtualColumn{expression='TIMESTAMP_FORMAT(timestamp_ceil(__time,'PT10M','','UTC'),'yyyy-MM-dd\\u0027T\\u0027HH:mm:ss.SSS\\u0027Z\\u0027','UTC')', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='score', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='score'}, GenericSumAggregatorFactory{name='a1:sum', fieldName='smoothingScore', inputType='double'}, CountAggregatorFactory{name='a1:count', fieldName='smoothingScore'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, ArithmeticPostAggregator{name='a1', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a1:sum'}, FieldAccessPostAggregator{fieldName='a1:count'}], op=QUOTIENT}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0, a1]}"
    );

    String sql3 =
        "SELECT TIMESTAMP_FORMAT(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', 'UTC'), modelType, score, smoothingScore FROM" +
        "(" +
        "  SELECT" +
        "    TIME_CEIL(__time, 'PT10M') AS event_time," +
        "    modelType, " +
        "    AVG(score) AS score," +
        "    AVG(smoothingScore) AS smoothingScore" +
        "  FROM b" +
        "  WHERE __time >= '2023-02-02 23:16:37' AND __time < '2023-02-05 02:16:37' AND assetId = 'CVD_8G_PM_CH'" +
        "  GROUP BY 1, 2" +
        ") AS x " +
        "ORDER BY event_time";
    testQuery(sql3, expected);
    hook.verifyHooked(
        "MrqnKQ7i6D2TpWUxtvwLKQ==",
        "GroupByQuery{dataSource='b', querySegmentSpec=MultipleIntervalSegmentSpec{intervals=[2023-02-02T23:16:37.000Z/2023-02-05T02:16:37.000Z]}, granularity=PeriodGranularity{period=PT10M, timeZone=UTC}, dimensions=[DefaultDimensionSpec{dimension='modelType', outputName='d1'}], filter=assetId=='CVD_8G_PM_CH', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='score', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='score'}, GenericSumAggregatorFactory{name='a1:sum', fieldName='smoothingScore', inputType='double'}, CountAggregatorFactory{name='a1:count', fieldName='smoothingScore'}], postAggregatorSpecs=[MathPostAggregator{name='d0', expression='timestamp_ceil(__time,'PT10M','','UTC')', finalize=true}, ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, ArithmeticPostAggregator{name='a1', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a1:sum'}, FieldAccessPostAggregator{fieldName='a1:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='TIMESTAMP_FORMAT(d0,'yyyy-MM-dd\\u0027T\\u0027HH:mm:ss.SSS\\u0027Z\\u0027','UTC')', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[p0, d1, a0, a1, d0]}"
    );
  }
}
