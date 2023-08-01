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

import io.druid.query.JoinQueryConfig;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Test;

public class TpchTestJoinReorder extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static final TestQuerySegmentWalker walker = TpchTestHelper.walker.duplicate().withQueryHook(hook);

  static {
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setSemiJoinThreshold(100000);
    join.setBroadcastJoinThreshold(51);     // supplier + 1
    join.setBloomFilterThreshold(100);
    join.setForcedFilterHugeThreshold(5000);
    join.setForcedFilterTinyThreshold(100);
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void tpch1() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH1, TpchTest.TPCH1_PLAN, TpchTest.TPCH1_RESULT);
  }

  @Test
  public void tpch2() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH2, TpchTest.TPCH2_EXPLAIN_JR, TpchTest.TPCH2_RESULT);
  }

  @Test
  public void tpch2S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH2, TpchTest.TPCH2_EXPLAIN_JR2, TpchTest.TPCH2_RESULT);
  }

  @Test
  public void tpch3() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH3, TpchTest.TPCH3_EXPLAIN_JR, TpchTest.TPCH3_RESULT);
  }

  @Test
  public void tpch3S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH3, TpchTest.TPCH3_EXPLAIN_JR, TpchTest.TPCH3_RESULT);
  }

  @Test
  public void tpch4() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH4, TpchTest.TPCH4_EXPLAIN, TpchTest.TPCH4_RESULT);
  }

  @Test
  public void tpch4S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH4, TpchTest.TPCH4_EXPLAIN, TpchTest.TPCH4_RESULT);
  }

  @Test
  public void tpch5() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH5, TpchTest.TPCH5_EXPLAIN_JR, TpchTest.TPCH5_RESULT);
  }

  @Test
  public void tpch5S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH5, TpchTest.TPCH5_EXPLAIN_JR2, TpchTest.TPCH5_RESULT);
  }

  @Test
  public void tpch6() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH6, TpchTest.TPCH6_EXPLAIN, TpchTest.TPCH6_RESULT);
  }

  @Test
  public void tpch6S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH6, TpchTest.TPCH6_EXPLAIN, TpchTest.TPCH6_RESULT);
  }

  @Test
  public void tpch7() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH7, TpchTest.TPCH7_EXPLAIN_JR, TpchTest.TPCH7_RESULT);
  }

  @Test
  public void tpch7S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH7, TpchTest.TPCH7_EXPLAIN_JR2, TpchTest.TPCH7_RESULT);
  }

  @Test
  public void tpch8() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH8, TpchTest.TPCH8_EXPLAIN_JR, TpchTest.TPCH8_RESULT);
  }

  @Test
  public void tpch8S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH8, TpchTest.TPCH8_EXPLAIN_JR2, TpchTest.TPCH8_RESULT);
  }

  @Test
  public void tpch9() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH9, TpchTest.TPCH9_EXPLAIN_JR, TpchTest.TPCH9_RESULT);
  }

  @Test
  public void tpch9S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH9, TpchTest.TPCH9_EXPLAIN_JR, TpchTest.TPCH9_RESULT);
  }

  @Test
  public void tpch10() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH10, TpchTest.TPCH10_EXPLAIN_JR, TpchTest.TPCH10_RESULT);
  }

  @Test
  public void tpch10S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH10, TpchTest.TPCH10_EXPLAIN_JR2, TpchTest.TPCH10_RESULT);
  }

  @Test
  public void tpch11() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH11, TpchTest.TPCH11_EXPLAIN_JR, TpchTest.TPCH11_RESULT);
  }

  @Test
  public void tpch11S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH11, TpchTest.TPCH11_EXPLAIN_JR, TpchTest.TPCH11_RESULT);
  }

  @Test
  public void tpch12() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH12, TpchTest.TPCH12_EXPLAIN, TpchTest.TPCH12_RESULT);
  }

  @Test
  public void tpch12S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH12, TpchTest.TPCH12_EXPLAIN, TpchTest.TPCH12_RESULT);
  }

  @Test
  public void tpch13() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH13, TpchTest.TPCH13_EXPLAIN, TpchTest.TPCH13_RESULT);
  }

  @Test
  public void tpch13S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH13, TpchTest.TPCH13_EXPLAIN, TpchTest.TPCH13_RESULT);
  }

  @Test
  public void tpch14() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH14, TpchTest.TPCH14_EXPLAIN, TpchTest.TPCH14_RESULT);
  }

  @Test
  public void tpch14S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH14, TpchTest.TPCH14_EXPLAIN, TpchTest.TPCH14_RESULT);
  }

  @Test
  public void tpch15() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH15, TpchTest.TPCH15_EXPLAIN_JR, TpchTest.TPCH15_RESULT);
  }

  @Test
  public void tpch15S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH15, TpchTest.TPCH15_EXPLAIN_JR, TpchTest.TPCH15_RESULT);
  }

  @Test
  public void tpch16() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH16, TpchTest.TPCH16_EXPLAIN_JR, TpchTest.TPCH16_RESULT);
  }

  @Test
  public void tpch16S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH16, TpchTest.TPCH16_EXPLAIN_JR, TpchTest.TPCH16_RESULT);
  }

  @Test
  public void tpch17() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH17, TpchTest.TPCH17_EXPLAIN_JR, TpchTest.TPCH17_RESULT);
  }

  @Test
  public void tpch17S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH17, TpchTest.TPCH17_EXPLAIN_JR, TpchTest.TPCH17_RESULT);
  }

  @Test
  public void tpch18() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH18, TpchTest.TPCH18_EXPLAIN_JR, TpchTest.TPCH18_RESULT);
  }

  @Test
  public void tpch18S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH18, TpchTest.TPCH18_EXPLAIN_JR, TpchTest.TPCH18_RESULT);
  }

  @Test
  public void tpch19() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH19, TpchTest.TPCH19_EXPLAIN, TpchTest.TPCH19_RESULT);
  }

  @Test
  public void tpch19S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH19, TpchTest.TPCH19_EXPLAIN, TpchTest.TPCH19_RESULT);
  }

  @Test
  public void tpch20() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH20, TpchTest.TPCH20_EXPLAIN_JR, TpchTest.TPCH20_RESULT);
  }

  @Test
  public void tpch20S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH20, TpchTest.TPCH20_EXPLAIN_JR2, TpchTest.TPCH20_RESULT);
  }

  @Test
  public void tpch21() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH21, TpchTest.TPCH21_EXPLAIN_JR, TpchTest.TPCH21_RESULT);
  }

  @Test
  public void tpch21S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH21, TpchTest.TPCH21_EXPLAIN_JR2, TpchTest.TPCH21_RESULT);
  }

  @Test
  public void tpch22() throws Exception
  {
    testQuery(JOIN_REORDERING, TpchTest.TPCH22, TpchTest.TPCH22_EXPLAIN_JR, TpchTest.TPCH22_RESULT);
  }

  @Test
  public void tpch22S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, TpchTest.TPCH22, TpchTest.TPCH22_EXPLAIN_JR, TpchTest.TPCH22_RESULT);
  }
}
