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

import static io.druid.sql.calcite.SsbTest.SSB2_1;
import static io.druid.sql.calcite.SsbTest.SSB2_1_PLAN_JR;
import static io.druid.sql.calcite.SsbTest.SSB2_1_PLAN_JR2;
import static io.druid.sql.calcite.SsbTest.SSB2_1_RESULT;
import static io.druid.sql.calcite.SsbTest.SSB2_2;
import static io.druid.sql.calcite.SsbTest.SSB2_2_PLAN_JR;
import static io.druid.sql.calcite.SsbTest.SSB2_2_PLAN_JR2;
import static io.druid.sql.calcite.SsbTest.SSB2_2_RESULT;
import static io.druid.sql.calcite.SsbTest.SSB3_1;
import static io.druid.sql.calcite.SsbTest.SSB3_1_PLAN_JR;
import static io.druid.sql.calcite.SsbTest.SSB3_1_PLAN_JR2;
import static io.druid.sql.calcite.SsbTest.SSB3_1_RESULT;
import static io.druid.sql.calcite.SsbTest.SSB3_2;
import static io.druid.sql.calcite.SsbTest.SSB3_2_PLAN_JR;
import static io.druid.sql.calcite.SsbTest.SSB3_2_PLAN_JR2;
import static io.druid.sql.calcite.SsbTest.SSB3_2_RESULT;
import static io.druid.sql.calcite.SsbTest.SSB4_1;
import static io.druid.sql.calcite.SsbTest.SSB4_1_PLAN_JR;
import static io.druid.sql.calcite.SsbTest.SSB4_1_PLAN_JR2;
import static io.druid.sql.calcite.SsbTest.SSB4_1_RESULT;
import static io.druid.sql.calcite.SsbTest.SSB4_2;
import static io.druid.sql.calcite.SsbTest.SSB4_2_PLAN_JR;
import static io.druid.sql.calcite.SsbTest.SSB4_2_PLAN_JR2;
import static io.druid.sql.calcite.SsbTest.SSB4_2_RESULT;
import static io.druid.sql.calcite.SsbTest.SSB4_3;
import static io.druid.sql.calcite.SsbTest.SSB4_3_PLAN;
import static io.druid.sql.calcite.SsbTest.SSB4_3_RESULT;

public class SsbTestJoinReorder extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static final TestQuerySegmentWalker walker = SsbTestHelper.walker.duplicate().withQueryHook(hook);

  public SsbTestJoinReorder()
  {
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
  public void ssb1_1() throws Exception
  {
    testQuery(
        JOIN_REORDERING,
        "SELECT sum(LO_EXTENDEDPRICE*LO_DISCOUNT) as revenue"
        + " FROM ssb_lineorder, ssb_date"
        + " WHERE ssb_lineorder.__time = ssb_date.__time AND"
        + "       D_YEAR = 1992 AND"
        + "       LO_DISCOUNT between 1 and 3 AND"
        + "       LO_QUANTITY < 25"
        ,
        "DruidOuterQueryRel(scanProject=[*($0, $1)], revenue=[SUM($0)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
        + "    DruidQueryRel(table=[druid.ssb_lineorder], scanFilter=[AND(SEARCH($2, Sarg[[1..3]]), <($9, 25))], scanProject=[$2, $3, $16])\n"
        + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[=($13, 1992)], scanProject=[$16])\n"
        ,
        new Object[]{7.4425744E7}
    );

    hook.verifyHooked(
        "W+I5FGhL3ogRs9wnkQyGGg==",
        "StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time]}",
        "TimeseriesQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && BoundDimFilter{LO_QUANTITY < 25(numeric)} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_EXTENDEDPRICE, LO_DISCOUNT]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_EXTENDEDPRICE * LO_DISCOUNT)', inputType='double'}], outputColumns=[a0]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && BoundDimFilter{LO_QUANTITY < 25(numeric)} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_EXTENDEDPRICE, LO_DISCOUNT]}"
    );
  }

  @Test
  public void ssb2_1() throws Exception
  {
    testQuery(JOIN_REORDERING, SSB2_1, SSB2_1_PLAN_JR, SSB2_1_RESULT);

    hook.verifyHooked(
        "kU/7no752Wntc7LccNJ90g==",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY]}",
        "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_part, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_part, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb2_1S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, SSB2_1, SSB2_1_PLAN_JR2, SSB2_1_RESULT);

    hook.verifyHooked(
        "pCzqW4nDH1eE0haGjeHGWQ==",
        "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_part+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_part+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb2_2() throws Exception
  {
    testQuery(JOIN_REORDERING, SSB2_2, SSB2_2_PLAN_JR, SSB2_2_RESULT);

    hook.verifyHooked(
        "CKYcEobMwoz4bUXCqJmqOQ==",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
        "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_part, leftJoinColumns=[P_PARTKEY], rightAlias=ssb_lineorder+ssb_supplier+ssb_date, rightJoinColumns=[LO_PARTKEY]}, hashLeft=true, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_part, leftJoinColumns=[P_PARTKEY], rightAlias=ssb_lineorder+ssb_supplier+ssb_date, rightJoinColumns=[LO_PARTKEY]}, hashLeft=true, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}]}"
    );
  }

  @Test
  public void ssb2_2S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, SSB2_2, SSB2_2_PLAN_JR2, SSB2_2_RESULT);

    hook.verifyHooked(
        "nM8cKcCAHXDQpCpvwAvK6Q==",
        "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_part+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:string, P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_part+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb3_1() throws Exception
  {
    testQuery(JOIN_REORDERING, SSB3_1, SSB3_1_PLAN_JR, SSB3_1_RESULT);

    hook.verifyHooked(
        "L7OUCFe5+sWlUlswOXrWnQ==",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
        "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='C_NATION', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb3_1S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, SSB3_1, SSB3_1_PLAN_JR2, SSB3_1_RESULT);

    hook.verifyHooked(
        "htWyISHrota8hwqaJyrFWQ==",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='C_NATION', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb3_2() throws Exception
  {
    testQuery(JOIN_REORDERING, SSB3_2, SSB3_2_PLAN_JR, SSB3_2_RESULT);

    hook.verifyHooked(
        "AAYkirQQ3igXMI1CM80oQQ==",
        "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY]}",
        "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_CITY:string, S_SUPPKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CITY:string, C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='C_CITY', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_CITY:string, S_SUPPKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CITY:string, C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb3_2S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, SSB3_2, SSB3_2_PLAN_JR2, SSB3_2_RESULT);

    hook.verifyHooked(
        "nMIEwNgujPFQRJr0FFqmDw==",
        "StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CITY:string, C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_CITY:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='C_CITY', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CITY:string, C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_CITY:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb4_1() throws Exception
  {
    testQuery(JOIN_REORDERING, SSB4_1, SSB4_1_PLAN_JR, SSB4_1_RESULT);

    hook.verifyHooked(
        "TvU19dhxaFKGWdugshr71Q==",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
        "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
        "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb4_1S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, SSB4_1, SSB4_1_PLAN_JR2, SSB4_1_RESULT);

    hook.verifyHooked(
        "9de8UGOEa7uwjBSsjtwAAA==",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
        "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string, C_NATION:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb4_2() throws Exception
  {
    testQuery(JOIN_REORDERING, SSB4_2, SSB4_2_PLAN_JR, SSB4_2_RESULT);

    hook.verifyHooked(
        "zpMOFMSLnhrXJgzGa1bY4w==",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
        "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
        "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}",
        "GroupByQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[ssb_part]}, StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_supplier+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb4_2S() throws Exception
  {
    testQuery(JOIN_REORDERING_WITH_SELECTIVITY, SSB4_2, SSB4_2_PLAN_JR2, SSB4_2_RESULT);

    hook.verifyHooked(
        "nVCxQZjv2b2c4g3jEh0/Wg==",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY]}",
        "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
        "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}",
        "GroupByQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[ssb_part]}, StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }

  @Test
  public void ssb4_3() throws Exception
  {
    testQuery(SSB4_3, SSB4_3_PLAN, SSB4_3_RESULT);

    hook.verifyHooked(
        "9bLXYX+anh0TfJZB33U0wA==",
        "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
        "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
        "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}",
        "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[14, 188, 239, 255, 295, 302, 4, 414, 42, 455, ..23 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[14, 188, 239, 255, 295, 302, 4, 414, 42, 455, ..23 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
        "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
        "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:string, __time:long}}]}"
    );
  }
}
