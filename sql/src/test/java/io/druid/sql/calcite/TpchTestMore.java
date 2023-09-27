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

import java.util.concurrent.Executors;

public class TpchTestMore extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static final TestQuerySegmentWalker walker =
      TpchTestHelper.walker.duplicate()
                           .withExecutor(Executors.newFixedThreadPool(4))
                           .withQueryHook(hook);

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  private void p3542()
  {
    // left broadcast + sort (see StreamQuery.getMergeOrdering)
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setHashJoinThreshold(-1);
    join.setSemiJoinThreshold(-1);
    join.setBroadcastJoinThreshold(51);
    join.setBloomFilterThreshold(-1);
    join.setForcedFilterHugeThreshold(5000);
    join.setForcedFilterTinyThreshold(100);
  }

  @Test
  public void test3542_7() throws Exception
  {
    p3542();
    testQuery(
        TpchTest.TPCH7,
        TpchTest.TPCH7_EXPLAIN,
        TpchTest.TPCH7_RESULT
    );
    hook.verifyHooked(
        "qLL36H5ByBh5GsSTO6IGrA==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='v0', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}}",
        "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3542_8() throws Exception
  {
    p3542();
    testQuery(
        TpchTest.TPCH8,
        TpchTest.TPCH8_EXPLAIN,
        TpchTest.TPCH8_RESULT
    );
    hook.verifyHooked(
        "pZ1A2nv5YcU8cT10KWkbEQ==",
        "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='v0', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0.0D)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
        "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}]}",
        "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3542_9() throws Exception
  {
    p3542();
    testQuery(
        TpchTest.TPCH9,
        TpchTest.TPCH9_EXPLAIN,
        TpchTest.TPCH9_RESULT
    );
    hook.verifyHooked(
        "YHJUKfWRtPG5G4sl7kPsJA==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+supplier, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}]}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='v0', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+supplier, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}]}",
        "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}"
    );
  }

  private JoinQueryConfig p3798()
  {
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setHashJoinThreshold(-1);
    join.setSemiJoinThreshold(-1);
    join.setBroadcastJoinThreshold(-1);
    join.setBloomFilterThreshold(-1);
    join.setForcedFilterHugeThreshold(5000);
    join.setForcedFilterTinyThreshold(100);
    return join;
  }

  @Test
  public void test3798_8() throws Exception
  {
    p3798();
    testQuery(
        TpchTest.TPCH8,
        TpchTest.TPCH8_EXPLAIN,
        TpchTest.TPCH8_RESULT
    );
    hook.verifyHooked(
        "IW6vwZODKYdnYKHx7Mtk6w==",
        "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], orderingSpecs=[OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=ForcedFilter{fieldNames=[L_PARTKEY], valuesLen=7}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='v0', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0.0D)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
        "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3798_8B() throws Exception
  {
    p3798().setBroadcastJoinThreshold(100);
    testQuery(
        TpchTest.TPCH8,
        TpchTest.TPCH8_EXPLAIN,
        TpchTest.TPCH8_RESULT
    );
    hook.verifyHooked(
        "pZ1A2nv5YcU8cT10KWkbEQ==",
        "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='v0', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0.0D)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
        "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}]}",
        "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3798_9() throws Exception
  {
    p3798();
    testQuery(
        TpchTest.TPCH9,
        TpchTest.TPCH9_EXPLAIN,
        TpchTest.TPCH9_RESULT
    );
    hook.verifyHooked(
        "90uhu2RlKGTGOrRpIRoZdA==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=ForcedFilter{fieldNames=[L_SUPPKEY], valuesLen=50}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY], orderingSpecs=[OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='v0', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}]}",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY], orderingSpecs=[OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3798_9B() throws Exception
  {
    p3798().setBroadcastJoinThreshold(100);
    testQuery(
        TpchTest.TPCH9,
        TpchTest.TPCH9_EXPLAIN,
        TpchTest.TPCH9_RESULT
    );
    hook.verifyHooked(
        "YHJUKfWRtPG5G4sl7kPsJA==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+supplier, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}]}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='v0', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+supplier, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:string}, applyFilter=true}]}",
        "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3852() throws Exception
  {
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setSemiJoinThreshold(100000);
    join.setBroadcastJoinThreshold(51);
    join.setForcedFilterHugeThreshold(10000);
    join.setForcedFilterTinyThreshold(100);

    testQuery(
        TpchTest.TPCH9,
        TpchTest.TPCH9_EXPLAIN,
        TpchTest.TPCH9_RESULT
    );
    hook.verifyHooked(
        "5g+amTry7Xe6JAcUCj5Y+g==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
        "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}, $hash=true}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, StreamQuery{dataSource='partsupp', filter=InDimsFilter{dimensions=[PS_SUPPKEY, PS_PARTKEY], values=[[1, 722], [1, 773], [1, 800], [10, 194], [10, 209], [10, 275], [10, 467], [10, 659], [10, 733], [10, 966], [..176 more]]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='v0', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='partsupp', filter=InDimsFilter{dimensions=[PS_SUPPKEY, PS_PARTKEY], values=[[1, 722], [1, 773], [1, 800], [10, 194], [10, 209], [10, 275], [10, 467], [10, 659], [10, 733], [10, 966], [..176 more]]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
        "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
    );
  }

  @Test
  public void test4312() throws Exception
  {
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setSemiJoinThreshold(100000);
    join.setBroadcastJoinThreshold(51);
    join.setForcedFilterHugeThreshold(5000);    // applies forced filter from hashing alias
    join.setForcedFilterTinyThreshold(100);

    testQuery(
        TpchTest.TPCH9,
        TpchTest.TPCH9_EXPLAIN,
        TpchTest.TPCH9_RESULT
    );
    hook.verifyHooked(
        "WBAN0sIcjLbOwmMV+ZDp7g==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
        "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:string, S_SUPPKEY:string}}, $hash=true}",
        "StreamQuery{dataSource='partsupp', filter=InDimsFilter{dimensions=[PS_SUPPKEY, PS_PARTKEY], values=[[1, 722], [1, 773], [1, 800], [10, 194], [10, 209], [10, 275], [10, 467], [10, 659], [10, 733], [10, 966], [..176 more]]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem, partsupp]}, StreamQuery{dataSource='orders', filter=ForcedFilter{fieldNames=[O_ORDERKEY], valuesLen=1664}, columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='v0', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='orders', filter=InDimFilter{dimension='O_ORDERKEY', values=[10052, 10080, 10084, 10114, 10116, 10150, 10208, 10242, 10245, 1027, ..1325 more]}, columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
    );
  }

  @Test
  public void test3947() throws Exception
  {
    //9665|442|3|1|11|14766.84|0.06|0.00|R|F|1994-05-11|1994-06-28|1994-06-04|TAKE BACK RETURN|MAIL|y across the quickly even frays? fluffi|
    //9665|912|43|2|5|9064.55|0.02|0.04|A|F|1994-06-27|1994-06-12|1994-07-21|NONE|RAIL|ly ironic tithes |
    //9665|584|4|3|50|74229.00|0.02|0.04|R|F|1994-07-31|1994-07-12|1994-08-11|TAKE BACK RETURN|REG AIR| ironic deposits. final warhorses h|
    testQuery(
        "SELECT L_ORDERKEY, COUNT(DISTINCT L_SUPPKEY) AS CNTSUPP"
        + "  FROM lineitem"
        + "  WHERE L_RECEIPTDATE > L_COMMITDATE AND L_ORDERKEY IS NOT NULL AND L_ORDERKEY=9665"
        + "  GROUP BY L_ORDERKEY",
        new Object[]{"9665", 2L}
    );
  }

  @Test
  public void test3983() throws Exception
  {
    // tpch6, revised
    String SQL =
        "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) as REVENUE FROM lineitem"
        + " WHERE L_SHIPDATE >= '1993-01-01' AND L_SHIPDATE < '1994-01-01'"
        + " AND L_DISCOUNT = 0.07"
        + " AND L_QUANTITY < 25";
    // testQuery(SQL);
    // "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(CAST(L_DISCOUNT, 'DOUBLE') == 0.07)'} && BoundDimFilter{L_QUANTITY < 25(numeric)} && BoundDimFilter{1993-01-01 <= L_SHIPDATE < 1994-01-01(lexicographic)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * L_DISCOUNT)', inputType='double'}], outputColumns=[a0]}",

    testQuery(SQL, new Object[] {215600.05411791173D});

    hook.verifyHooked(
        "AGJ6I8MbXY8+Epbbfhe4Cg==",
        "TimeseriesQuery{dataSource='lineitem', filter=(BoundDimFilter{1993-01-01 <= L_SHIPDATE < 1994-01-01} && MathExprFilter{expression='(L_DISCOUNT == 0.07F)'} && BoundDimFilter{L_QUANTITY < 25(numeric)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * 0.07F)', inputType='double'}], outputColumns=[a0]}"
    );
  }

  @Test
  public void test4007() throws Exception
  {
    // exact: 147
    testQuery("SELECT COUNT(DISTINCT L_LINENUMBER,L_RETURNFLAG,L_SHIPMODE) FROM lineitem", new Object[]{147L});
    hook.verifyHooked(
        "eQeBskc9KTlzE1vYMYPdrA==",
        "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='a0', fieldNames=[L_LINENUMBER, L_RETURNFLAG, L_SHIPMODE], groupingSets=Noop, byRow=true, round=true, b=11}], outputColumns=[a0]}"
    );

    // exact: 126
    testQuery("SELECT COUNT(DISTINCT L_LINENUMBER,L_RETURNFLAG,L_SHIPMODE) FROM lineitem WHERE L_SHIPMODE < 'T' ", new Object[]{129L});
    hook.verifyHooked(
        "v0D5JV07F6+I3eY3uG423Q==",
        "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{L_SHIPMODE < T}, aggregatorSpecs=[CardinalityAggregatorFactory{name='a0', fieldNames=[L_LINENUMBER, L_RETURNFLAG, L_SHIPMODE], groupingSets=Noop, byRow=true, round=true, b=11}], outputColumns=[a0]}"
    );
  }

  @Test
  public void test4129_1() throws Exception
  {
    testQuery("SELECT COUNT(*) FROM lineitem", new Object[]{30201L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE = L_COMMITDATE", new Object[]{263L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE != L_COMMITDATE", new Object[]{30201L - 263L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE > L_COMMITDATE", new Object[]{18965L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE >= L_COMMITDATE", new Object[]{18965L + 263L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE < L_COMMITDATE", new Object[]{30201L - (18965L + 263L)});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_RECEIPTDATE <= L_COMMITDATE", new Object[]{30201L - 18965L});

    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE = L_RECEIPTDATE", new Object[]{263L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE != L_RECEIPTDATE", new Object[]{30201L - 263L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE < L_RECEIPTDATE", new Object[]{18965L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE <= L_RECEIPTDATE", new Object[]{18965L + 263L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE > L_RECEIPTDATE", new Object[]{30201L - (18965L + 263L)});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_COMMITDATE >= L_RECEIPTDATE", new Object[]{30201L - 18965L});
  }

  @Test
  public void test4129_2() throws Exception
  {
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1", new Object[]{7500L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1 AND NOT(L_RECEIPTDATE != L_COMMITDATE)", new Object[]{65L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1 AND NOT(L_RECEIPTDATE = L_COMMITDATE)", new Object[]{7500L - 65L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1 AND NOT(L_RECEIPTDATE > L_COMMITDATE)", new Object[]{2697L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1 AND NOT(L_RECEIPTDATE <= L_COMMITDATE)", new Object[]{7500L - 2697L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1 AND NOT(L_RECEIPTDATE < L_COMMITDATE)", new Object[]{4868L});
    testQuery("SELECT COUNT(*) FROM lineitem WHERE L_LINENUMBER = 1 AND NOT(L_RECEIPTDATE >= L_COMMITDATE)", new Object[]{7500L - 4868L});
  }

  @Test
  public void test4178() throws Exception
  {
    // ascending
    Object[][] expected1 = {
        {"1992-02-22", 49L}, {"1992-04-04", 48L}, {"1992-04-12", 50L}, {"1992-04-18", 49L}, {"1992-05-08", 49L},
        {"1992-10-01", 50L}, {"1992-11-15", 48L}, {"1992-12-26", 48L}, {"1993-02-24", 48L}, {"1993-03-04", 50L},
        {"1993-03-20", 49L}, {"1993-03-26", 50L}, {"1993-03-26", 48L}, {"1993-05-20", 50L}, {"1993-06-17", 50L},
        {"1993-07-12", 50L}, {"1994-02-14", 50L}, {"1994-02-22", 48L}, {"1994-05-24", 50L}, {"1994-05-28", 49L},
        {"1994-06-19", 49L}, {"1994-08-03", 49L}, {"1994-08-15", 48L}, {"1994-08-17", 48L}, {"1994-08-29", 48L},
        {"1994-09-02", 48L}, {"1994-10-09", 49L}, {"1994-10-19", 50L}, {"1994-11-18", 49L}, {"1994-11-21", 49L},
        {"1994-11-22", 49L}, {"1995-01-22", 48L}, {"1995-02-20", 48L}, {"1995-03-02", 49L}, {"1995-03-18", 48L},
        {"1995-03-21", 48L}, {"1995-07-14", 50L}, {"1995-10-03", 49L}, {"1995-10-26", 49L}, {"1995-10-28", 50L}
    };
    testQuery(
        "SELECT TIMESTAMP_FORMAT(__time, 'yyyy-MM-dd'), L_QUANTITY FROM lineitem" +
        "   WHERE L_LINENUMBER = 1 AND L_QUANTITY >= 48 AND L_SHIPMODE = 'TRUCK'" +
        "   ORDER BY __time LIMIT 40",
        expected1
    );

    // descending
    Object[][] expected2 = {
        {"1998-09-04", 49L}, {"1998-08-26", 49L}, {"1998-07-24", 49L}, {"1998-07-20", 49L}, {"1998-07-16", 49L},
        {"1998-05-26", 49L}, {"1998-03-04", 48L}, {"1998-02-02", 49L}, {"1997-12-29", 48L}, {"1997-12-26", 48L},
        {"1997-11-16", 50L}, {"1997-10-15", 50L}, {"1997-10-09", 49L}, {"1997-10-03", 49L}, {"1997-09-27", 50L},
        {"1997-08-22", 48L}, {"1997-08-16", 49L}, {"1997-05-05", 50L}, {"1997-04-04", 50L}, {"1997-03-19", 48L},
        {"1997-03-09", 50L}, {"1997-03-06", 49L}, {"1997-02-27", 49L}, {"1996-12-04", 50L}, {"1996-09-10", 48L},
        {"1996-09-04", 50L}, {"1996-05-01", 48L}, {"1996-04-20", 50L}, {"1995-11-13", 48L}, {"1995-10-28", 50L},
        {"1995-10-26", 49L}, {"1995-10-03", 49L}, {"1995-07-14", 50L}, {"1995-03-21", 48L}, {"1995-03-18", 48L},
        {"1995-03-02", 49L}, {"1995-02-20", 48L}, {"1995-01-22", 48L}, {"1994-11-22", 49L}, {"1994-11-21", 49L}
    };
    testQuery(
        "SELECT TIMESTAMP_FORMAT(__time, 'yyyy-MM-dd'), L_QUANTITY FROM lineitem" +
        "   WHERE L_LINENUMBER = 1 AND L_QUANTITY >= 48 AND L_SHIPMODE = 'TRUCK'" +
        "   ORDER BY __time DESC LIMIT 40",
        expected2
    );

    // on single segment
    Object[][] expected3 = {
        {"1997-12-29", 48L}, {"1997-12-26", 48L}, {"1997-11-16", 50L}, {"1997-10-15", 50L}, {"1997-10-09", 49L},
        {"1997-10-03", 49L}, {"1997-09-27", 50L}, {"1997-08-22", 48L}, {"1997-08-16", 49L}, {"1997-05-05", 50L},
        {"1997-04-04", 50L}, {"1997-03-19", 48L}, {"1997-03-09", 50L}, {"1997-03-06", 49L}, {"1997-02-27", 49L}
    };
    testQuery(
        "SELECT TIMESTAMP_FORMAT(__time, 'yyyy-MM-dd'), L_QUANTITY FROM lineitem" +
        "   WHERE L_LINENUMBER = 1 AND L_QUANTITY >= 48 AND L_SHIPMODE = 'TRUCK'" +
        " AND __time BETWEEN TIMESTAMP '1997-01-01' AND TIMESTAMP '1998-01-01'" +
        "   ORDER BY __time DESC LIMIT 40",
        expected3
    );
  }

  @Test
  public void test4203() throws Exception
  {
    Object[][] expected = {
        {0.06F, 17L}, {0.05F, 23L}, {0.07F, 22L}, {0.07F, 4L}, {0.07F, 8L}, {0.06F, 2L}, {0.05F, 24L},
        {0.07F, 6L}, {0.07F, 11L}, {0.06F, 19L}, {0.05F, 1L}, {0.07F, 13L}, {0.06F, 5L}, {0.05F, 18L}, {0.07F, 17L}
    };
    testQuery(
        "SELECT L_DISCOUNT, L_QUANTITY FROM lineitem"
        + " WHERE"
        + "    L_SHIPDATE >= '1993-01-01' AND"
        + "    L_SHIPDATE < '1993-01-10' AND"
        + "    L_DISCOUNT BETWEEN 0.06 - 0.01 and 0.06 + 0.01 AND"
        + "    L_QUANTITY < 25",
        expected
    );
  }

  @Test
  public void tpch4303() throws Exception
  {
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setHashJoinThreshold(10);
    join.setSemiJoinThreshold(100000);
    join.setBroadcastJoinThreshold(51);     // supplier + 1
    join.setBloomFilterThreshold(100);
    join.setForcedFilterHugeThreshold(5000);
    join.setForcedFilterTinyThreshold(100);

    testQuery(JOIN_REORDERING_WITH_E, TpchTest.TPCH9, TpchTest.TPCH9_EXPLAIN_JR, TpchTest.TPCH9_RESULT);

    hook.verifyHooked(
        "n/9cYF21SsJtfnzHnN9i0Q==",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}, CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}, CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:string, N_NATIONKEY:string}}}], timeColumnName=__time}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='v0', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}]}",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:string, N_NATIONKEY:string}}}",
        "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, v0], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}"
    );
  }

  @Test
  public void tpch430x() throws Exception
  {
    JoinQueryConfig join = walker.getQueryConfig().getJoin();
    join.setHashJoinThreshold(1);
    join.setSemiJoinThreshold(3000);
    join.setBroadcastJoinThreshold(100000);
    join.setBloomFilterThreshold(100);
    join.setForcedFilterHugeThreshold(5000);
    join.setForcedFilterTinyThreshold(100);

    testQuery(JOIN_REORDERING_WITH_E, TpchTest.TPCH21_2, TpchTest.TPCH21_2_EXPLAIN_JR2, TpchTest.TPCH21_RESULT);

    hook.verifyHooked(
        "RnW+qO1LOa9VcSacZRUgDw==",
        "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
        "StreamQuery{dataSource='supplier', filter=S_NATIONKEY=='24', columns=[S_SUPPKEY]}",
        "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && InDimFilter{dimension='L_SUPPKEY', values=[10, 19, 46, 49]}), columns=[L_ORDERKEY, L_SUPPKEY]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=orders, leftJoinColumns=[O_ORDERKEY], rightAlias=lineitem+supplier+nation, rightJoinColumns=[L_ORDERKEY]}, hashLeft=false, hashSignature={L_ORDERKEY:string, L_SUPPKEY:string}, applyFilter=true}}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], outputColumns=[d0, d1]}",
        "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=orders, leftJoinColumns=[O_ORDERKEY], rightAlias=lineitem+supplier+nation, rightJoinColumns=[L_ORDERKEY]}, hashLeft=false, hashSignature={L_ORDERKEY:string, L_SUPPKEY:string}, applyFilter=true}}",
        "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=orders, leftJoinColumns=[O_ORDERKEY], rightAlias=lineitem+supplier+nation, rightJoinColumns=[L_ORDERKEY]}, hashLeft=false, hashSignature={L_ORDERKEY:string, L_SUPPKEY:string}, applyFilter=true}}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], outputColumns=[d0, d1]}",
        "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=orders, leftJoinColumns=[O_ORDERKEY], rightAlias=lineitem+supplier+nation, rightJoinColumns=[L_ORDERKEY]}, hashLeft=false, hashSignature={L_ORDERKEY:string, L_SUPPKEY:string}, applyFilter=true}}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=!(L_ORDERKEY==NULL), columns=[L_ORDERKEY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_ORDERKEY], rightAlias=orders+lineitem+supplier+nation, rightJoinColumns=[d0]}, hashLeft=false, hashSignature={d0:string, d1:string}, applyFilter=true}}', dimensions=[DefaultDimensionSpec{dimension='d0', outputName='_d0'}, DefaultDimensionSpec{dimension='d1', outputName='_d1'}], filter=MathExprFilter{expression='(L_SUPPKEY != d1)'}, outputColumns=[_d0, _d1]}",
        "StreamQuery{dataSource='lineitem', filter=!(L_ORDERKEY==NULL), columns=[L_ORDERKEY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_ORDERKEY], rightAlias=orders+lineitem+supplier+nation, rightJoinColumns=[d0]}, hashLeft=false, hashSignature={d0:string, d1:string}, applyFilter=true}}",
        "StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:string, N_NATIONKEY:string}, applyFilter=true}}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), columns=[L_ORDERKEY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_ORDERKEY], rightAlias=orders+lineitem+supplier+nation, rightJoinColumns=[d0]}, hashLeft=false, hashSignature={d0:string, d1:string}, applyFilter=true}}', dimensions=[DefaultDimensionSpec{dimension='d0', outputName='_d0'}, DefaultDimensionSpec{dimension='d1', outputName='_d1'}], filter=MathExprFilter{expression='(L_SUPPKEY != d1)'}, postAggregatorSpecs=[MathPostAggregator{name='p0', expression='true', finalize=true}], outputColumns=[_d0, _d1, p0]}",
        "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), columns=[L_ORDERKEY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_ORDERKEY], rightAlias=orders+lineitem+supplier+nation, rightJoinColumns=[d0]}, hashLeft=false, hashSignature={d0:string, d1:string}, applyFilter=true}}",
        "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_SUPPKEY==NULL) && !(L_ORDERKEY==NULL)), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_ORDERKEY, L_SUPPKEY], rightAlias=lineitem+orders+lineitem+supplier+nation, rightJoinColumns=[_d0, _d1]}, hashLeft=false, hashSignature={_d0:string, _d1:string}, applyFilter=true}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+lineitem+orders+lineitem+supplier+nation, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NAME:string, S_NATIONKEY:string, S_SUPPKEY:string, N_NAME:string, N_NATIONKEY:string}}, BroadcastJoinProcessor{element=JoinElement{joinType=LO, leftAlias=lineitem+lineitem+orders+lineitem+supplier+nation+supplier+nation, leftJoinColumns=[L_ORDERKEY, L_SUPPKEY], rightAlias=lineitem+orders+lineitem+supplier+nation, rightJoinColumns=[_d0, _d1]}, hashLeft=false, hashSignature={_d0:string, _d1:string, p0:boolean}}]}",
        "GroupByQuery{dataSource='StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && !(O_ORDERKEY==NULL)), columns=[O_ORDERKEY, O_ORDERSTATUS], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=orders, leftJoinColumns=[O_ORDERKEY], rightAlias=lineitem+lineitem+orders+lineitem+supplier+nation+supplier+nation+lineitem+orders+lineitem+supplier+nation, rightJoinColumns=[L_ORDERKEY]}, hashLeft=false, hashSignature={L_COMMITDATE:string, L_ORDERKEY:string, L_RECEIPTDATE:string, L_SUPPKEY:string, _d0:string, _d1:string, S_NAME:string, S_NATIONKEY:string, S_SUPPKEY:string, N_NAME:string, N_NATIONKEY:string, _d00:string, _d10:string, p0:boolean}, applyFilter=true}}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], filter=p0==NULL, aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
        "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && !(O_ORDERKEY==NULL)), columns=[O_ORDERKEY, O_ORDERSTATUS], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=orders, leftJoinColumns=[O_ORDERKEY], rightAlias=lineitem+lineitem+orders+lineitem+supplier+nation+supplier+nation+lineitem+orders+lineitem+supplier+nation, rightJoinColumns=[L_ORDERKEY]}, hashLeft=false, hashSignature={L_COMMITDATE:string, L_ORDERKEY:string, L_RECEIPTDATE:string, L_SUPPKEY:string, _d0:string, _d1:string, S_NAME:string, S_NATIONKEY:string, S_SUPPKEY:string, N_NAME:string, N_NATIONKEY:string, _d00:string, _d10:string, p0:boolean}, applyFilter=true}}"
    );
  }
}
