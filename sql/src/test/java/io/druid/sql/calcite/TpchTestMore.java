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

import io.druid.query.QueryConfig;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Before;
import org.junit.Test;

public class TpchTestMore extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static final QueryConfig config = new QueryConfig();
  private static final TestQuerySegmentWalker walker;

  static {
    walker = TpchTestHelper.walker.duplicate()
                                  .withQueryConfig(config)
                                  .withQueryHook(hook);
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Before
  public void before()
  {
    // left broadcast + sort (see StreamQuery.getMergeOrdering)
    config.getJoin().setHashJoinThreshold(-1);
    config.getJoin().setSemiJoinThreshold(-1);
    config.getJoin().setBroadcastJoinThreshold(51);
    config.getJoin().setBloomFilterThreshold(-1);
    hook.clear();
  }

  @Test
  public void test3542_7() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        TpchTest.TPCH7,
        TpchTest.TPCH7_EXPLAIN,
        TpchTest.TPCH7_RESULT
    );
    hook.verifyHooked(
        "GlKGqgArZooV1MhxHyeEdQ==",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}, CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME0', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=((N_NAME0=='KENYA' && N_NAME=='PERU') || (N_NAME0=='PERU' && N_NAME=='KENYA')), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
        "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
        "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3542_8() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        TpchTest.TPCH8,
        TpchTest.TPCH8_EXPLAIN,
        TpchTest.TPCH8_RESULT
    );
    hook.verifyHooked(
        "qr7gZLbdI+DRm0ExySFCMw==",
        "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE]}",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
        "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
        "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_ORDERKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
        "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}",
        "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='R_REGIONKEY', direction=ascending}]}"
    );
  }

  @Test
  public void test3542_9() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        TpchTest.TPCH9,
        TpchTest.TPCH9_EXPLAIN,
        TpchTest.TPCH9_RESULT
    );
    hook.verifyHooked(
        "Fe6gF53dkY2Inbn42uH/UQ==",
        "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_NAME, P_PARTKEY]}",
        "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=lineitem, rightJoinColumns=[L_PARTKEY]}, hashLeft=true, hashSignature={P_NAME:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
        "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='L_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='L_PARTKEY', direction=ascending}], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=lineitem, rightJoinColumns=[L_PARTKEY]}, hashLeft=true, hashSignature={P_NAME:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
        "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], orderingSpecs=[OrderByColumnSpec{dimension='PS_SUPPKEY', direction=ascending}, OrderByColumnSpec{dimension='PS_PARTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY], orderingSpecs=[OrderByColumnSpec{dimension='O_ORDERKEY', direction=ascending}]}",
        "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], orderingSpecs=[OrderByColumnSpec{dimension='N_NATIONKEY', direction=ascending}]}"
    );
  }
}
