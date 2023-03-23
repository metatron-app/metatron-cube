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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

// the problem is.. some queries containing join return empty cause dataset is too small (s=0.005)
@RunWith(Parameterized.class)
public class SsbTest extends SsbTestHelper
{
  @Parameterized.Parameters(name = "semi:{0}, broadcast:{1}, bloom:{2}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[]{false, false, false}, new Object[]{false, false, true},
        new Object[]{false, true, false}, new Object[]{false, true, true},
        new Object[]{true, false, false}, new Object[]{true, false, true},
        new Object[]{true, true, false}, new Object[]{true, true, true}
    );
  }

  private final boolean semiJoin;
  private final boolean broadcastJoin;
  private final boolean bloomFilter;

  public SsbTest(boolean semiJoin, boolean broadcastJoin, boolean bloomFilter)
  {
    walker.getQueryConfig().getJoin().setSemiJoinThreshold(semiJoin ? 100000 : -1);
    walker.getQueryConfig().getJoin().setBroadcastJoinThreshold(broadcastJoin ? 51 : -1);     // supplier + 1
    walker.getQueryConfig().getJoin().setBloomFilterThreshold(bloomFilter ? 100 : 1000000);
    this.semiJoin = semiJoin;
    this.broadcastJoin = broadcastJoin;
    this.bloomFilter = bloomFilter;
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

  @Test
  public void ssb1_1() throws Exception
  {
    testQuery(
        "SELECT sum(LO_EXTENDEDPRICE*LO_DISCOUNT) as revenue"
        + " FROM ssb_lineorder, ssb_date"
        + " WHERE ssb_lineorder.__time = ssb_date.__time AND"
        + "       D_YEAR = 1992 AND"
        + "       LO_DISCOUNT between 1 and 3 AND"
        + "       LO_QUANTITY < 25"
        ,
        "DruidOuterQueryRel(scanProject=[*($0, $1)], revenue=[SUM($0)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
        + "    DruidQueryRel(table=[druid.ssb_lineorder], scanFilter=[AND(>=($2, 1), <=($2, 3), <($9, 25))], scanProject=[$2, $3, $16])\n"
        + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[=($13, 1992)], scanProject=[$16])\n"
        ,
        new Object[]{7.4425744E7}
    );

    if (semiJoin) {
      hook.verifyHooked(
          "T0WfyqFjEEM24ZkR6UaBDg==",
          "StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time]}",
          "TimeseriesQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_EXTENDEDPRICE, LO_DISCOUNT]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_EXTENDEDPRICE * LO_DISCOUNT)', inputType='double'}], outputColumns=[a0]}",
          "StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_EXTENDEDPRICE, LO_DISCOUNT]}"
      );
    } else if (broadcastJoin) {
      hook.verifyHooked(
          "aCGKccO22Hb+D8qJN/WkNw==",
          "StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time]}",
          "TimeseriesQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_DISCOUNT, LO_EXTENDEDPRICE, __time], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={__time:long}}}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_EXTENDEDPRICE * LO_DISCOUNT)', inputType='double'}], outputColumns=[a0]}",
          "StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_DISCOUNT, LO_EXTENDEDPRICE, __time], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={__time:long}}}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "hjkLWvZvJsmABbDJNwzwBg==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && BloomDimFilter.Factory{bloomSource=$view:ssb_date[__time](D_YEAR=='1992'), fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop, maxNumEntries=12}), columns=[LO_DISCOUNT, LO_EXTENDEDPRICE, __time]}, StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_EXTENDEDPRICE * LO_DISCOUNT)', inputType='double'}], outputColumns=[a0]}",
            "TimeseriesQuery{dataSource='ssb_date', filter=D_YEAR=='1992', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[__time], groupingSets=Noop, byRow=true, maxNumEntries=12}]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_DISCOUNT, LO_EXTENDEDPRICE, __time]}",
            "StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "S78J/cSGntJmFvXR0a/7kQ==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)}), columns=[LO_DISCOUNT, LO_EXTENDEDPRICE, __time]}, StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_EXTENDEDPRICE * LO_DISCOUNT)', inputType='double'}], outputColumns=[a0]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BoundDimFilter{LO_QUANTITY < 25(numeric)} && BoundDimFilter{1 <= LO_DISCOUNT <= 3(numeric)}), columns=[LO_DISCOUNT, LO_EXTENDEDPRICE, __time]}",
            "StreamQuery{dataSource='ssb_date', filter=D_YEAR=='1992', columns=[__time], $hash=true}"
        );
      }
    }
  }

  public static final String SSB2_1 =
      "SELECT sum(LO_REVENUE), D_YEAR, P_BRAND1"
      + " FROM ssb_lineorder, ssb_date, ssb_part, ssb_supplier"
      + " WHERE ssb_lineorder.__time = ssb_date.__time AND"
      + "       LO_PARTKEY = P_PARTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       P_CATEGORY = 'MFGR#12' AND"
      + "       S_REGION = 'AMERICA'"
      + " GROUP BY D_YEAR, P_BRAND1"
      + " ORDER BY D_YEAR, P_BRAND1";

  public static final String SSB2_1_PLAN =
      "DruidOuterQueryRel(group=[{0, 1}], EXPR$0=[SUM($2)], aggregateProject=[$2, $0, $1], sort=[$1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 3, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$8, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n"
      + "      DruidQueryRel(table=[druid.ssb_part], scanFilter=[=($1, 'MFGR#12')], scanProject=[$0, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n";

  public static final String SSB2_1_PLAN_JR =
      "DruidOuterQueryRel(group=[{0, 1}], EXPR$0=[SUM($2)], aggregateProject=[$2, $0, $1], sort=[$1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[3, 2, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1, 3])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$8, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "      DruidQueryRel(table=[druid.ssb_part], scanFilter=[=($1, 'MFGR#12')], scanProject=[$0, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n";

  public static final String SSB2_1_PLAN_JR2 =
      "DruidOuterQueryRel(group=[{0, 1}], EXPR$0=[SUM($2)], aggregateProject=[$2, $0, $1], sort=[$1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[3, 2, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 2, 3])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$8, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_part], scanFilter=[=($1, 'MFGR#12')], scanProject=[$0, $6])\n"
      + "      DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n";

  public static final Object[][] SSB2_1_RESULT = {
      {2420990D, "1992", "MFGR#1211"},
      {7034664D, "1992", "MFGR#129"}
  };

  @Test
  public void ssb2_1() throws Exception
  {
    testQuery(SSB2_1, SSB2_1_PLAN, SSB2_1_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "bSa7GAP4dCBc+UtE8VskGg==",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}]}"
        );
      } else {
        hook.verifyHooked(
            "DFvViZNYXGv+TIIR9F07Ig==",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]} && InDimFilter{dimension='LO_PARTKEY', values=[135, 144, 235, 283, 296, 327, 419, 474, 502, 542, ..22 more]} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]} && InDimFilter{dimension='LO_PARTKEY', values=[135, 144, 235, 283, 296, 327, 419, 474, 502, 542, ..22 more]} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "+X4ho03B0z1KeD56elxnUQ==",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date+ssb_part, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date+ssb_part, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}]}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "f3wuQt3wN1H8Y00IZJqVig==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomDimFilter.Factory{bloomSource=$view:ssb_part[P_PARTKEY](P_CATEGORY=='MFGR#12'), fields=[DefaultDimensionSpec{dimension='LO_PARTKEY', outputName='LO_PARTKEY'}], groupingSets=Noop, maxNumEntries=32} && BloomDimFilter.Factory{bloomSource=$view:ssb_supplier[S_SUPPKEY](S_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop, maxNumEntries=4}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
              "TimeseriesQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=32}]}",
              "TimeseriesQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[S_SUPPKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
              "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_PARTKEY', outputName='LO_PARTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "uJf0xiaizK2n3sjWmUc2pg==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
              "StreamQuery{dataSource='ssb_lineorder', columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#12', columns=[P_BRAND1, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String SSB2_2 =
      "SELECT sum(LO_REVENUE), D_YEAR, P_BRAND1"
      + " FROM ssb_lineorder, ssb_date, ssb_part, ssb_supplier"
      + " WHERE ssb_lineorder.__time = ssb_date.__time AND"
      + "       LO_PARTKEY = P_PARTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       P_BRAND1 between 'MFGR#2221' and 'MFGR#2228' AND"
      + "       S_REGION = 'AMERICA'"
      + " GROUP BY D_YEAR, P_BRAND1"
      + " ORDER BY D_YEAR, P_BRAND1";

  public static final String SSB2_2_PLAN =
      "DruidOuterQueryRel(group=[{0, 1}], EXPR$0=[SUM($2)], aggregateProject=[$2, $0, $1], sort=[$1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 3, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$8, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n"
      + "      DruidQueryRel(table=[druid.ssb_part], scanFilter=[AND(>=($0, 'MFGR#2221'), <=($0, 'MFGR#2228'))], scanProject=[$0, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n";

  public static final String SSB2_2_PLAN_JR =
      "DruidOuterQueryRel(group=[{0, 1}], EXPR$0=[SUM($2)], aggregateProject=[$2, $0, $1], sort=[$1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[4, 0, 3])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[AND(>=($0, 'MFGR#2221'), <=($0, 'MFGR#2228'))], scanProject=[$0, $6])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1, 3])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$8, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "      DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n";

  public static final String SSB2_2_PLAN_JR2 =
      "DruidOuterQueryRel(group=[{0, 1}], EXPR$0=[SUM($2)], aggregateProject=[$2, $0, $1], sort=[$1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[3, 2, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 2, 3])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$8, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_part], scanFilter=[AND(>=($0, 'MFGR#2221'), <=($0, 'MFGR#2228'))], scanProject=[$0, $6])\n"
      + "      DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n";

  public static final Object[] SSB2_2_RESULT = {
      2781578.0, "1992", "MFGR#2223"
  };

  @Test
  public void ssb2_2() throws Exception
  {
    testQuery(SSB2_2, SSB2_2_PLAN, SSB2_2_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "E+DQn0Bq4BcfIupvZ7HOLg==",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}]}"
        );
      } else {
        hook.verifyHooked(
            "afIc9BD4+15zfECRVKcTGg==",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]} && InDimFilter{dimension='LO_PARTKEY', values=[170, 200, 27, 536, 573, 752, 818, 852]} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]} && InDimFilter{dimension='LO_PARTKEY', values=[170, 200, 27, 536, 573, 752, 818, 852]} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "gEVI4SwXcq61gCtZPJOKzg==",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date+ssb_part, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_PARTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date, leftJoinColumns=[LO_PARTKEY], rightAlias=ssb_part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_BRAND1:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_date+ssb_part, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}]}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "fOVGRbLUFa3sqYqiQg0j5Q==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomDimFilter.Factory{bloomSource=$view:ssb_part[P_PARTKEY](BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}), fields=[DefaultDimensionSpec{dimension='LO_PARTKEY', outputName='LO_PARTKEY'}], groupingSets=Noop, maxNumEntries=8} && BloomDimFilter.Factory{bloomSource=$view:ssb_supplier[S_SUPPKEY](S_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop, maxNumEntries=4}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
              "TimeseriesQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=8}]}",
              "TimeseriesQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[S_SUPPKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
              "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_PARTKEY', outputName='LO_PARTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop}), columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "Cf9YLn6QD3VIFMUZdzuP/w==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[a0, d0, d1]}",
              "StreamQuery{dataSource='ssb_lineorder', columns=[LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=BoundDimFilter{MFGR#2221 <= P_BRAND1 <= MFGR#2228}, columns=[P_BRAND1, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}"
          );
        }

      }
    }
  }

  public static final String SSB3_1 =
      "SELECT C_NATION, S_NATION, D_YEAR, sum(LO_REVENUE) as revenue"
      + " FROM ssb_customer, ssb_lineorder, ssb_supplier, ssb_date"
      + " WHERE LO_CUSTKEY = C_CUSTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       ssb_lineorder.__time = ssb_date.__time AND"
      + "       C_REGION = 'AMERICA' AND"
      + "       S_REGION = 'AMERICA' AND"
      + "       D_YEAR >= 1992 AND D_YEAR <= 1997"
      + " GROUP BY C_NATION, S_NATION, D_YEAR"
      + " ORDER BY D_YEAR ASC, revenue DESC";

  public static final String SSB3_1_PLAN =
      "DruidOuterQueryRel(group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$2:ASC, $3:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 3, 4, 1])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 3, 4, 5])\n"
      + "        DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2, $5])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $10, $13, $16])\n"
      + "      DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$3, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[AND(>=($13, 1992), <=($13, 1997))], scanProject=[$13, $16])\n";

  public static final String SSB3_1_PLAN_JR =
      "DruidOuterQueryRel(group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$2:ASC, $3:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[3, 2, 4, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$3, $6])\n"
      + "      DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2, $5])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[AND(>=($13, 1992), <=($13, 1997))], scanProject=[$13, $16])\n";

  public static final String SSB3_1_PLAN_JR2 =
      "DruidOuterQueryRel(group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$2:ASC, $3:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[2, 3, 4, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[0, 2, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 5])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2, $5])\n"
      + "      DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$3, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[AND(>=($13, 1992), <=($13, 1997))], scanProject=[$13, $16])\n";

  public static final Object[][] SSB3_1_RESULT = {
      {"CANADA", "UNITED STATES", "1992", 2.9221324E7},
      {"BRAZIL", "UNITED STATES", "1992", 1.0390371E7},
      {"CANADA", "PERU", "1992", 7442653.0},
      {"BRAZIL", "ARGENTINA", "1992", 6862420.0},
      {"PERU", "PERU", "1992", 4973989.0},
      {"BRAZIL", "PERU", "1992", 3795334.0},
      {"CANADA", "ARGENTINA", "1992", 1441163.0},
      {"PERU", "ARGENTINA", "1992", 948913.0}
  };

  @Test
  public void ssb3_1() throws Exception
  {
    testQuery(SSB3_1, SSB3_1_PLAN, SSB3_1_RESULT);

    if (broadcastJoin) {
      hook.verifyHooked(
          "mtPvUn4Nr7FjZbbXKZiOvw==",
          "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
          "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY]}",
          "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer, leftJoinColumns=[C_CUSTKEY], rightAlias=ssb_lineorder, rightJoinColumns=[LO_CUSTKEY]}, hashLeft=true, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='C_NATION', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
          "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer, leftJoinColumns=[C_CUSTKEY], rightAlias=ssb_lineorder, rightJoinColumns=[LO_CUSTKEY]}, hashLeft=true, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATION:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}"
      );
    } else {
      if (semiJoin) {
        hook.verifyHooked(
            "etR2lqb8ADng+k2HcAZbCA==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[ssb_customer]}, StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NATION', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "AN64W6VUFrysMjSuAOMEHA==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}, StreamQuery{dataSource='ssb_lineorder', filter=(BloomDimFilter.Factory{bloomSource=$view:ssb_customer[C_CUSTKEY](C_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop, maxNumEntries=31} && BloomDimFilter.Factory{bloomSource=$view:ssb_supplier[S_SUPPKEY](S_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop, maxNumEntries=4} && BloomDimFilter.Factory{bloomSource=$view:ssb_date[__time](BoundDimFilter{1992 <= D_YEAR <= 1997}), fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop, maxNumEntries=12}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NATION', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=31}]}",
              "TimeseriesQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[S_SUPPKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
              "TimeseriesQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[__time], groupingSets=Noop, byRow=true, maxNumEntries=12}]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}",
              "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "MxJ6ig3wN8qLgSTgip7qog==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}, StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NATION', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}",
              "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}"
          );
        }
      }
    }
  }

  public static final String SSB3_2 =
      "SELECT C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue"
      + " FROM ssb_customer, ssb_lineorder, ssb_supplier, ssb_date"
      + " WHERE LO_CUSTKEY = C_CUSTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       ssb_lineorder.__time = ssb_date.__time AND"
      + "       C_NATION = 'MOROCCO' AND"
      + "       S_NATION = 'MOROCCO' AND"
      + "       D_YEAR >= 1992 AND D_YEAR <= 1997"
      + " GROUP BY C_CITY, S_CITY, D_YEAR"
      + " ORDER BY D_YEAR ASC, revenue DESC";

  public static final String SSB3_2_PLAN =
      "DruidOuterQueryRel(group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$2:ASC, $3:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 3, 4, 1])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 3, 4, 5])\n"
      + "        DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($5, 'MOROCCO')], scanProject=[$1, $2])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $10, $13, $16])\n"
      + "      DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($3, 'MOROCCO')], scanProject=[$1, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[AND(>=($13, 1992), <=($13, 1997))], scanProject=[$13, $16])\n";

  public static final String SSB3_2_PLAN_JR =
      "DruidOuterQueryRel(group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$2:ASC, $3:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[3, 2, 4, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($3, 'MOROCCO')], scanProject=[$1, $6])\n"
      + "      DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($5, 'MOROCCO')], scanProject=[$1, $2])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[AND(>=($13, 1992), <=($13, 1997))], scanProject=[$13, $16])\n";

  public static final String SSB3_2_PLAN_JR2 =
      "DruidOuterQueryRel(group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$2:ASC, $3:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[2, 3, 4, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[0, 2, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4])\n"
      + "        DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $10, $13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($5, 'MOROCCO')], scanProject=[$1, $2])\n"
      + "      DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($3, 'MOROCCO')], scanProject=[$1, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_date], scanFilter=[AND(>=($13, 1992), <=($13, 1997))], scanProject=[$13, $16])\n";

  public static final Object[][] SSB3_2_RESULT = {
      {"MOROCCO  0", "MOROCCO  7", "1992", 1.8457988E7},
      {"MOROCCO  7", "MOROCCO  7", "1992", 1.2019794E7}
  };

  @Test
  public void ssb3_2() throws Exception
  {
    testQuery(SSB3_2, SSB3_2_PLAN, SSB3_2_RESULT);

    if (broadcastJoin) {
      hook.verifyHooked(
          "bE64Bd+7bPaNfe2edPsbJA==",
          "StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY]}",
          "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY]}",
          "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer, leftJoinColumns=[C_CUSTKEY], rightAlias=ssb_lineorder, rightJoinColumns=[LO_CUSTKEY]}, hashLeft=true, hashSignature={C_CITY:dimension.string, C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_CITY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', dimensions=[DefaultDimensionSpec{dimension='C_CITY', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
          "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[LO_SUPPKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer, leftJoinColumns=[C_CUSTKEY], rightAlias=ssb_lineorder, rightJoinColumns=[LO_CUSTKEY]}, hashLeft=true, hashSignature={C_CITY:dimension.string, C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder, leftJoinColumns=[LO_SUPPKEY], rightAlias=ssb_supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_CITY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_customer+ssb_lineorder+ssb_supplier, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}"
      );
    } else {
      if (semiJoin) {
        hook.verifyHooked(
            "oqQ0AfHynjlZFYSPFuPKPw==",
            "StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[ssb_customer]}, StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[1, 107, 32, 34, 53, 79, 95, 99]} && LO_SUPPKEY=='4' && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CITY', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[1, 107, 32, 34, 53, 79, 95, 99]} && LO_SUPPKEY=='4' && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "dwsBs6EZTu74H5yfWCWIHw==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY], $hash=true}, StreamQuery{dataSource='ssb_lineorder', filter=(BloomDimFilter.Factory{bloomSource=$view:ssb_customer[C_CUSTKEY](C_NATION=='MOROCCO'), fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop, maxNumEntries=8} && BloomDimFilter.Factory{bloomSource=$view:ssb_supplier[S_SUPPKEY](S_NATION=='MOROCCO'), fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop, maxNumEntries=1} && BloomDimFilter.Factory{bloomSource=$view:ssb_date[__time](BoundDimFilter{1992 <= D_YEAR <= 1997}), fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop, maxNumEntries=12}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CITY', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=8}]}",
              "TimeseriesQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[S_SUPPKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "TimeseriesQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[__time], groupingSets=Noop, byRow=true, maxNumEntries=12}]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_SUPPKEY', outputName='LO_SUPPKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "KVsG88AgTuQDncO2m+SHqg==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY], $hash=true}, StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}], timeColumnName=__time}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CITY', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='D_YEAR', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='LO_REVENUE', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d2', direction=ascending}, OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_NATION=='MOROCCO', columns=[C_CITY, C_CUSTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_REVENUE, LO_SUPPKEY, __time]}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='MOROCCO', columns=[S_CITY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=BoundDimFilter{1992 <= D_YEAR <= 1997}, columns=[D_YEAR, __time], $hash=true}"
          );
        }
      }
    }
  }

  public static final String SSB4_1 =
      "SELECT D_YEAR, C_NATION, sum(LO_REVENUE - LO_SUPPLYCOST) as profit"
      + " FROM ssb_date, ssb_customer, ssb_supplier, ssb_part, ssb_lineorder"
      + " WHERE LO_CUSTKEY = C_CUSTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       LO_PARTKEY = P_PARTKEY AND"
      + "       ssb_lineorder.__time = ssb_date.__time AND"
      + "       C_REGION = 'AMERICA' AND"
      + "       S_REGION = 'AMERICA' AND"
      + "      (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')"
      + " GROUP BY D_YEAR, C_NATION"
      + " ORDER BY D_YEAR, C_NATION";

  public static final String SSB4_1_PLAN =
      "DruidOuterQueryRel(scanProject=[$0, $1, -($2, $3)], group=[{0, 1}], profit=[SUM($2)], sort=[$0:ASC, $1:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[0], outputColumns=[0, 3, 7, 9])\n"
      + "    DruidOuterQueryRel(scanProject=[$0, $1, $2, $3, $10, $4, $5, $6, $7, $8, $9])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[0])\n"
      + "        DruidOuterQueryRel(scanProject=[$8, $9, $6, $7, $0, $1, $2, $3, $4, $5])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
      + "              DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "              DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2, $5])\n"
      + "            DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[OR(=($4, 'MFGR#1'), =($4, 'MFGR#2'))], scanProject=[$6])\n";

  public static final String SSB4_1_PLAN_JR =
      "DruidOuterQueryRel(scanProject=[$0, $1, -($2, $3)], group=[{0, 1}], profit=[SUM($2)], sort=[$0:ASC, $1:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[5, 4, 2, 3])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[OR(=($4, 'MFGR#1'), =($4, 'MFGR#2'))], scanProject=[$6])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 4, 6])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 4, 5])\n"
      + "          DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "          DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "        DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2, $5])\n"
      + "      DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n";

  public static final String SSB4_1_PLAN_JR2 =
      "DruidOuterQueryRel(scanProject=[$0, $1, -($2, $3)], group=[{0, 1}], profit=[SUM($2)], sort=[$0:ASC, $1:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[5, 4, 2, 3])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[OR(=($4, 'MFGR#1'), =($4, 'MFGR#2'))], scanProject=[$6])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1, 3, 4, 5])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 4, 5, 7])\n"
      + "          DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "          DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2, $5])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$6])\n"
      + "      DruidQueryRel(table=[druid.ssb_date], scanProject=[$13, $16])\n";

  public static final Object[][] SSB4_1_RESULT = {
      {"1992", "BRAZIL", 9912885.0},
      {"1992", "CANADA", 2.7347035E7},
      {"1992", "PERU", 4876746.0}
  };

  @Test
  public void ssb4_1() throws Exception
  {
    testQuery(SSB4_1, SSB4_1_PLAN, SSB4_1_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "joYd3ztcH5Mp1DqNlyFwUg==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, C_NATION, LO_REVENUE, LO_SUPPLYCOST]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, C_NATION, LO_REVENUE, LO_SUPPLYCOST]}",
            "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}"
        );
      } else {
        hook.verifyHooked(
            "30c64rG/0rdSlZVRKQWIEg==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, C_NATION, LO_REVENUE, LO_SUPPLYCOST]}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, C_NATION, LO_REVENUE, LO_SUPPLYCOST]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}"
        );
      }
    } else if (broadcastJoin) {
      hook.verifyHooked(
          "qSDRBw0Vo3ZU2L6mzsYAIQ==",
          "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION]}",
          "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
          "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
          "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
          "StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string, C_NATION:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}",
          "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY], $hash=true}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "m5IE4amDU8+cbpy+BMUkJw==",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=BloomDimFilter.Factory{bloomSource=$view:ssb_customer[C_CUSTKEY](C_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop, maxNumEntries=31}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "TimeseriesQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=31}]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop}, columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "+/NkHXfcRsOXA3KpengwBQ==",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='C_NATION', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, C_NATION, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY, C_NATION], $hash=true}",
            "StreamQuery{dataSource='ssb_date', columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_PARTKEY], $hash=true}"
        );
      }
    }
  }

  public static final String SSB4_2 =
      "SELECT D_YEAR, S_NATION, P_CATEGORY, sum(LO_REVENUE - LO_SUPPLYCOST) as profit"
      + " FROM ssb_date, ssb_customer, ssb_supplier, ssb_part, ssb_lineorder"
      + " WHERE LO_CUSTKEY = C_CUSTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       LO_PARTKEY = P_PARTKEY AND"
      + "       ssb_lineorder.__time = ssb_date.__time AND"
      + "       C_REGION = 'AMERICA' AND"
      + "       S_REGION = 'AMERICA' AND"
      + "      (D_YEAR = 1992 OR D_YEAR = 1993) AND"
      + "      (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')"
      + " GROUP BY D_YEAR, S_NATION, P_CATEGORY"
      + " ORDER BY D_YEAR, S_NATION, P_CATEGORY";

  public static final String SSB4_2_PLAN =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, -($3, $4)], group=[{0, 1, 2}], profit=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[1], outputColumns=[0, 3, 11, 7, 9])\n"
      + "    DruidOuterQueryRel(scanProject=[$0, $1, $2, $9, $10, $3, $4, $5, $6, $7, $8])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[1])\n"
      + "        DruidOuterQueryRel(scanProject=[$7, $8, $6, $0, $1, $2, $3, $4, $5])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
      + "              DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "              DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2])\n"
      + "            DruidQueryRel(table=[druid.ssb_date], scanFilter=[OR(=($13, 1992), =($13, 1993))], scanProject=[$13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$3, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[OR(=($4, 'MFGR#1'), =($4, 'MFGR#2'))], scanProject=[$1, $6])\n";

  public static final String SSB4_2_PLAN_JR =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, -($3, $4)], group=[{0, 1, 2}], profit=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[6, 5, 0, 3, 4])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[OR(=($4, 'MFGR#1'), =($4, 'MFGR#2'))], scanProject=[$1, $6])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 4, 5])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5, 6])\n"
      + "          DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "          DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$3, $6])\n"
      + "        DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2])\n"
      + "      DruidQueryRel(table=[druid.ssb_date], scanFilter=[OR(=($13, 1992), =($13, 1993))], scanProject=[$13, $16])\n";

  public static final String SSB4_2_PLAN_JR2 =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, -($3, $4)], group=[{0, 1, 2}], profit=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[6, 5, 0, 3, 4])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[OR(=($4, 'MFGR#1'), =($4, 'MFGR#2'))], scanProject=[$1, $6])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 4, 5])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 4, 5])\n"
      + "          DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "          DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($5, 'AMERICA')], scanProject=[$3, $6])\n"
      + "      DruidQueryRel(table=[druid.ssb_date], scanFilter=[OR(=($13, 1992), =($13, 1993))], scanProject=[$13, $16])\n";

  public static final Object[][] SSB4_2_RESULT = {
      {"1992", "ARGENTINA", "MFGR#15", 1337854.0},
      {"1992", "ARGENTINA", "MFGR#24", 6754066.0},
      {"1992", "PERU", "MFGR#11", 8035565.0},
      {"1992", "PERU", "MFGR#22", 3883082.0},
      {"1992", "PERU", "MFGR#24", 2894105.0},
      {"1992", "UNITED STATES", "MFGR#13", 3965165.0},
      {"1992", "UNITED STATES", "MFGR#14", 6219701.0},
      {"1992", "UNITED STATES", "MFGR#23", 7608801.0},
      {"1992", "UNITED STATES", "MFGR#24", 1438327.0}
  };

  @Test
  public void ssb4_2() throws Exception
  {
    testQuery(SSB4_2, SSB4_2_PLAN, SSB4_2_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "NTcSwL82PtPa0x/oq3v8nQ==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
            "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}"
        );
      } else {
        hook.verifyHooked(
            "/jHT/usgZk8cl6NBFOho6g==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[1, 10, 1000, 105, 115, 119, 120, 123, 126, 128, ..368 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_SUPPKEY', values=[1, 10, 3, 8]}, columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "suL6RIaGN7SrVotaG6SWTw==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
            "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "ZHRE0RFQ4O11nwmtr9ofUg==",
              "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomDimFilter.Factory{bloomSource=$view:ssb_customer[C_CUSTKEY](C_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop, maxNumEntries=31} && BloomDimFilter.Factory{bloomSource=$view:ssb_date[__time](InDimFilter{dimension='D_YEAR', values=[1992, 1993]}), fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop, maxNumEntries=12}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=31}]}",
              "TimeseriesQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[__time], groupingSets=Noop, byRow=true, maxNumEntries=12}]}",
              "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "/Wa6c9cKukRLdiribeWqYw==",
              "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_NATION', outputName='d1'}, DefaultDimensionSpec{dimension='P_CATEGORY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_NATION, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_REGION=='AMERICA', columns=[S_NATION, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=InDimFilter{dimension='P_MFGR', values=[MFGR#1, MFGR#2]}, columns=[P_CATEGORY, P_PARTKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String SSB4_3 =
      "SELECT D_YEAR, S_CITY, P_BRAND1, sum(LO_REVENUE - LO_SUPPLYCOST) as profit"
      + " FROM ssb_date, ssb_customer, ssb_supplier, ssb_part, ssb_lineorder"
      + " WHERE LO_CUSTKEY = C_CUSTKEY AND"
      + "       LO_SUPPKEY = S_SUPPKEY AND"
      + "       LO_PARTKEY = P_PARTKEY AND"
      + "       ssb_lineorder.__time = ssb_date.__time AND"
      + "       C_REGION = 'AMERICA' AND"
      + "       S_NATION = 'UNITED STATES' AND"
      + "      (D_YEAR = 1992 OR D_YEAR = 1993) AND"
      + "       P_CATEGORY = 'MFGR#14'"
      + " GROUP BY D_YEAR, S_CITY, P_BRAND1"
      + " ORDER BY D_YEAR, S_CITY, P_BRAND1";

  public static final String SSB4_3_PLAN =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, -($3, $4)], group=[{0, 1, 2}], profit=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[1], outputColumns=[0, 3, 11, 7, 9])\n"
      + "    DruidOuterQueryRel(scanProject=[$0, $1, $2, $9, $10, $3, $4, $5, $6, $7, $8])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[1])\n"
      + "        DruidOuterQueryRel(scanProject=[$7, $8, $6, $0, $1, $2, $3, $4, $5])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
      + "              DruidQueryRel(table=[druid.ssb_lineorder], scanProject=[$1, $8, $10, $13, $14, $16])\n"
      + "              DruidQueryRel(table=[druid.ssb_customer], scanFilter=[=($7, 'AMERICA')], scanProject=[$2])\n"
      + "            DruidQueryRel(table=[druid.ssb_date], scanFilter=[OR(=($13, 1992), =($13, 1993))], scanProject=[$13, $16])\n"
      + "        DruidQueryRel(table=[druid.ssb_supplier], scanFilter=[=($3, 'UNITED STATES')], scanProject=[$1, $6])\n"
      + "    DruidQueryRel(table=[druid.ssb_part], scanFilter=[=($1, 'MFGR#14')], scanProject=[$0, $6])\n";

  public static final Object[] SSB4_3_RESULT = {
      "1992", "UNITED ST6", "MFGR#1433", 6219701.0
  };

  @Test
  public void ssb4_3() throws Exception
  {
    testQuery(SSB4_3, SSB4_3_PLAN, SSB4_3_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "UBBlJxYN57xXAtXEi3sTuw==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
            "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[14, 188, 239, 255, 295, 302, 4, 414, 42, 455, ..23 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[14, 188, 239, 255, 295, 302, 4, 414, 42, 455, ..23 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}"
        );
      } else {
        hook.verifyHooked(
            "LuIHjCndFqkHi/qZ+m2WJA==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[14, 188, 239, 255, 295, 302, 4, 414, 42, 455, ..23 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_part]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_supplier]}], timeColumnName=__time}', filter=InDimFilter{dimension='LO_PARTKEY', values=[14, 188, 239, 255, 295, 302, 4, 414, 42, 455, ..23 more]}, columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, MaterializedQuery{dataSource=[ssb_customer]}], timeColumnName=__time}, MaterializedQuery{dataSource=[ssb_date]}], timeColumnName=__time}', filter=LO_SUPPKEY=='10', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(InDimFilter{dimension='LO_CUSTKEY', values=[101, 106, 117, 121, 122, 13, 133, 14, 141, 144, ..21 more]} && InDimFilter{dimension='__time', values=[694224000000, 694310400000, 694396800000, 694483200000, 694569600000, 694656000000, 694742400000, 694828800000, 694915200000, 695001600000, ..2 more]}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "rLpsfLxxEFDJ6lcvrp217Q==",
            "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY]}",
            "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
            "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fieldNames=[LO_CUSTKEY], groupingSets=Noop} && BloomFilter{fieldNames=[__time], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder, leftJoinColumns=[LO_CUSTKEY], rightAlias=ssb_customer, rightJoinColumns=[C_CUSTKEY]}, hashLeft=false, hashSignature={C_CUSTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=ssb_lineorder+ssb_customer, leftJoinColumns=[__time], rightAlias=ssb_date, rightJoinColumns=[__time]}, hashLeft=false, hashSignature={D_YEAR:dimension.string, __time:long}}]}",
            "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "p86poxlTS92IRVyKXBC6ag==",
              "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomDimFilter.Factory{bloomSource=$view:ssb_customer[C_CUSTKEY](C_REGION=='AMERICA'), fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop, maxNumEntries=31} && BloomDimFilter.Factory{bloomSource=$view:ssb_date[__time](InDimFilter{dimension='D_YEAR', values=[1992, 1993]}), fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop, maxNumEntries=12}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=31}]}",
              "TimeseriesQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[__time], groupingSets=Noop, byRow=true, maxNumEntries=12}]}",
              "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_lineorder', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='LO_CUSTKEY', outputName='LO_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='__time', outputName='__time'}], groupingSets=Noop}), columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "e4iB9iVvnDHipu0fuulaqA==",
              "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='D_YEAR', outputName='d0'}, DefaultDimensionSpec{dimension='S_CITY', outputName='d1'}, DefaultDimensionSpec{dimension='P_BRAND1', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(LO_REVENUE - LO_SUPPLYCOST)', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, S_CITY, S_SUPPKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}, StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}], timeColumnName=__time}', columns=[D_YEAR, __time0, C_CUSTKEY, LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_lineorder', columns=[LO_CUSTKEY, LO_PARTKEY, LO_REVENUE, LO_SUPPKEY, LO_SUPPLYCOST, __time]}",
              "StreamQuery{dataSource='ssb_customer', filter=C_REGION=='AMERICA', columns=[C_CUSTKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_date', filter=InDimFilter{dimension='D_YEAR', values=[1992, 1993]}, columns=[D_YEAR, __time], $hash=true}",
              "StreamQuery{dataSource='ssb_supplier', filter=S_NATION=='UNITED STATES', columns=[S_CITY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='ssb_part', filter=P_CATEGORY=='MFGR#14', columns=[P_BRAND1, P_PARTKEY], $hash=true}"
          );
        }
      }
    }
  }
}
