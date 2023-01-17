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
import io.druid.data.ValueDesc;
import io.druid.query.JoinQueryConfig;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

// the problem is.. some queries containing join return empty cause dataset is too small (s=0.005)
@RunWith(Parameterized.class)
public class TpchTest extends TpchTestHelper
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

  public TpchTest(boolean semiJoin, boolean broadcastJoin, boolean bloomFilter)
  {
    JoinQueryConfig joinConf = walker.getQueryConfig().getJoin();
    joinConf.setSemiJoinThreshold(semiJoin ? 100000 : -1);
    joinConf.setBroadcastJoinThreshold(broadcastJoin ? 51 : -1);     // supplier + 1
    joinConf.setBloomFilterThreshold(bloomFilter ? 100 : 1000000);
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

  public static final String TPCH1 =
      "SELECT"
      + " L_RETURNFLAG,"
      + " L_LINESTATUS,"
      + " SUM(L_QUANTITY) AS SUM_QTY,"
      + " SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,"
      + " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,"
      + " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,"
      + " AVG(L_QUANTITY) AS AVG_QTY,"
      + " AVG(L_EXTENDEDPRICE) AS AVG_PRICE,"
      + " AVG(L_DISCOUNT) AS AVG_DISC,"
      + " COUNT(*) AS COUNT_ORDER"
      + " FROM"
      + "    lineitem"
      + " WHERE"
      + "    L_SHIPDATE <= '1998-09-16'"
      + " GROUP BY"
      + "    L_RETURNFLAG,"
      + "    L_LINESTATUS"
      + " ORDER BY"
      + "    L_RETURNFLAG,"
      + "    L_LINESTATUS";

  public static final String TPCH1_PLAN =
      "DruidQueryRel(table=[druid.lineitem], "
      + "scanFilter=[<=($11, '1998-09-16')], "
      + "scanProject=[$10, $5, $8, $3, *($3, -(1, $2)), *(*($3, -(1, $2)), +(1, $15)), $2], "
      + "group=[{0, 1}], SUM_QTY=[SUM($2)], SUM_BASE_PRICE=[SUM($3)], SUM_DISC_PRICE=[SUM($4)], SUM_CHARGE=[SUM($5)], AVG_QTY=[AVG($2)], AVG_PRICE=[AVG($3)], AVG_DISC=[AVG($6)], COUNT_ORDER=[COUNT()], "
      + "sort=[$0:ASC, $1:ASC])\n";

  public static final Object[][] TPCH1_RESULT = {
      {"A", "F", 189203L, 2.649171512299999E8D, 2.5172256669044656E8D, 2.618137696907937E8D, 25L, 35407.26426490242D, 0.05014435F, 7482L},
      {"N", "F", 4654L, 6647990.519999999D, 6333568.496621376D, 6584905.261532691D, 26L, 37139.61184357541D, 0.04849162F, 179L},
      {"N", "O", 376815L, 5.2792684456999964E8D, 5.016250502495867E8D, 5.2164760657695186E8D, 25L, 35840.247424983005D, 0.04984861F, 14730L},
      {"R", "F", 191214L, 2.6792430413999987E8D, 2.5454761805335242E8D, 2.6480436570269588E8D, 25L, 35972.65093179375D, 0.04983217F, 7448L}
  };

  @Test
  public void tpch1() throws Exception
  {
    testQuery(
        TPCH1,
        TPCH1_PLAN,
        newGroupBy()
            .dataSource("lineitem")
            .dimensions(
                DefaultDimensionSpec.of("L_RETURNFLAG", "d0"),
                DefaultDimensionSpec.of("L_LINESTATUS", "d1")
            )
            .filters(BoundDimFilter.lte("L_SHIPDATE", "1998-09-16"))
            .aggregators(
                GenericSumAggregatorFactory.ofLong("a0", "L_QUANTITY"),
                GenericSumAggregatorFactory.ofDouble("a1", "L_EXTENDEDPRICE"),
                GenericSumAggregatorFactory.expr(
                    "a2",
                    "(L_EXTENDEDPRICE * (1 - L_DISCOUNT))",
                    ValueDesc.DOUBLE
                ),
                GenericSumAggregatorFactory.expr(
                    "a3",
                    "((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) * (1 + L_TAX))",
                    ValueDesc.DOUBLE
                ),
                CountAggregatorFactory.of("a4:count", "L_QUANTITY"),
                CountAggregatorFactory.of("a5:count", "L_EXTENDEDPRICE"),
                GenericSumAggregatorFactory.ofDouble("a6:sum", "L_DISCOUNT"),
                CountAggregatorFactory.of("a6:count", "L_DISCOUNT"),
                CountAggregatorFactory.of("a7")
            )
            .postAggregators(
                new FieldAccessPostAggregator("a4:sum", "a0"),
                new ArithmeticPostAggregator("a4", "quotient", Arrays.asList(
                    new FieldAccessPostAggregator(null, "a4:sum"),
                    new FieldAccessPostAggregator(null, "a4:count")
                )),
                new FieldAccessPostAggregator("a5:sum", "a1"),
                new ArithmeticPostAggregator("a5", "quotient", Arrays.asList(
                    new FieldAccessPostAggregator(null, "a5:sum"),
                    new FieldAccessPostAggregator(null, "a5:count")
                )),
                new ArithmeticPostAggregator("a6", "quotient", Arrays.asList(
                    new FieldAccessPostAggregator(null, "a6:sum"),
                    new FieldAccessPostAggregator(null, "a6:count")
                ))
            )
            .limitSpec(LimitSpec.of(
                OrderByColumnSpec.asc("d0"),
                OrderByColumnSpec.asc("d1")
            ))
            .outputColumns("d0", "d1", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7")
            .build(),
        TPCH1_RESULT
    );
  }

  public static final String TPCH2 =
      "SELECT"
      + "    S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT"
      + " FROM"
      + "    part, supplier, partsupp, nation, region"
      + " WHERE"
      + "    P_PARTKEY = PS_PARTKEY AND"
      + "    S_SUPPKEY = PS_SUPPKEY AND"
      + "    P_SIZE = 37 AND"
      + "    P_TYPE LIKE '%COPPER' AND"
      + "    S_NATIONKEY = N_NATIONKEY AND"
      + "    N_REGIONKEY = R_REGIONKEY AND"
      + "    R_NAME = 'EUROPE' AND"
      + "    PS_SUPPLYCOST = ("
      + "        SELECT "
      + "            MIN(PS_SUPPLYCOST)"
      + "        FROM"
      + "            partsupp, supplier, nation, region"
      + "        WHERE"
      + "            P_PARTKEY = PS_PARTKEY AND"
      + "            S_SUPPKEY = PS_SUPPKEY AND"
      + "            S_NATIONKEY = N_NATIONKEY AND"
      + "            N_REGIONKEY = R_REGIONKEY AND"
      + "            R_NAME = 'EUROPE'"
      + "    )"
      + " ORDER BY"
      + "    S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY"
      + " LIMIT 100";

  public static final String TPCH2_EXPLAIN =
      "DruidOuterQueryRel(sort=[$0:DESC, $2:ASC, $1:ASC, $3:ASC], fetch=[100])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1, 7], rightKeys=[0, 1], outputColumns=[2, 5, 8, 1, 0, 3, 6, 4])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[9], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 5, 6, 7, 8])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[1], outputColumns=[0, 1, 2, 3, 4, 5, 7, 8, 9, 11])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[6], outputColumns=[3, 4, 5, 6, 7, 8, 9, 10, 2])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
      + "            DruidQueryRel(table=[druid.partsupp], scanFilter=[IS NOT NULL($2)], scanProject=[$2, $3, $4])\n"
      + "            DruidQueryRel(table=[druid.part], scanFilter=[AND(=($7, 37), LIKE($8, '%COPPER'), IS NOT NULL($5))], scanProject=[$3, $5])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$0, $1, $2, $3, $4, $5, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanProject=[$1, $2, $3])\n"
      + "      DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'EUROPE')], scanProject=[$2])\n"
      + "    DruidOuterQueryRel(group=[{0}], EXPR$0=[MIN($1)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1, 4])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[0, 2, 3])\n"
      + "            DruidQueryRel(table=[druid.partsupp], scanFilter=[IS NOT NULL($2)], scanProject=[$2, $3, $4])\n"
      + "            DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "          DruidQueryRel(table=[druid.nation], scanProject=[$2, $3])\n"
      + "        DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'EUROPE')], scanProject=[$2])\n";

  public static final String TPCH2_EXPLAIN_JR =
      "DruidOuterQueryRel(sort=[$0:DESC, $2:ASC, $1:ASC, $3:ASC], fetch=[100])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[5], outputColumns=[3, 6, 9, 2, 1, 4, 7, 5])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2, 0], rightKeys=[1, 1], outputColumns=[1, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanFilter=[IS NOT NULL($2)], scanProject=[$2, $3, $4])\n"
      + "        DruidOuterQueryRel(group=[{0}], EXPR$0=[MIN($1)])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 2])\n"
      + "            DruidQueryRel(table=[druid.partsupp], scanFilter=[IS NOT NULL($2)], scanProject=[$2, $3, $4])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
      + "              DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "              DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0])\n"
      + "                DruidQueryRel(table=[druid.nation], scanProject=[$2, $3])\n"
      + "                DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'EUROPE')], scanProject=[$2])\n"
      + "      DruidQueryRel(table=[druid.part], scanFilter=[AND(=($7, 37), LIKE($8, '%COPPER'), IS NOT NULL($5))], scanProject=[$3, $5])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[1], outputColumns=[0, 1, 2, 3, 5, 6, 7])\n"
      + "      DruidQueryRel(table=[druid.supplier], scanProject=[$0, $1, $2, $3, $4, $5, $6])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1])\n"
      + "        DruidQueryRel(table=[druid.nation], scanProject=[$1, $2, $3])\n"
      + "        DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'EUROPE')], scanProject=[$2])\n";

  public static final Object[][] TPCH2_RESULT = {
      {6820.35, "Supplier#000000007", "UNITED KINGDOM", "560", "Manufacturer#2", "s,4TicNGB4uO6PaSqNBUq", "33-990-965-2201", "s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit"},
      {3556.47, "Supplier#000000032", "UNITED KINGDOM", "381", "Manufacturer#5", "yvoD3TtZSx1skQNCK8agk5bZlZLug", "33-484-637-7873", "usly even depths. quickly ironic theodolites s"},
      {2972.26, "Supplier#000000016", "RUSSIA", "396", "Manufacturer#3", "YjP5C55zHDXL7LalK27zfQnwejdpin4AMpvh", "32-822-502-4215", "ously express ideas haggle quickly dugouts? fu"}
  };

  @Test
  public void tpch2() throws Exception
  {
    testQuery(TPCH2, TPCH2_EXPLAIN, TPCH2_RESULT);

    if (broadcastJoin) {
      if (semiJoin) {
        hook.verifyHooked(
            "ihTJr1UmqtiG+eDuEU3peQ==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY]}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string}}}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_SUPPKEY], groupingSets=Noop}, columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=true, hashSignature={PS_PARTKEY:dimension.string, PS_SUPPKEY:dimension.string, PS_SUPPLYCOST:double, P_MFGR:dimension.string, P_PARTKEY:dimension.string}}}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='nation', filter=(BloomFilter{fieldNames=[N_NATIONKEY], groupingSets=Noop} && N_REGIONKEY=='3'), columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, PS_SUPPLYCOST:double}}, $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}",
            "StreamQuery{dataSource='nation', filter=(BloomFilter{fieldNames=[N_NATIONKEY], groupingSets=Noop} && N_REGIONKEY=='3'), columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, PS_SUPPLYCOST:double}}, $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "FlMruFrf1lmOlpMoQIswlw==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY]}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string}}}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_SUPPKEY], groupingSets=Noop}, columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=true, hashSignature={PS_PARTKEY:dimension.string, PS_SUPPKEY:dimension.string, PS_SUPPLYCOST:double, P_MFGR:dimension.string, P_PARTKEY:dimension.string}}}",
            "StreamQuery{dataSource='nation', filter=BloomFilter{fieldNames=[N_NATIONKEY], groupingSets=Noop}, columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, PS_SUPPLYCOST:double}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='region', filter=(R_NAME=='EUROPE' && BloomFilter{fieldNames=[R_REGIONKEY], groupingSets=Noop}), columns=[R_REGIONKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_PHONE:dimension.string, PS_SUPPLYCOST:double, N_NAME:dimension.string, N_REGIONKEY:dimension.string}}, $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}",
            "StreamQuery{dataSource='region', filter=(R_NAME=='EUROPE' && BloomFilter{fieldNames=[R_REGIONKEY], groupingSets=Noop}), columns=[R_REGIONKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_PHONE:dimension.string, PS_SUPPLYCOST:double, N_NAME:dimension.string, N_REGIONKEY:dimension.string}}, $hash=true}"
        );
      }
    } else {
      if (semiJoin) {
        hook.verifyHooked(
            "JxpYve4b8Y50+UZ6At6nXw==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[381, 396, 501, 560]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[381, 396, 501, 560]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "PYYi//lEoeLROj2xLYa0+A==",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((P_SIZE=='37' && P_TYPE LIKE '%COPPER')), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=4}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomDimFilter.Factory{bloomSource=$view:region[R_REGIONKEY](R_NAME=='EUROPE'), fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
              "TimeseriesQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
              "TimeseriesQuery{dataSource='region', filter=R_NAME=='EUROPE', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[R_REGIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "5JzhSju4yPzjD7Q2YKvvbw==",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
              "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String TPCH3 =
      "SELECT"
      + "    L_ORDERKEY,"
      + "    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,"
      + "    O_ORDERDATE,"
      + "    O_SHIPPRIORITY"
      + " FROM"
      + "    customer, orders, lineitem"
      + " WHERE"
      + "    C_MKTSEGMENT = 'BUILDING' AND"
      + "    C_CUSTKEY = O_CUSTKEY AND"
      + "    L_ORDERKEY = O_ORDERKEY AND"
      + "    O_ORDERDATE < '1995-03-22' AND"
      + "    L_SHIPDATE > '1995-03-22'"
      + " GROUP BY"
      + "    L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY"
      + " ORDER BY"
      + "    revenue DESC, O_ORDERDATE"
      + " LIMIT 10";

  public static final String TPCH3_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, *($3, -(1, $4))], group=[{0, 1, 2}], revenue=[SUM($3)], aggregateProject=[$0, $3, $1, $2], sort=[$1:DESC, $2:ASC], fetch=[10])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[2], outputColumns=[5, 0, 2, 4, 3])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[2, 3, 4])\n"
      + "      DruidQueryRel(table=[druid.customer], scanFilter=[=($4, 'BUILDING')], scanProject=[$3])\n"
      + "      DruidQueryRel(table=[druid.orders], scanFilter=[<($3, '1995-03-22')], scanProject=[$2, $3, $4, $7])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[>($11, '1995-03-22')], scanProject=[$2, $3, $6])\n";

  public static final String TPCH3_EXPLAIN_JR =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, *($3, -(1, $4))], group=[{0, 1, 2}], revenue=[SUM($3)], aggregateProject=[$0, $3, $1, $2], sort=[$1:DESC, $2:ASC], fetch=[10])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[2], outputColumns=[5, 0, 2, 4, 3])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3])\n"
      + "      DruidQueryRel(table=[druid.orders], scanFilter=[<($3, '1995-03-22')], scanProject=[$2, $3, $4, $7])\n"
      + "      DruidQueryRel(table=[druid.customer], scanFilter=[=($4, 'BUILDING')], scanProject=[$3])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[>($11, '1995-03-22')], scanProject=[$2, $3, $6])\n";

  public static final Object[][] TPCH3_RESULT = {
      {"26304", 358077.0152279817D, "1995-03-20", 0L},
      {"928", 289800.9607996043D, "1995-03-02", 0L},
      {"4327", 187634.62862386403D, "1995-03-16", 0L},
      {"20453", 176905.6235388234D, "1995-03-11", 0L},
      {"20486", 171516.90596939923D, "1995-03-06", 0L},
      {"18820", 163812.8043091065D, "1995-02-12", 0L},
      {"16096", 147838.6416906625D, "1995-01-20", 0L},
      {"3749", 135109.43370970472D, "1995-02-24", 0L},
      {"19365", 126378.68876224649D, "1995-01-17", 0L},
      {"6560", 123264.19097787395D, "1995-01-05", 0L}
  };

  @Test
  public void tpch3() throws Exception
  {
    testQuery(TPCH3, TPCH3_EXPLAIN, TPCH3_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "FYHjAvKgjGFVvFtUiJhxhQ==",
          "StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY]}",
          "StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22} && InDimFilter{dimension='O_CUSTKEY', values=[1, 102, 103, 108, 109, 11, 113, 116, 121, 123, ..145 more]}), columns=[O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[orders]}, StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-03-22 < L_SHIPDATE} && InDimFilter{dimension='L_ORDERKEY', values=[10016, 10144, 10146, 10149, 10150, 10182, 1030, 10305, 10310, 10433, ..740 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
          "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-03-22 < L_SHIPDATE} && InDimFilter{dimension='L_ORDERKEY', values=[10016, 10144, 10146, 10149, 10150, 10182, 1030, 10305, 10310, 10433, ..740 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "1fWsP1F8bGV85swx3xc7Eg==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=(C_MKTSEGMENT=='BUILDING' && BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{O_ORDERDATE < 1995-03-22}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=3669}), columns=[C_CUSTKEY], $hash=true}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22} && BloomDimFilter.Factory{bloomSource=$view:customer[C_CUSTKEY](C_MKTSEGMENT=='BUILDING'), fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='O_CUSTKEY'}], groupingSets=Noop, maxNumEntries=155} && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](BoundDimFilter{1995-03-22 < L_SHIPDATE}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=16164}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=3669}]}",
            "TimeseriesQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=155}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=16164}]}",
            "StreamQuery{dataSource='customer', filter=(C_MKTSEGMENT=='BUILDING' && BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}), columns=[C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22} && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='O_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
        );
      } else {
        hook.verifyHooked(
            "/DGG1Y4rbcMR76X7+AJ9vw==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
            "StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
        );
      }
    }
  }

  public static final String TPCH4 =
      "SELECT"
      + "   O_ORDERPRIORITY, count(*) as order_count"
      + " FROM"
      + "   orders as o"
      + " WHERE"
      + "   O_ORDERDATE >= '1996-05-01' AND"
      + "   O_ORDERDATE < '1996-08-01' AND"
      + "   EXISTS ("
      + "     SELECT 1 FROM lineitem WHERE L_ORDERKEY = o.O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE"
      + "   )"
      + " GROUP BY O_ORDERPRIORITY"
      + " ORDER BY O_ORDERPRIORITY";

  public static final String TPCH4_EXPLAIN =
      "DruidOuterQueryRel(group=[{0}], order_count=[COUNT()], sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
      + "    DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1996-05-01'), <($3, '1996-08-01'), IS NOT NULL($4))], scanProject=[$4, $5])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(<($1, $9), IS NOT NULL($6))], scanProject=[$6], group=[{0}])\n";

  public static final Object[][] TPCH4_RESULT = {
      {"1-URGENT", 53L},
      {"2-HIGH", 40L},
      {"3-MEDIUM", 50L},
      {"4-NOT SPECIFIED", 59L},
      {"5-LOW", 53L}
  };

  @Test
  public void tpch4() throws Exception
  {
    testQuery(TPCH4, TPCH4_EXPLAIN, TPCH4_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "fMTWjynD08YNXTgJo63RfQ==",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01} && InDimFilter{dimension='O_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..6896 more]}), columns=[O_ORDERPRIORITY]}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01} && InDimFilter{dimension='O_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..6896 more]}), columns=[O_ORDERPRIORITY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "gIX4kOXICTX29bChTb+L3A==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL) && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, maxNumEntries=287}), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=287}]}",
            "StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL) && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop}), outputColumns=[d0]}"
        );
      } else {
        hook.verifyHooked(
            "zP/TKd50uEjcB/R8ie2rYA==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}"
        );
      }
    }
  }

  public static final String TPCH5 =
      "SELECT"
      + "    N_NAME,"
      + "    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE"
      + " FROM"
      + "    customer, orders, lineitem, supplier, nation, region"
      + " WHERE"
      + "    C_CUSTKEY = O_CUSTKEY AND"
      + "    L_ORDERKEY = O_ORDERKEY AND"
      + "    L_SUPPKEY = S_SUPPKEY AND"
      + "    C_NATIONKEY = S_NATIONKEY AND"
      + "    S_NATIONKEY = N_NATIONKEY AND"
      + "    N_REGIONKEY = R_REGIONKEY AND"
      + "    R_NAME = 'AFRICA' AND"
      + "    O_ORDERDATE >= '1993-01-01' AND"
      + "    O_ORDERDATE < '1994-01-01'"
      + " GROUP BY"
      + "    N_NAME"
      + " ORDER BY"
      + "    REVENUE DESC";

  public static final String TPCH5_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[$0, *($1, -(1, $2))], group=[{0}], REVENUE=[SUM($1)], sort=[$1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[2, 1, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3, 0], rightKeys=[1, 0], outputColumns=[1, 2, 4])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[2], outputColumns=[0, 2, 3, 5])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 3])\n"
      + "            DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "            DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-01-01'), <($3, '1994-01-01'))], scanProject=[$2, $4])\n"
      + "          DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $14])\n"
      + "        DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "      DruidQueryRel(table=[druid.nation], scanProject=[$1, $2, $3])\n"
      + "    DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AFRICA')], scanProject=[$2])\n";

  public static final String TPCH5_EXPLAIN_JR =
      "DruidOuterQueryRel(scanProject=[$0, *($1, -(1, $2))], group=[{0}], REVENUE=[SUM($1)], sort=[$1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2, 3], rightKeys=[1, 0], outputColumns=[4, 1, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 6])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $14])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 3, 4])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[0, 1, 2])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1])\n"
      + "            DruidQueryRel(table=[druid.nation], scanProject=[$1, $2, $3])\n"
      + "            DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AFRICA')], scanProject=[$2])\n"
      + "    DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-01-01'), <($3, '1994-01-01'))], scanProject=[$2, $4])\n";

  public static final Object[][] TPCH5_RESULT = {
      {"KENYA", 523154.4750718259D},
      {"MOROCCO", 218260.09096727896D},
      {"ETHIOPIA", 167163.61263319192D},
      {"ALGERIA", 157068.92618799844D},
      {"MOZAMBIQUE", 151814.8570359957D}
  };

  @Test
  public void tpch5() throws Exception
  {
    testQuery(TPCH5, TPCH5_EXPLAIN, TPCH5_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "giwXupZQOc6QFswM2lo9Bw==",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[customer]}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERKEY]}",
          "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
          "StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "BIUpxVODtiA0zT4wGC4xMw==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=1129}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01}, columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomDimFilter.Factory{bloomSource=$view:region[R_REGIONKEY](R_NAME=='AFRICA'), fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=1129}]}",
            "TimeseriesQuery{dataSource='region', filter=R_NAME=='AFRICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[R_REGIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
            "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01}, columns=[O_CUSTKEY, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "QAsOikOty9t2Px7oXGkBiQ==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01}, columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01}, columns=[O_CUSTKEY, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}"
        );
      }
    }
  }

  public static final String TPCH6 =
      "SELECT"
      + "    SUM(L_EXTENDEDPRICE * L_DISCOUNT) as REVENUE"
      + " FROM"
      + "    lineitem"
      + " WHERE"
      + "    L_SHIPDATE >= '1993-01-01' AND"
      + "    L_SHIPDATE < '1994-01-01' AND"
      + "    L_DISCOUNT BETWEEN 0.06 - 0.01 and 0.06 + 0.01 AND"
      + "    L_QUANTITY < 25";

  public static final String TPCH6_EXPLAIN =
      "DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1993-01-01'), <($11, '1994-01-01'), >=($2, 0.05:DECIMAL(4, 2)), <=($2, 0.07:DECIMAL(4, 2)), <($8, 25))], scanProject=[*($3, $2)], REVENUE=[SUM($0)])\n";

  public static final Object[][] TPCH6_RESULT = {{419743.2357189747}};

  @Test
  public void tpch6() throws Exception
  {
    testQuery(TPCH6, TPCH6_EXPLAIN, TPCH6_RESULT);

    hook.verifyHooked(
        "O0Hi2VcG5/cTI5Ge/0u79w==",
        "TimeseriesQuery{dataSource='lineitem', filter=(BoundDimFilter{L_QUANTITY < 25(numeric)} && BoundDimFilter{0.05 <= L_DISCOUNT <= 0.07(numeric)} && BoundDimFilter{1993-01-01 <= L_SHIPDATE < 1994-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * L_DISCOUNT)', inputType='double'}], outputColumns=[a0]}"
    );
  }

  protected static final String TPCH7 =
      "SELECT"
      + "    SUPP_NATION,"
      + "    CUST_NATION,"
      + "    L_YEAR,"
      + "    SUM(VOLUME) AS REVENUE"
      + " FROM"
      + "  ("
      + "    SELECT"
      + "        N1.N_NAME AS SUPP_NATION,"
      + "        N2.N_NAME AS CUST_NATION,"
      + "        YEAR(L_SHIPDATE) AS L_YEAR,"
      + "        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME"
      + "    FROM"
      + "        supplier, lineitem, orders, customer, nation N1, nation N2"
      + "    WHERE"
      + "        S_SUPPKEY = L_SUPPKEY AND" 
      + "        O_ORDERKEY = L_ORDERKEY AND" 
      + "        C_CUSTKEY = O_CUSTKEY AND" 
      + "        S_NATIONKEY = N1.N_NATIONKEY AND" 
      + "        C_NATIONKEY = N2.N_NATIONKEY AND " 
      + "        ("
      + "           (N1.N_NAME = 'KENYA' AND N2.N_NAME = 'PERU') OR" 
      + "           (N1.N_NAME = 'PERU' AND N2.N_NAME = 'KENYA')"
      + "        ) AND" 
      + "        L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'"
      + "  ) AS SHIPPING"
      + " GROUP BY"
      + "    SUPP_NATION, CUST_NATION, L_YEAR"
      + " ORDER BY"
      + "    SUPP_NATION, CUST_NATION, L_YEAR";

  public static final String TPCH7_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[OR(AND(=($0, 'KENYA'), =($1, 'PERU')), AND(=($0, 'PERU'), =($1, 'KENYA')))], scanProject=[$0, $1, YEAR($2), *($3, -(1, $4))], group=[{0, 1, 2}], REVENUE=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[4, 5, 2, 1, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0], outputColumns=[0, 1, 2, 3, 6])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[4], outputColumns=[0, 2, 3, 4, 5])\n"
      + "            DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "            DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1995-01-01'), <=($11, '1996-12-31'))], scanProject=[$2, $3, $6, $11, $14])\n"
      + "          DruidQueryRel(table=[druid.orders], scanProject=[$2, $4])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "      DruidQueryRel(table=[druid.nation], scanFilter=[OR(=($1, 'KENYA'), =($1, 'PERU'))], scanProject=[$1, $2])\n"
      + "    DruidQueryRel(table=[druid.nation], scanFilter=[OR(=($1, 'PERU'), =($1, 'KENYA'))], scanProject=[$1, $2])\n";

  public static final String TPCH7_EXPLAIN_JR =
      "DruidOuterQueryRel(scanFilter=[OR(AND(=($0, 'KENYA'), =($1, 'PERU')), AND(=($0, 'PERU'), =($1, 'KENYA')))], scanProject=[$0, $1, YEAR($2), *($3, -(1, $4))], group=[{0, 1, 2}], REVENUE=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[2], outputColumns=[6, 1, 5, 3, 2])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 3])\n"
      + "      DruidQueryRel(table=[druid.orders], scanProject=[$2, $4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[0, 2])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanFilter=[OR(=($1, 'PERU'), =($1, 'KENYA'))], scanProject=[$1, $2])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0], outputColumns=[0, 1, 2, 3, 6])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1995-01-01'), <=($11, '1996-12-31'))], scanProject=[$2, $3, $6, $11, $14])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2])\n"
      + "        DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanFilter=[OR(=($1, 'KENYA'), =($1, 'PERU'))], scanProject=[$1, $2])\n";

  public static final Object[][] TPCH7_RESULT = {
      {"KENYA", "PERU", 1995L, 155808.41736393946D},
      {"KENYA", "PERU", 1996L, 335577.4810472459D},
      {"PERU", "KENYA", 1995L, 243818.19482950834D},
      {"PERU", "KENYA", 1996L, 105976.7651234877D}
  };

  @Test
  public void tpch7() throws Exception
  {
    testQuery(TPCH7, TPCH7_EXPLAIN, TPCH7_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "CpbfNuxiYrLBif9WfBwFvw==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, MaterializedQuery{dataSource=[orders]}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
            "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "YiuSu4U149rVSFR641vLrA==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[supplier]}, StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[orders]}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
            "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      }
    } else {
      if (broadcastJoin) {
        if (bloomFilter) {
          hook.verifyHooked(
              "Ehcgv/wNqz5UGzt7lD7U9w==",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}), fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop, maxNumEntries=2}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "dl85XOsXI4RbuA4f9v75rg==",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "LRIiI35297KN2T7PG0Iriw==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}), fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop, maxNumEntries=2}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}), fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop, maxNumEntries=2}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "TimeseriesQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "StreamQuery{dataSource='supplier', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "DfFk/t/ijNbEYQAo3AV24g==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String TPCH8 =
      "SELECT"
      + "    O_YEAR,"
      + "    SUM(case when NATION = 'ROMANIA' then VOLUME else 0 end) / SUM(VOLUME) as MKT_SHARE"
      + " FROM"
      + "   ("
      + "    SELECT"
      + "       YEAR(O_ORDERDATE) as O_YEAR,"
      + "       L_EXTENDEDPRICE * (1 - L_DISCOUNT) as VOLUME,"
      + "       N2.N_NAME as NATION"
      + "    FROM"
      + "       part, supplier, lineitem, orders, customer, nation N1, nation N2, region"
      + "    WHERE"
      + "       P_PARTKEY = L_PARTKEY AND" 
      + "       S_SUPPKEY = L_SUPPKEY AND" 
      + "       L_ORDERKEY = O_ORDERKEY AND" 
      + "       O_CUSTKEY = C_CUSTKEY AND" 
      + "       C_NATIONKEY = N1.N_NATIONKEY AND" 
      + "       N1.N_REGIONKEY = R_REGIONKEY AND" 
      + "       R_NAME = 'AMERICA' AND" 
      + "       S_NATIONKEY = N2.N_NATIONKEY AND" 
      + "       O_ORDERDATE BETWEEN '1995-01-01' and '1996-12-31' AND" 
      + "       P_TYPE = 'ECONOMY BURNISHED NICKEL'"
      + "   ) AS ALL_NATIONS"
      + " GROUP BY O_YEAR"
      + " ORDER BY O_YEAR";

  public static final String TPCH8_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[YEAR($0), CASE(=($1, 'ROMANIA'), *($2, -(1, $3)), 0:DOUBLE), *($2, -(1, $3))], group=[{0}], agg#0=[SUM($1)], agg#1=[SUM($2)], aggregateProject=[$0, /($1, $2)], sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[2, 4, 1, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2, 3, 4, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0], outputColumns=[0, 1, 2, 3, 6])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 4, 6])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[2], outputColumns=[0, 1, 2, 4, 5])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[1], outputColumns=[6, 0, 1, 2])\n"
      + "              DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0])\n"
      + "                DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $7, $14])\n"
      + "                DruidQueryRel(table=[druid.part], scanFilter=[=($8, 'ECONOMY BURNISHED NICKEL')], scanProject=[$5])\n"
      + "              DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "            DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1995-01-01'), <=($3, '1996-12-31'))], scanProject=[$2, $3, $4])\n"
      + "          DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanProject=[$2, $3])\n"
      + "      DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
      + "    DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AMERICA')], scanProject=[$2])\n";

  public static final String TPCH8_EXPLAIN_JR =
      "DruidOuterQueryRel(scanProject=[YEAR($0), CASE(=($1, 'ROMANIA'), *($2, -(1, $3)), 0:DOUBLE), *($2, -(1, $3))], group=[{0}], agg#0=[SUM($1)], agg#1=[SUM($2)], aggregateProject=[$0, /($1, $2)], sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[4, 3, 1, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 4])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $7, $14])\n"
      + "        DruidQueryRel(table=[druid.part], scanFilter=[=($8, 'ECONOMY BURNISHED NICKEL')], scanProject=[$5])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2])\n"
      + "        DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2])\n"
      + "      DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1995-01-01'), <=($3, '1996-12-31'))], scanProject=[$2, $3, $4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0])\n"
      + "          DruidQueryRel(table=[druid.nation], scanProject=[$2, $3])\n"
      + "          DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AMERICA')], scanProject=[$2])\n";

  public static final Object[][] TPCH8_RESULT = {
      {1995L, 0.15367145767949628D},
      {1996L, 0.3838133760159879D}
  };

  @Test
  public void tpch8() throws Exception
  {
    testQuery(TPCH8, TPCH8_EXPLAIN, TPCH8_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "/p74CF4fuS8SaXolZ7mQ+A==",
            "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31} && InDimFilter{dimension='O_ORDERKEY', values=[10114, 10242, 10245, 103, 10310, 10338, 10498, 10658, 10851, 11040, ..199 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
            "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31} && InDimFilter{dimension='O_ORDERKEY', values=[10114, 10242, 10245, 103, 10310, 10338, 10498, 10658, 10851, 11040, ..199 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}",
            "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "3tQ604/F8SvNvREhjTmJKw==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[customer]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "EJ3DxHHBsXdoo4EB2JYwoQ==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[customer]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      }
    } else {
      if (broadcastJoin) {
        if (bloomFilter) {
          hook.verifyHooked(
              "3+ZgYTEELicUxBoIb9PP9g==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "0oyl3JgDKobLmMHapYjx3A==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        }
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "6p6N0NNSWaEdKr6KjkD5ZA==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](P_TYPE=='ECONOMY BURNISHED NICKEL'), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=7} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "TimeseriesQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=7}]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "YBnZUHPV5ZO3cSNBXdbAXQ==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String TPCH9 =
      "SELECT"
      + "    NATION,"
      + "    O_YEAR,"
      + "    SUM(AMOUNT) as SUM_PROFIT"
      + " FROM"
      + "    ("
      + "        SELECT"
      + "            N_NAME as NATION,"
      + "            YEAR(O_ORDERDATE) as O_YEAR,"
      + "            L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY as AMOUNT"
      + "        FROM"
      + "            part, supplier, lineitem, partsupp, orders, nation"
      + "        WHERE"
      + "            S_SUPPKEY = L_SUPPKEY AND" 
      + "            PS_SUPPKEY = L_SUPPKEY AND" 
      + "            PS_PARTKEY = L_PARTKEY AND" 
      + "            P_PARTKEY = L_PARTKEY AND" 
      + "            O_ORDERKEY = L_ORDERKEY AND" 
      + "            S_NATIONKEY = N_NATIONKEY AND" 
      + "            P_NAME LIKE '%plum%'"
      + "    ) AS PROFIT"
      + " GROUP BY"
      + "    NATION, O_YEAR"
      + " ORDER BY"
      + "    NATION, O_YEAR DESC";

  public static final String TPCH9_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[$0, YEAR($1), -(*($2, -(1, $3)), *($4, $5))], group=[{0, 1}], SUM_PROFIT=[SUM($2)], sort=[$0:ASC, $1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[6, 5, 2, 1, 4, 3])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[0, 1, 2, 4, 5, 6])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[6, 4], rightKeys=[1, 0], outputColumns=[0, 1, 2, 3, 5, 9])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[6, 0, 1, 2, 3, 4, 5])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1])\n"
      + "            DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $7, $8, $14])\n"
      + "            DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "          DruidQueryRel(table=[druid.part], scanFilter=[LIKE($4, '%plum%')], scanProject=[$5])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3, $4])\n"
      + "      DruidQueryRel(table=[druid.orders], scanProject=[$3, $4])\n"
      + "    DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n";

  public static final String TPCH9_EXPLAIN_JR =
      "DruidOuterQueryRel(scanProject=[$0, YEAR($1), -(*($2, -(1, $3)), *($4, $5))], group=[{0, 1}], SUM_PROFIT=[SUM($2)], sort=[$0:ASC, $1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[4, 6, 1, 0, 5, 3])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[5, 3], rightKeys=[1, 0], outputColumns=[0, 1, 2, 4, 6, 9])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 5, 6])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 5, 7])\n"
      + "          DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $7, $8, $14])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2])\n"
      + "            DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "            DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
      + "        DruidQueryRel(table=[druid.part], scanFilter=[LIKE($4, '%plum%')], scanProject=[$5])\n"
      + "      DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3, $4])\n"
      + "    DruidQueryRel(table=[druid.orders], scanProject=[$3, $4])\n";

  public static final Object[][] TPCH9_RESULT = {
      {"ALGERIA", 1998L, 114041.26288207977D},
      {"ALGERIA", 1997L, 420005.52036544395D},
      {"ALGERIA", 1996L, 179435.92888346483D},
      {"ALGERIA", 1995L, 582584.3183930825D},
      {"ALGERIA", 1994L, 460802.8492547316D},
      {"ALGERIA", 1993L, 358757.83164962556D},
      {"ALGERIA", 1992L, 196711.9832104973D},
      {"ARGENTINA", 1998L, 108010.48797064778D},
      {"ARGENTINA", 1997L, 38692.829331616085D},
      {"ARGENTINA", 1996L, 56161.95963088006D},
      {"ARGENTINA", 1995L, 206313.39825887277D},
      {"ARGENTINA", 1994L, 138654.5828448139D},
      {"ARGENTINA", 1993L, 130070.15644093126D},
      {"ARGENTINA", 1992L, 249187.62255413557D},
      {"BRAZIL", 1997L, 275368.15491910925D},
      {"BRAZIL", 1996L, 121697.60100621039D},
      {"BRAZIL", 1995L, 180382.19198142894D},
      {"BRAZIL", 1994L, 135981.81329815474D},
      {"BRAZIL", 1993L, 92990.53442113772D},
      {"BRAZIL", 1992L, 132591.4652138137D},
      {"CANADA", 1998L, 203860.59596039643D},
      {"CANADA", 1997L, 321769.0730324793D},
      {"CANADA", 1996L, 171418.1153069712D},
      {"CANADA", 1995L, 335742.06347199733D},
      {"CANADA", 1994L, 111252.62846753643D},
      {"CANADA", 1993L, 195046.82483966634D},
      {"CANADA", 1992L, 290137.69375805295D},
      {"CHINA", 1998L, 172477.83204571763D},
      {"CHINA", 1997L, 275949.41903671867D},
      {"CHINA", 1996L, 262160.8688651954D},
      {"CHINA", 1995L, 311497.508512106D},
      {"CHINA", 1994L, 163460.95307904793D},
      {"CHINA", 1993L, 180435.7027487494D},
      {"CHINA", 1992L, 330379.5508661473D},
      {"EGYPT", 1998L, 21087.458950298802D},
      {"EGYPT", 1997L, 103924.71502282038D},
      {"EGYPT", 1996L, 100910.742700351D},
      {"EGYPT", 1995L, 79938.60535488653D},
      {"EGYPT", 1994L, 187349.30304467538D},
      {"EGYPT", 1993L, 330374.5263726049D},
      {"EGYPT", 1992L, 280424.466604101D},
      {"ETHIOPIA", 1998L, 194613.56218396308D},
      {"ETHIOPIA", 1997L, 220107.2526853096D},
      {"ETHIOPIA", 1996L, 158622.32201870752D},
      {"ETHIOPIA", 1995L, 146433.78034954268D},
      {"ETHIOPIA", 1994L, 223731.00827797133D},
      {"ETHIOPIA", 1993L, 392406.4595612766D},
      {"ETHIOPIA", 1992L, 120304.05524324537D},
      {"GERMANY", 1998L, 106323.37565803423D},
      {"GERMANY", 1997L, 92601.54000000001D},
      {"GERMANY", 1996L, 198944.05598116555D},
      {"GERMANY", 1995L, 165687.04067567264D},
      {"GERMANY", 1994L, 226676.94357343647D},
      {"GERMANY", 1993L, 141024.68808797604D},
      {"GERMANY", 1992L, 293949.9785120052D},
      {"INDIA", 1998L, 126584.38706637183D},
      {"INDIA", 1997L, 242388.40911733187D},
      {"INDIA", 1996L, 263227.1670390725D},
      {"INDIA", 1995L, 205509.06789985023D},
      {"INDIA", 1994L, 361137.83027029823D},
      {"INDIA", 1993L, 283929.86687772715D},
      {"INDIA", 1992L, 341885.8311905579D},
      {"INDONESIA", 1998L, 274430.051628585D},
      {"INDONESIA", 1997L, 465366.50635826826D},
      {"INDONESIA", 1996L, 500014.3016792602D},
      {"INDONESIA", 1995L, 424459.6305658971D},
      {"INDONESIA", 1994L, 346039.43092811655D},
      {"INDONESIA", 1993L, 450136.56378824974D},
      {"INDONESIA", 1992L, 602251.4145831331D},
      {"IRAN", 1998L, 131147.61823914346D},
      {"IRAN", 1997L, 87582.15097435769D},
      {"IRAN", 1996L, 95232.70604957585D},
      {"IRAN", 1995L, 115417.67062810008D},
      {"IRAN", 1994L, 190750.94539575386D},
      {"IRAN", 1993L, 78173.58147189885D},
      {"IRAN", 1992L, 9445.441430400575D},
      {"IRAQ", 1998L, 64116.222065007096D},
      {"IRAQ", 1997L, 53046.80409555316D},
      {"IRAQ", 1996L, 98945.09816294358D},
      {"IRAQ", 1994L, -791.6299371585264D},
      {"IRAQ", 1993L, 112985.29805446045D},
      {"IRAQ", 1992L, 90281.52294340223D},
      {"JAPAN", 1998L, 134707.8442967207D},
      {"JAPAN", 1997L, 187434.71473944632D},
      {"JAPAN", 1996L, 130783.96095723027D},
      {"JAPAN", 1995L, 245886.58956717473D},
      {"JAPAN", 1994L, 96861.93096909915D},
      {"JAPAN", 1993L, 91508.39774376526D},
      {"JAPAN", 1992L, 319633.41638581344D},
      {"JORDAN", 1998L, 84023.6913846031D},
      {"JORDAN", 1997L, 248273.9293701095D},
      {"JORDAN", 1996L, 303736.13496585406D},
      {"JORDAN", 1995L, 269849.5180936673D},
      {"JORDAN", 1994L, 82437.45704854291D},
      {"JORDAN", 1993L, 290887.21199729946D},
      {"JORDAN", 1992L, 275791.7712003958D},
      {"KENYA", 1998L, 74049.85009683364D},
      {"KENYA", 1997L, 311392.6744855124D},
      {"KENYA", 1996L, 185216.46649997216D},
      {"KENYA", 1995L, 80162.49574048087D},
      {"KENYA", 1994L, 302921.19203382055D},
      {"KENYA", 1993L, 325086.96649505536D},
      {"KENYA", 1992L, 343416.7854685216D},
      {"MOROCCO", 1998L, 119855.49339878328D},
      {"MOROCCO", 1997L, 290008.633373567D},
      {"MOROCCO", 1996L, 14184.126619798131D},
      {"MOROCCO", 1995L, 69843.47769951589D},
      {"MOROCCO", 1994L, 191099.55208847D},
      {"MOROCCO", 1993L, 137202.08287584715D},
      {"MOROCCO", 1992L, 66594.12967929707D},
      {"MOZAMBIQUE", 1998L, 117097.67474634536D},
      {"MOZAMBIQUE", 1997L, 363205.03742464836D},
      {"MOZAMBIQUE", 1996L, 311449.27169638564D},
      {"MOZAMBIQUE", 1995L, 473208.3954721531D},
      {"MOZAMBIQUE", 1994L, 442759.0845858489D},
      {"MOZAMBIQUE", 1993L, 440542.9893679537D},
      {"MOZAMBIQUE", 1992L, 287795.5268082155D},
      {"PERU", 1998L, 102725.66279401527D},
      {"PERU", 1997L, 171472.8264013625D},
      {"PERU", 1996L, 294416.15261718613D},
      {"PERU", 1995L, 112348.73268373786D},
      {"PERU", 1994L, 95837.18593006683D},
      {"PERU", 1993L, 138317.5969789736D},
      {"PERU", 1992L, 85667.16847534657D},
      {"ROMANIA", 1998L, 2421.287401699462D},
      {"ROMANIA", 1997L, 102189.50098745801D},
      {"ROMANIA", 1996L, 81265.36594303243D},
      {"ROMANIA", 1995L, 47749.04802742277D},
      {"ROMANIA", 1994L, 35394.23633686883D},
      {"ROMANIA", 1993L, 42641.98851210192D},
      {"ROMANIA", 1992L, 49277.804907966856D},
      {"RUSSIA", 1998L, 548958.6482764423D},
      {"RUSSIA", 1997L, 466773.9098531575D},
      {"RUSSIA", 1996L, 901266.033027536D},
      {"RUSSIA", 1995L, 803254.3646324247D},
      {"RUSSIA", 1994L, 932974.120513519D},
      {"RUSSIA", 1993L, 843491.4803470027D},
      {"RUSSIA", 1992L, 876831.2496177027D},
      {"UNITED KINGDOM", 1998L, 81480.06686721236D},
      {"UNITED KINGDOM", 1997L, 58282.63452262785D},
      {"UNITED KINGDOM", 1996L, 134110.58770714886D},
      {"UNITED KINGDOM", 1995L, 83918.57284126579D},
      {"UNITED KINGDOM", 1994L, 70544.89821118998D},
      {"UNITED KINGDOM", 1993L, 55681.24707224962D},
      {"UNITED KINGDOM", 1992L, 31602.86316145718D},
      {"UNITED STATES", 1998L, 196681.86753583292D},
      {"UNITED STATES", 1997L, 311459.7029831422D},
      {"UNITED STATES", 1996L, 451144.57652934256D},
      {"UNITED STATES", 1995L, 481350.94638033805D},
      {"UNITED STATES", 1994L, 473742.82106392196D},
      {"UNITED STATES", 1993L, 324866.8118531974D},
      {"UNITED STATES", 1992L, 343496.2652782098D},
      {"VIETNAM", 1998L, 198132.13110275078D},
      {"VIETNAM", 1997L, 426951.29134074517D},
      {"VIETNAM", 1996L, 610135.1674077166D},
      {"VIETNAM", 1995L, 316695.8518692686D},
      {"VIETNAM", 1994L, 489111.94819765206D},
      {"VIETNAM", 1993L, 343970.28961719247D},
      {"VIETNAM", 1992L, 352275.066762814D}
  };

  @Test
  public void tpch9() throws Exception
  {
    testQuery(TPCH9, TPCH9_EXPLAIN, TPCH9_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "fw7reEYRXbg43rban9b60A==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
            "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, StreamQuery{dataSource='partsupp', filter=InDimsFilter{dimensions=[PS_SUPPKEY, PS_PARTKEY], values=[[1, 722], [1, 773], [1, 800], [10, 194], [10, 209], [10, 275], [10, 467], [10, 659], [10, 733], [10, 966], [..176 more]]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='partsupp', filter=InDimsFilter{dimensions=[PS_SUPPKEY, PS_PARTKEY], values=[[1, 722], [1, 773], [1, 800], [10, 194], [10, 209], [10, 275], [10, 467], [10, 659], [10, 733], [10, 966], [..176 more]]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "0GVeM5U3ganT9f+bMYiW8Q==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && InDimFilter{dimension='L_PARTKEY', values=[104, 118, 181, 186, 194, 209, 219, 263, 264, 275, ..39 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "6xlQd2p6rcuehD8ZE4OCtg==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+supplier, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+supplier, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}], $hash=true}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "30ejIEBzrA+Y3KoGTN9uDQ==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](P_NAME LIKE '%plum%'), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=49}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
              "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE '%plum%', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=49}]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "4/nV8vn8yQQDTr85mDNoyg==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
              "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String TPCH10 =
      "SELECT"
      + "    C_CUSTKEY,"
      + "    C_NAME,"
      + "    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as REVENUE,"
      + "    C_ACCTBAL,"
      + "    N_NAME,"
      + "    C_ADDRESS,"
      + "    C_PHONE,"
      + "    C_COMMENT"
      + " FROM"
      + "    customer, orders, lineitem, nation"
      + " WHERE"
      + "    C_CUSTKEY = O_CUSTKEY"
      + "    AND L_ORDERKEY = O_ORDERKEY"
      + "    AND O_ORDERDATE >= '1993-07-01'"
      + "    AND O_ORDERDATE < '1993-10-01'"
      + "    AND L_RETURNFLAG = 'R'"
      + "    AND C_NATIONKEY = N_NATIONKEY"
      + " GROUP BY"
      + "    C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT"
      + " ORDER BY"
      + "    REVENUE DESC"
      + " LIMIT 20";

  public static final String TPCH10_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, $3, $4, $5, $6, *($7, -(1, $8))], group=[{0, 1, 2, 3, 4, 5, 6}], REVENUE=[SUM($7)], aggregateProject=[$0, $1, $7, $2, $4, $5, $3, $6], sort=[$2:DESC], fetch=[20])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1], outputColumns=[3, 4, 0, 6, 9, 1, 2, 8, 7])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[2], outputColumns=[0, 1, 2, 3, 4, 5, 6, 8, 9])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 5, 6, 8])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$0, $1, $2, $3, $5, $6, $7])\n"
      + "        DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-07-01'), <($3, '1993-10-01'))], scanProject=[$2, $4])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[=($10, 'R')], scanProject=[$2, $3, $6])\n"
      + "    DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n";

  public static final String TPCH10_EXPLAIN_JR =
      "DruidOuterQueryRel(scanProject=[$0, $1, $2, $3, $4, $5, $6, *($7, -(1, $8))], group=[{0, 1, 2, 3, 4, 5, 6}], REVENUE=[SUM($7)], aggregateProject=[$0, $1, $7, $2, $4, $5, $3, $6], sort=[$2:DESC], fetch=[20])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[2], outputColumns=[3, 4, 0, 5, 6, 1, 2, 9, 8])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 5, 6, 8])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1], outputColumns=[0, 1, 2, 3, 4, 6, 7])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$0, $1, $2, $3, $5, $6, $7])\n"
      + "        DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
      + "      DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-07-01'), <($3, '1993-10-01'))], scanProject=[$2, $4])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[=($10, 'R')], scanProject=[$2, $3, $6])\n";

  public static final Object[][] TPCH10_RESULT = {
      {"22", "Customer#000000022", 376659.33794523217D, 591.98D, "CANADA", "QI6p41,FNs5k7RZoCCVPUTkUdYpB", "13-806-545-9701", "s nod furiously above the furiously ironic ideas. "},
      {"217", "Customer#000000217", 337546.50298096647D, 378.33D, "UNITED KINGDOM", "YIy05RMdthrXqdfnNKud", "33-159-298-3849", "ven frays wake according to the carefully "},
      {"715", "Customer#000000715", 327733.10233764054D, 85.05D, "ROMANIA", "9qLvF42uxUarKl4I 2pEKOMNJmo8Ro5EK", "29-500-408-6392", "hins boost quickly. quickly regular epitaphs haggle fluffily quickly bold pinto beans. regular"},
      {"55", "Customer#000000055", 325304.2184793751D, 4572.11D, "IRAN", "zIRBR4KNEl HzaiV3a i9n6elrxzDEh8r8pDom", "20-180-440-8525", "ully unusual packages wake bravely bold packages. unusual requests boost deposits! blithely ironic packages ab"},
      {"19", "Customer#000000019", 295856.25229804683D, 8914.71D, "CHINA", "uc,3bHIx84H,wdrmLOjVsiqXCq2tr", "28-396-526-5053", " nag. furiously careful packages are slyly at the accounts. furiously regular in"},
      {"686", "Customer#000000686", 284498.96677950415D, 5503.36D, "FRANCE", "1j C80VWHe ITCVCV", "16-682-293-3599", " even deposits print quickly. foxes wake. furiously ironic asymptotes across the bold foxes"},
      {"202", "Customer#000000202", 280435.6192224468D, 2237.64D, "GERMANY", "Q0uJ1frCbi9yvu", "17-905-805-4635", "fully along the carefully pending Tiresias; special packages along the carefully special deposits try to"},
      {"679", "Customer#000000679", 268885.680341735D, 1394.44D, "IRAN", "IJf1FlZL9I9m,rvofcoKy5pRUOjUQV", "20-146-696-9508", "ely pending frays boost carefully"},
      {"448", "Customer#000000448", 260133.3756423737D, 8117.27D, "UNITED STATES", "BH4vtnDpabk0NgoGNJWu4OUXnidfJ", "34-985-422-6009", "unts. final pinto beans boost carefully. furiously even foxes according to the express, regular pa"},
      {"394", "Customer#000000394", 245405.0088580988D, 5200.96D, "UNITED KINGDOM", "nxW1jt,MQvImdr z72gAt1bslnfEipCh,bKZN", "33-422-600-6936", " instructions. carefully special ideas after the fluffily unusual r"},
      {"64", "Customer#000000064", 245401.5889329308D, -646.64D, "CANADA", "MbCeGY20kaKK3oalJD,OT", "13-558-731-7204", "structions after the quietly ironic theodolites cajole be"},
      {"559", "Customer#000000559", 243818.187256828D, 5872.94D, "GERMANY", "A3ACFoVbP,gPe xknVJMWC,wmRxb Nmg fWFS,UP", "17-395-429-6655", "al accounts cajole carefully across the accounts. furiously pending pinto beans across the "},
      {"586", "Customer#000000586", 242057.2150677127D, 5134.35D, "IRAQ", "vGaA9XBtn,hlswFhSjLIXGlLEDD2flE8UXwj", "21-239-369-7791", "above the blithely express ideas. slyly r"},
      {"721", "Customer#000000721", 234606.65694861457D, 3420.64D, "VIETNAM", "N6hr4gV9EkPBuE3Ayu ", "31-174-552-2949", "ar instructions. packages haggle stealthily ironic deposits. even platelets detect quickly. even sheaves along"},
      {"65", "Customer#000000065", 228551.89613367125D, 8795.16D, "UNITED KINGDOM", "RGT yzQ0y4l0H90P783LG4U95bXQFDRXbWa1sl,X", "33-733-623-5267", "y final foxes serve carefully. theodolites are carefully. pending i"},
      {"352", "Customer#000000352", 226905.6798173411D, 6257.88D, "INDONESIA", "HqhIE5GRTK0dFtWpJUQENU4aa1bwdsUBEWtzUw", "19-906-158-8420", "ts are. blithely special requests wake. furiously bold packages among the blithely eve"},
      {"79", "Customer#000000079", 220721.16073114896D, 5121.28D, "MOROCCO", "n5hH2ftkVRwW8idtD,BmM2", "25-147-850-4166", "es. packages haggle furiously. regular, special requests poach after the quickly express ideas. blithely pending re"},
      {"710", "Customer#000000710", 217848.30989936687D, 7412.12D, "RUSSIA", "OCLSZuXw1AEK NLvlofMkuK,YNe,bJD40a", "32-459-427-9559", "ges integrate express, even ideas"},
      {"484", "Customer#000000484", 213702.96280260698D, 4245.0D, "SAUDI ARABIA", "ismzlUzrqRMRGWmCEUUjkBsi", "30-777-953-8902", "y against the express, even packages. blithely pending pearls haggle furiously above the fur"},
      {"292", "Customer#000000292", 203414.1759173521D, 2975.43D, "IRAQ", "hCXh3vxC4uje9", "21-457-910-2923", "usly regular, ironic accounts. blithely regular platelets are carefully. blithely unusual ideas affi"}
  };

  @Test
  public void tpch10() throws Exception
  {
    testQuery(TPCH10, TPCH10_EXPLAIN, TPCH10_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "vhFeRwpBhefLkTSWuK5v0g==",
          "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=(InDimFilter{dimension='C_CUSTKEY', values=[1, 115, 121, 122, 124, 127, 128, 13, 131, 136, ..195 more]} && InDimFilter{dimension='C_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}), columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, MaterializedQuery{dataSource=[orders]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
          "StreamQuery{dataSource='customer', filter=(InDimFilter{dimension='C_CUSTKEY', values=[1, 115, 121, 122, 124, 127, 128, 13, 131, 136, ..195 more]} && InDimFilter{dimension='C_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}), columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
          "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "Vcx+KQam6cQ8zDBd0fbuuA==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=286}, columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=286}]}",
            "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "E4YSrAdtmo1dPiq3PhhO8w==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
            "StreamQuery{dataSource='customer', columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      }
    }
  }

  public static final String TPCH11 =
      "SELECT * FROM ("
      + "  SELECT"
      + "    PS_PARTKEY,"
      + "    SUM(PS_SUPPLYCOST * PS_AVAILQTY) as PART_VALUE"
      + "  FROM"
      + "    partsupp, supplier, nation"
      + "  WHERE"
      + "    PS_SUPPKEY = S_SUPPKEY"
      + "    AND S_NATIONKEY = N_NATIONKEY"
      + "    AND N_NAME = 'GERMANY'"
      + "  GROUP BY"
      + "    PS_PARTKEY"
      + " ) AS inner_query"
      + " WHERE"
      + "  PART_VALUE > ("
      + "    SELECT"
      + "      SUM(PS_SUPPLYCOST * PS_AVAILQTY)"
      + "    FROM"
      + "      partsupp,"
      + "      supplier,"
      + "      nation"
      + "    WHERE"
      + "      PS_SUPPKEY = S_SUPPKEY"
      + "      AND S_NATIONKEY = N_NATIONKEY"
      + "      AND N_NAME = 'GERMANY'"
      + "  ) * 0.0001"
      + " ORDER BY PART_VALUE DESC";

  public static final String TPCH11_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[>($1, *($2, 0.0001:DECIMAL(5, 4)))], scanProject=[$0, $1], sort=[$1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER])\n"
      + "    DruidOuterQueryRel(scanProject=[$0, *($1, $2)], group=[{0}], PART_VALUE=[SUM($1)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[1, 2, 0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[0, 1, 3, 4])\n"
      + "          DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $2, $3, $4])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$2])\n"
      + "    DruidOuterQueryRel(scanProject=[*($0, $1)], EXPR$0=[SUM($0)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[0, 2, 3])\n"
      + "          DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $3, $4])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$2])\n";

  public static final String TPCH11_EXPLAIN_JR =
      "DruidOuterQueryRel(scanFilter=[>($1, *($2, 0.0001:DECIMAL(5, 4)))], scanProject=[$0, $1], sort=[$1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER])\n"
      + "    DruidOuterQueryRel(scanProject=[$0, *($1, $2)], group=[{0}], PART_VALUE=[SUM($1)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 3, 0])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $2, $3, $4])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "          DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$2])\n"
      + "    DruidOuterQueryRel(scanProject=[*($0, $1)], EXPR$0=[SUM($0)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 0])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $3, $4])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "          DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$2])\n";

  public static final Object[][] TPCH11_RESULT = {
      {"657", 7952582.180000001D}, {"527", 7799597.1899999995D}, {"570", 7665801.33D}, {"93", 7645677.04D},
      {"187", 7363442.4D}, {"715", 7215116.4799999995D}, {"549", 7115227.5D}, {"143", 7037648.64D},
      {"989", 6970889.640000001D}, {"69", 6970845.15D}, {"115", 6706677.33D}, {"668", 6702794.699999999D},
      {"943", 6458641.7D}, {"619", 6381886.600000001D}, {"751", 6334687.3D}, {"493", 6144995.15D},
      {"730", 6100067.88D}, {"693", 5819806.220000001D}, {"19", 5593808.0D}, {"386", 5475929.1899999995D},
      {"82", 5434136.829999999D}, {"942", 5409015.8D}, {"182", 5191583.7299999995D}, {"543", 5189101.68D},
      {"743", 5039271.42D}, {"993", 4973063.04D}, {"732", 4932809.82D}, {"163", 4901152.25D},
      {"211", 4879076.01D}, {"516", 4676882.0D}, {"216", 4561143.8D}, {"826", 4410041.96D},
      {"895", 4345555.02D}, {"597", 4341311.640000001D}, {"482", 4315726.819999999D}, {"198", 4315049.0D},
      {"403", 4283221.600000001D}, {"981", 4259584.13D}, {"442", 4107626.12D}, {"521", 3913058.16D},
      {"864", 3728780.16D}, {"582", 3717165.4000000004D}, {"837", 3640184.0100000002D}, {"423", 3602031.8000000003D},
      {"293", 3463498.8D}, {"902", 3459429.6D}, {"832", 3447746.46D}, {"922", 3387665.52D},
      {"933", 3368071.5D}, {"682", 3333963.96D}, {"140", 3155253.92D}, {"394", 3118101.5599999996D},
      {"167", 3034999.03D}, {"382", 3014555.96D}, {"451", 2926228.56D}, {"7", 2834026.8D},
      {"789", 2782828.96D}, {"704", 2761460.85D}, {"422", 2710606.7800000003D}, {"970", 2702851.9099999997D},
      {"250", 2647099.56D}, {"43", 2587359.58D}, {"717", 2574344.56D}, {"443", 2382150.05D},
      {"882", 2321004.56D}, {"245", 2319450.9D}, {"346", 2318315.7600000002D}, {"480", 2316197.2399999998D},
      {"608", 2225519.48D}, {"856", 2211867.0D}, {"328", 2208471.1900000004D}, {"8", 2172003.9D},
      {"848", 2136067.6D}, {"132", 2125100.6799999997D}, {"621", 2115063.27D}, {"67", 2107088.16D},
      {"265", 2093560.02D}, {"193", 2077700.32D}, {"118", 2059808.86D}, {"298", 2026981.74D},
      {"355", 2010156.61D}, {"559", 1987422.84D}, {"782", 1866799.55D}, {"574", 1797527.3D},
      {"80", 1740096.97D}, {"532", 1682311.48D}, {"243", 1603235.16D}, {"762", 1594238.3D},
      {"893", 1533657.58D}, {"643", 1512838.8D}, {"393", 1484779.2000000002D}, {"129", 1450282.92D},
      {"412", 1446605.2D}, {"276", 1414562.67D}, {"632", 1408087.26D}, {"46", 1361500.28D},
      {"104", 1340876.41D}, {"292", 1327114.13D}, {"741", 1270376.7999999998D}, {"227", 1235815.6800000002D},
      {"259", 1178367.68D}, {"793", 1081096.2D}, {"932", 980620.6799999999D}, {"325", 971188.85D},
      {"32", 894484.09D}, {"809", 858034.3400000001D}, {"962", 846619.25D}, {"885", 836872.4D},
      {"874", 834634.7100000001D}, {"20", 832802.32D}, {"374", 729413.96D}, {"490", 720683.6D},
      {"178", 695176.94D}, {"433", 680530.32D}, {"339", 654195.71D}, {"563", 643298.76D},
      {"375", 601371.5499999999D}, {"843", 589964.5499999999D}, {"755", 554786.94D}, {"469", 499663.14999999997D},
      {"903", 446803.76D}, {"913", 425798.64999999997D}, {"634", 420249.7D}, {"363", 400413.19999999995D},
      {"706", 392540.80000000005D}, {"1000", 384893.60000000003D}, {"593", 381772.8D}, {"815", 377968.08D},
      {"432", 370528.51999999996D}, {"472", 295403.24D}, {"152", 278490.62D}, {"610", 272143.99D},
      {"766", 259972.93D}, {"31", 236051.49D}, {"307", 224806.82D}, {"538", 223747.85000000003D},
      {"804", 222522.30000000002D}, {"232", 211879.92D}, {"778", 204061.15000000002D}, {"586", 185146.12D},
      {"314", 171835.30000000002D}, {"982", 155171.73D}, {"234", 151548.0D}, {"54", 138952.64D},
      {"951", 127107.90000000001D}, {"510", 125887.92D}, {"332", 112181.3D}, {"56", 69545.7D},
      {"343", 56511.840000000004D}, {"461", 54348.78D}
  };

  @Test
  public void tpch11() throws Exception
  {
    testQuery(TPCH11, TPCH11_EXPLAIN, TPCH11_RESULT);

    if (broadcastJoin) {
      hook.verifyHooked(
          "tLWCMQFMO5gL2GSTFUFT5w==",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY]}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY]}",
          "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}]}",
          "TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}",
          "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}]}"
      );
    } else {
      if (semiJoin) {
        hook.verifyHooked(
            "PEBTEBtvvrEpSaT917jeiA==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "wsyUvM+7TYovXmRy/auAnQ==",
              "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](N_NAME=='GERMANY'), fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](N_NAME=='GERMANY'), fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}]}",
              "TimeseriesQuery{dataSource='nation', filter=N_NAME=='GERMANY', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "TimeseriesQuery{dataSource='nation', filter=N_NAME=='GERMANY', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}",
              "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "H2yt3nQ9nGunjE+Wqk/xLw==",
              "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}",
              "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NATIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String TPCH12 =
      "SELECT"
      + "    L_SHIPMODE,"
      + "    sum(case"
      + "        when O_ORDERPRIORITY = '1-URGENT'"
      + "            or O_ORDERPRIORITY = '2-HIGH'"
      + "            then 1"
      + "        else 0"
      + "    end) as high_line_count,"
      + "    sum(case"
      + "        when O_ORDERPRIORITY <> '1-URGENT'"
      + "            and O_ORDERPRIORITY <> '2-HIGH'"
      + "            then 1"
      + "        else 0"
      + "    end) as low_line_count"
      + " FROM"
      + "    orders,"
      + "    lineitem"
      + " WHERE"
      + "    O_ORDERKEY = L_ORDERKEY"
      + "    and L_SHIPMODE in ('REG AIR', 'MAIL')"
      + "    and L_COMMITDATE < L_RECEIPTDATE"
      + "    and L_SHIPDATE < L_COMMITDATE"
      + "    and L_RECEIPTDATE >= '1995-01-01'"
      + "    and L_RECEIPTDATE < '1996-01-01'"
      + " group by"
      + "    L_SHIPMODE"
      + " order by"
      + "    L_SHIPMODE";

  public static final String TPCH12_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[$0, CASE(OR(=($1, '1-URGENT'), =($1, '2-HIGH')), 1, 0), CASE(AND(<>($1, '1-URGENT'), <>($1, '2-HIGH')), 1, 0), IS TRUE(OR(=($1, '1-URGENT'), =($1, '2-HIGH'))), IS TRUE(AND(<>($1, '1-URGENT'), <>($1, '2-HIGH')))], group=[{0}], high_line_count=[COUNT() FILTER $3], low_line_count=[COUNT() FILTER $4], sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[3, 1])\n"
      + "    DruidQueryRel(table=[druid.orders], scanProject=[$4, $5])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(OR(=($13, 'REG AIR'), =($13, 'MAIL')), <($1, $9), <($11, $1), >=($9, '1995-01-01'), <($9, '1996-01-01'))], scanProject=[$6, $13])\n";

  public static final Object[][] TPCH12_RESULT = {{"MAIL", 34L, 44L}, {"REG AIR", 37L, 43L}};

  @Test
  public void tpch12() throws Exception
  {
    testQuery(TPCH12, TPCH12_EXPLAIN, TPCH12_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "Dm0SJuLpt8LHpEHcVIhxCQ==",
          "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=InDimFilter{dimension='O_ORDERKEY', values=[10145, 10213, 10535, 10624, 10695, 10884, 11270, 11584, 11936, 12037, ..137 more]}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}, MaterializedQuery{dataSource=[lineitem]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
          "StreamQuery{dataSource='orders', filter=InDimFilter{dimension='O_ORDERKEY', values=[10145, 10213, 10535, 10624, 10695, 10884, 11270, 11584, 11936, 12037, ..137 more]}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "CoWFYxlfgzypfvpnxl5Gag==",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY]((InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01})), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=158}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}, StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
            "TimeseriesQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01}), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=158}]}",
            "StreamQuery{dataSource='orders', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "/e2K10J4fWcM4nU4e/1Ajg==",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', columns=[O_ORDERKEY, O_ORDERPRIORITY]}, StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
            "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, O_ORDERPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}"
        );
      }
    }
  }

  public static final String TPCH13 =
      "SELECT"
      + "    c_count,"
      + "    count(*) as custdist"
      + " FROM ("
      + "   SELECT"
      + "      C_CUSTKEY,"
      + "      count(O_ORDERKEY) as c_count"
      + "   FROM"
      + "      customer left outer join orders on C_CUSTKEY = O_CUSTKEY and O_COMMENT not like '%unusual%accounts%'"
      + "   GROUP BY"
      + "      C_CUSTKEY"
      + " ) c_orders"
      + " GROUP BY c_count"
      + " ORDER BY custdist desc, c_count desc";

  public static final String TPCH13_EXPLAIN =
      "DruidOuterQueryRel(group=[{0}], custdist=[COUNT()], sort=[$1:DESC, $0:DESC])\n"
      + "  DruidOuterQueryRel(group=[{0}], c_count=[COUNT($1)], aggregateProject=[$1])\n"
      + "    DruidJoinRel(joinType=[LEFT], leftKeys=[0], rightKeys=[0], outputColumns=[0, 2])\n"
      + "      DruidQueryRel(table=[druid.customer], scanProject=[$3])\n"
      + "      DruidQueryRel(table=[druid.orders], scanFilter=[NOT(LIKE($1, '%unusual%accounts%'))], scanProject=[$2, $4])\n";

  public static final Object[][] TPCH13_RESULT = {
      {0L, 250L}, {12L, 34L}, {11L, 34L}, {19L, 29L}, {14L, 29L}, {8L, 28L}, {7L, 26L}, {20L, 25L},
      {17L, 24L}, {13L, 24L}, {9L, 24L}, {18L, 23L}, {16L, 23L}, {15L, 22L}, {21L, 21L}, {10L, 19L},
      {6L, 18L}, {23L, 17L}, {22L, 17L}, {27L, 9L}, {26L, 9L}, {24L, 9L}, {5L, 9L}, {4L, 8L},
      {25L, 6L}, {30L, 4L}, {29L, 4L}, {28L, 2L}, {3L, 2L}, {2L, 1L}
  };

  @Test
  public void tpch13() throws Exception
  {
    testQuery(TPCH13, TPCH13_EXPLAIN, TPCH13_RESULT);

    hook.verifyHooked(
        "hNsblffx2h8XYVnM7R9U2w==",
        "GroupByQuery{dataSource='GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY]}, StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0', fieldName='O_ORDERKEY'}], outputColumns=[a0]}', dimensions=[DefaultDimensionSpec{dimension='a0', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=descending}], limit=-1}, outputColumns=[d0, _a0]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY]}, StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0', fieldName='O_ORDERKEY'}], outputColumns=[a0]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY]}",
        "StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}"
    );
  }

  public static final String TPCH14 =
      "SELECT"
      + "    100.00 * sum(case when P_TYPE like 'PROMO%' then L_EXTENDEDPRICE * (1 - L_DISCOUNT) else 0 end) / sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as promo_revenue"
      + " FROM"
      + "    lineitem, part"
      + " WHERE"
      + "    L_PARTKEY = P_PARTKEY AND"
      + "    L_SHIPDATE >= '1995-08-01' AND"
      + "    L_SHIPDATE < '1995-09-01'";

  public static final String TPCH14_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[CASE(LIKE($0, 'PROMO%'), *($1, -(1, $2)), 0:DOUBLE), *($1, -(1, $2))], agg#0=[SUM($0)], agg#1=[SUM($1)], aggregateProject=[/(*(100.00:DECIMAL(5, 2), $0), $1)])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[4, 1, 0])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1995-08-01'), <($11, '1995-09-01'))], scanProject=[$2, $3, $7])\n"
      + "    DruidQueryRel(table=[druid.part], scanProject=[$5, $8])\n";

  public static final Object[][] TPCH14_RESULT = {{21.62198225363824}};

  @Test
  public void tpch14() throws Exception
  {
    testQuery(TPCH14, TPCH14_EXPLAIN, TPCH14_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "DKhutQLG0UffDJ5CMtRYZw==",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, StreamQuery{dataSource='part', filter=InDimFilter{dimension='P_PARTKEY', values=[102, 107, 109, 11, 110, 113, 12, 123, 124, 125, ..335 more]}, columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
          "StreamQuery{dataSource='part', filter=InDimFilter{dimension='P_PARTKEY', values=[102, 107, 109, 11, 110, 113, 12, 123, 124, 125, ..335 more]}, columns=[P_PARTKEY, P_TYPE]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "EplswJqLLXQiyM7m7XFlLg==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}, StreamQuery{dataSource='part', filter=BloomDimFilter.Factory{bloomSource=$view:lineitem[L_PARTKEY](BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}), fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop, maxNumEntries=408}, columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=408}]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='part', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop}, columns=[P_PARTKEY, P_TYPE]}"
        );
      } else {
        hook.verifyHooked(
            "WHKDXFb7ddHmjsHLa2mq0A==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}, StreamQuery{dataSource='part', columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='part', columns=[P_PARTKEY, P_TYPE]}"
        );
      }
    }
  }

  public static final String TPCH15 =
      "WITH revenue_view AS ("
      + "  SELECT"
      + "    L_SUPPKEY AS SUPPLIER_NO,"
      + "    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE"
      + "  FROM"
      + "    lineitem"
      + "  WHERE"
      + "    L_SHIPDATE >= '1996-01-01'"
      + "    AND L_SHIPDATE < '1996-04-01'"
      + "  GROUP BY"
      + "    L_SUPPKEY)"
      + " SELECT"
      + "  S_SUPPKEY,"
      + "  S_NAME,"
      + "  S_ADDRESS,"
      + "  S_PHONE,"
      + "  TOTAL_REVENUE"
      + " FROM"
      + "  supplier,"
      + "  revenue_view"
      + " WHERE"
      + "  S_SUPPKEY = SUPPLIER_NO"
      + "  AND TOTAL_REVENUE = ("
      + "    SELECT"
      + "      MAX(TOTAL_REVENUE)"
      + "    FROM"
      + "      revenue_view"
      + "    )"
      + " ORDER BY"
      + "  S_SUPPKEY";

  public static final String TPCH15_EXPLAIN =
      "DruidOuterQueryRel(sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0], outputColumns=[3, 1, 0, 2, 4])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 5])\n"
      + "      DruidQueryRel(table=[druid.supplier], scanProject=[$1, $3, $5, $6])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)])\n"
      + "    DruidOuterQueryRel(EXPR$0=[MAX($0)])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)], aggregateProject=[$1])\n";

  public static final String TPCH15_EXPLAIN_JR =
      "DruidOuterQueryRel(sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[3], outputColumns=[5, 3, 2, 4, 1])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 1])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)])\n"
      + "      DruidOuterQueryRel(EXPR$0=[MAX($0)])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)], aggregateProject=[$1])\n"
      + "    DruidQueryRel(table=[druid.supplier], scanProject=[$1, $3, $5, $6])\n";

  public static final Object[][] TPCH15_RESULT = {
      {"6", "Supplier#000000006", "tQxuVm7s7CnK", "24-696-997-4969", 1080265.1420867585D}
  };

  @Test
  public void tpch15() throws Exception
  {
    testQuery(TPCH15, TPCH15_EXPLAIN, TPCH15_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "JRoq/qkzxTOWdyB8NcOfPA==",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, sampleRatio=0.050}",
          "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[supplier]}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=(BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}, TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0], $hash=true}], timeColumnName=__time}', columns=[S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, a0], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=(BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}",
          "TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0], $hash=true}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}"
      );
    } else {
      hook.verifyHooked(
          "tnUE3u83cvgT7zjUsEJ0dQ==",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, sampleRatio=0.050}",
          "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}, TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0], $hash=true}], timeColumnName=__time}', columns=[S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, a0], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}",
          "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}",
          "TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0], $hash=true}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}"
      );
    }
  }

  public static final String TPCH16 =
      "SELECT"
      + "    P_BRAND,"
      + "    P_TYPE,"
      + "    P_SIZE,"
      + "    count(distinct PS_SUPPKEY) as supplier_cnt"
      + " FROM"
      + "    partsupp, part"
      + " WHERE"
      + "    P_PARTKEY = PS_PARTKEY and"
      + "    P_BRAND <> 'Brand#34' and"
      + "    P_TYPE not like 'ECONOMY BRUSHED%' and"
      + "    P_SIZE in (22, 14, 27, 49, 21, 33, 35, 28) and"
      + "    PS_SUPPKEY not in ("
      + "        SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT like '%Customer%Complaints%'"
      + "    )"
      + " GROUP BY"
      + "    P_BRAND, P_TYPE, P_SIZE"
      + " ORDER BY"
      + "    supplier_cnt desc, P_BRAND, P_TYPE, P_SIZE";

  public static final String TPCH16_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[OR(=($4, 0), AND(IS NULL($5), >=($6, $4), IS NOT NULL($3)))], scanProject=[$0, $1, $2, $3], group=[{0, 1, 2}], supplier_cnt=[COUNT(DISTINCT $3)], sort=[$3:DESC, $0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[LEFT], leftKeys=[1], rightKeys=[0], outputColumns=[2, 5, 4, 1, 6, 9, 7])\n"
      + "    DruidJoinRel(joinType=[INNER])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3])\n"
      + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(OR(=($7, 22), =($7, 14), =($7, 27), =($7, 49), =($7, 21), =($7, 33), =($7, 35), =($7, 28)), <>($0, 'Brand#34'), NOT(LIKE($8, 'ECONOMY BRUSHED%')))], scanProject=[$0, $5, $7, $8])\n"
      + "      DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6], c=[COUNT()], ck=[COUNT($0)])\n"
      + "    DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6, true], group=[{0, 1}])\n";

  public static final String TPCH16_EXPLAIN_JR =
      "DruidOuterQueryRel(scanFilter=[OR(=($4, 0), AND(IS NULL($5), >=($6, $4), IS NOT NULL($3)))], scanProject=[$0, $1, $2, $3], group=[{0, 1, 2}], supplier_cnt=[COUNT(DISTINCT $3)], sort=[$3:DESC, $0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[6, 9, 8, 1, 4, 3, 5])\n"
      + "    DruidJoinRel(joinType=[INNER])\n"
      + "      DruidJoinRel(joinType=[LEFT], leftKeys=[1], rightKeys=[0])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3])\n"
      + "        DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6, true], group=[{0, 1}])\n"
      + "      DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6], c=[COUNT()], ck=[COUNT($0)])\n"
      + "    DruidQueryRel(table=[druid.part], scanFilter=[AND(OR(=($7, 22), =($7, 14), =($7, 27), =($7, 49), =($7, 21), =($7, 33), =($7, 35), =($7, 28)), <>($0, 'Brand#34'), NOT(LIKE($8, 'ECONOMY BRUSHED%')))], scanProject=[$0, $5, $7, $8])\n";

  @Test
  public void tpch16() throws Exception
  {
    testQuery(TPCH16, TPCH16_EXPLAIN);

    if (semiJoin) {
      hook.verifyHooked(
          "NSsMaveAMTt/MP7z/wpYXQ==",
          "StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[105, 106, 111, 116, 127, 134, 14, 150, 155, 162, ..139 more]}, columns=[PS_PARTKEY, PS_SUPPKEY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d2'}], filter=((a0=='0' || d1==NULL) && (a0=='0' || MathExprFilter{expression='(a1 >= a0)'}) && (a0=='0' || !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fieldNames=[PS_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _d2, _a0]}",
          "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[105, 106, 111, 116, 127, 134, 14, 150, 155, 162, ..139 more]}, columns=[PS_PARTKEY, PS_SUPPKEY]}",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
          "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "u7LKMPxdTS8AZHSVa28pLw==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%'))), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=149}, columns=[PS_PARTKEY, PS_SUPPKEY]}, StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d2'}], filter=((a0=='0' || d1==NULL) && (a0=='0' || MathExprFilter{expression='(a1 >= a0)'}) && (a0=='0' || !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fieldNames=[PS_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _d2, _a0]}",
            "TimeseriesQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=149}]}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY]}",
            "StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
            "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
            "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "C/CWT+zUoAWHk01VN5V44A==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY]}, StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d2'}], filter=((a0=='0' || d1==NULL) && (a0=='0' || MathExprFilter{expression='(a1 >= a0)'}) && (a0=='0' || !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fieldNames=[PS_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _d2, _a0]}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY]}",
            "StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
            "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
            "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
        );
      }
    }
  }

  public static final String TPCH17 =
      "WITH Q17_PART AS ("
      + "  SELECT P_PARTKEY FROM part WHERE"
      + "  P_BRAND = 'Brand#31'"      // changed 23 to 31
      + "  AND P_CONTAINER = 'MED BOX'"
      + "),"
      + "Q17_AVG AS ("
      + "  SELECT L_PARTKEY AS T_PARTKEY, 0.2 * AVG(L_QUANTITY) AS T_AVG_QUANTITY"
      + "  FROM lineitem"
      + "  WHERE L_PARTKEY IN (SELECT P_PARTKEY FROM Q17_PART)"
      + "  GROUP BY L_PARTKEY"
      + "),"
      + "Q17_PRICE AS ("
      + "  SELECT"
      + "    L_QUANTITY,"
      + "    L_PARTKEY,"
      + "    L_EXTENDEDPRICE"
      + "  FROM lineitem"
      + "  WHERE L_PARTKEY IN (SELECT P_PARTKEY FROM Q17_PART)"
      + ")"
      + "SELECT"
      + "    SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY"
      + " FROM"
      + "    Q17_AVG, Q17_PRICE"
      + " WHERE"
      + "    T_PARTKEY = L_PARTKEY AND L_QUANTITY < T_AVG_QUANTITY";

  public static final String TPCH17_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[<($1, $2)], scanProject=[$0], agg#0=[SUM($0)], aggregateProject=[/($0, 7.0:DECIMAL(2, 1))])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[4, 2, 1])\n"
      + "    DruidOuterQueryRel(group=[{0}], agg#0=[AVG($1)], aggregateProject=[$0, *(0.2:DECIMAL(2, 1), $1)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$7, $8])\n"
      + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 1, 0])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$3, $7, $8])\n"
      + "      DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n";

  public static final String TPCH17_EXPLAIN_JR =
      "DruidOuterQueryRel(scanFilter=[<($1, $2)], scanProject=[$0], agg#0=[SUM($0)], aggregateProject=[/($0, 7.0:DECIMAL(2, 1))])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 0, 4])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 1, 0])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$3, $7, $8])\n"
      + "      DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n"
      + "    DruidOuterQueryRel(group=[{0}], agg#0=[AVG($1)], aggregateProject=[$0, *(0.2:DECIMAL(2, 1), $1)])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$7, $8])\n"
      + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n";

  public static final Object[][] TPCH17_RESULT = {{4923.785714285714D}};

  @Test
  public void tpch17() throws Exception
  {
    testQuery(TPCH17, TPCH17_EXPLAIN, TPCH17_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "/qE2HKobtceg7C8yipTRmQ==",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0]}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0]}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}, StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_QUANTITY, L_PARTKEY, L_EXTENDEDPRICE]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < p0)'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}",
          "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_QUANTITY, L_PARTKEY, L_EXTENDEDPRICE]}"
      );
    } else {
      hook.verifyHooked(
          "5INUUVgklOsrgDS6t3DQFg==",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}, CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < p0)'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}",
          "StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}",
          "StreamQuery{dataSource='lineitem', columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}"
      );
    }
  }

  public static final String TPCH17_2 =
      "SELECT"
      + "  SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY"
      + " FROM"
      + "  lineitem,"
      + "  part"
      + " WHERE"
      + "  P_PARTKEY = L_PARTKEY"
      + "  AND P_BRAND = 'Brand#31'"
      + "  AND P_CONTAINER = 'MED BOX'"
      + "  AND L_QUANTITY < ("
      + "    SELECT"
      + "      0.2 * AVG(L_QUANTITY)"
      + "    FROM"
      + "      lineitem"
      + "    WHERE"
      + "      L_PARTKEY = P_PARTKEY"
      + "  )";

  public static final String TPCH17_2_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[<($1, *(0.2:DECIMAL(2, 1), $2))], scanProject=[$0], agg#0=[SUM($0)], aggregateProject=[/($0, 7.0:DECIMAL(2, 1))])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1, 4])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 2, 3])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[IS NOT NULL($7)], scanProject=[$3, $7, $8])\n"
      + "      DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'), IS NOT NULL($5))], scanProject=[$5])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[IS NOT NULL($7)], scanProject=[$7, $8], group=[{0}], agg#0=[AVG($1)])\n";

  @Test
  public void tpch17_2() throws Exception
  {
    testQuery(TPCH17_2, TPCH17_2_EXPLAIN, TPCH17_RESULT);

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "sebj1wpzsr1IOlX+zutvdg==",
            "StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY]}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), sampleRatio=0.050}",
            "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, $hash=true}",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=(!(L_PARTKEY==NULL) && InDimFilter{dimension='L_PARTKEY', values=[558, 855]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < (0.2B * a0))'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='_a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(_a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=(!(L_PARTKEY==NULL) && InDimFilter{dimension='L_PARTKEY', values=[558, 855]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}"
        );
      } else {
        hook.verifyHooked(
            "MHJy8O0qEdOrPfk7StBsvA==",
            "StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY], $hash=true}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), sampleRatio=0.050}",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && InDimFilter{dimension='L_PARTKEY', values=[558, 855]}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < (0.2B * a0))'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='_a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(_a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
            "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && InDimFilter{dimension='L_PARTKEY', values=[558, 855]}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "GIirHl/VGTw87+J8v7ShkQ==",
            "StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY]}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), sampleRatio=0.050}",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < (0.2B * a0))'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='_a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(_a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
            "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "fWsEyftFlr89JRA4CsME6g==",
              "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), sampleRatio=0.050}",
              "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX')), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=2}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < (0.2B * a0))'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='_a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(_a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
              "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
              "StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY], $hash=true}",
              "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}"
          );
        } else {
          hook.verifyHooked(
              "9mJRFnPSjTMo1Pah+90b1Q==",
              "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), sampleRatio=0.050}",
              "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=!(L_PARTKEY==NULL), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < (0.2B * a0))'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='_a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(_a0 / 7.0B)', finalize=true}], outputColumns=[p0]}",
              "StreamQuery{dataSource='lineitem', filter=!(L_PARTKEY==NULL), columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
              "StreamQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX' && !(P_PARTKEY==NULL)), columns=[P_PARTKEY], $hash=true}",
              "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], filter=!(L_PARTKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count', fieldName='L_QUANTITY'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[d0, a0]}"
          );
        }
      }
    }
  }

  public static final String TPCH18 =
      "SELECT  C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY)"
      + " FROM"
      + "  customer,"
      + "  orders,"
      + "  lineitem"
      + " WHERE"
      + "  O_ORDERKEY IN ( SELECT L_ORDERKEY FROM lineitem GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY) > 300 )"
      + "  AND C_CUSTKEY = O_CUSTKEY"
      + "  AND O_ORDERKEY = L_ORDERKEY"
      + " GROUP BY"
      + "  C_NAME,"
      + "  C_CUSTKEY,"
      + "  O_ORDERKEY,"
      + "  O_ORDERDATE,"
      + "  O_TOTALPRICE"
      + " ORDER BY"
      + "  O_TOTALPRICE DESC,"
      + "  O_ORDERDATE,"
      + "  O_ORDERKEY"
      + " LIMIT 100";

  public static final String TPCH18_EXPLAIN =
      "DruidOuterQueryRel(group=[{0, 1, 2, 3, 4}], EXPR$5=[SUM($5)], sort=[$4:DESC, $3:ASC, $2:ASC], fetch=[100])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[1, 0, 3, 2, 4, 5])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 6])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1, 3, 4, 5])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $5])\n"
      + "        DruidQueryRel(table=[druid.orders], scanProject=[$2, $3, $4, $8])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8], group=[{0}], agg#0=[SUM($1)], aggregateFilter=[>($1, 300)], aggregateProject=[$0])\n";

  public static final String TPCH18_EXPLAIN_JR =
      "DruidOuterQueryRel(group=[{0, 1, 2, 3, 4}], EXPR$5=[SUM($5)], sort=[$4:DESC, $3:ASC, $2:ASC], fetch=[100])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[4, 3, 1, 0, 2, 6])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 2, 3, 4, 5])\n"
      + "        DruidQueryRel(table=[druid.orders], scanProject=[$2, $3, $4, $8])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $5])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8], group=[{0}], agg#0=[SUM($1)], aggregateFilter=[>($1, 300)], aggregateProject=[$0])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8])\n";

  public static final Object[][] TPCH18_RESULT = {
      {"Customer#000000334", "334", "29158", "1995-10-21", 441562.47D, 305L},
      {"Customer#000000089", "89", "6882", "1997-04-09", 389430.93D, 303L}
  };

  @Test
  public void tpch18() throws Exception
  {
    testQuery(TPCH18, TPCH18_EXPLAIN, TPCH18_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "hJBlmTfOuvoZkuCs45IpRg==",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[customer]}, StreamQuery{dataSource='orders', filter=(InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]} && InDimFilter{dimension='O_ORDERKEY', values=[29158, 6882]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d1'}, DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='d2'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d3'}, DefaultDimensionSpec{dimension='O_TOTALPRICE', outputName='d4'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d4', direction=descending}, OrderByColumnSpec{dimension='d3', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=100}, outputColumns=[d0, d1, d2, d3, d4, a0]}",
          "StreamQuery{dataSource='orders', filter=(InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]} && InDimFilter{dimension='O_ORDERKEY', values=[29158, 6882]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}",
          "StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}"
      );
    } else {
      hook.verifyHooked(
          "MFrxitnwSnAsvDT8ea95Aw==",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d1'}, DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='d2'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d3'}, DefaultDimensionSpec{dimension='O_TOTALPRICE', outputName='d4'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d4', direction=descending}, OrderByColumnSpec{dimension='d3', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=100}, outputColumns=[d0, d1, d2, d3, d4, a0]}",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}",
          "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}",
          "StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0], $hash=true}"
      );
    }
  }

  public static final String TPCH19 =
      "SELECT"
      + "    SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE"
      + " FROM"
      + "    lineitem,"
      + "    part"
      + " WHERE"
      + "    ("
      + "        P_PARTKEY = L_PARTKEY"
      + "        AND P_BRAND = 'Brand#32'"
      + "        AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')"
      + "        AND L_QUANTITY >= 7 AND L_QUANTITY <= 7 + 10"
      + "        AND P_SIZE BETWEEN 1 AND 5"
      + "        AND L_SHIPMODE IN ('AIR', 'AIR REG')"
      + "        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'"
      + "    )"
      + "    OR"
      + "    ("
      + "        P_PARTKEY = L_PARTKEY"
      + "        AND P_BRAND = 'Brand#35'"
      + "        AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')"
      + "        AND L_QUANTITY >= 15 AND L_QUANTITY <= 15 + 10"
      + "        AND P_SIZE BETWEEN 1 AND 10"
      + "        AND L_SHIPMODE IN ('AIR', 'AIR REG')"
      + "        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'"
      + "    )"
      + "    OR"
      + "    ("
      + "        P_PARTKEY = L_PARTKEY"
      + "        AND P_BRAND = 'Brand#24'"
      + "        AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')"
      + "        AND L_QUANTITY >= 26 AND L_QUANTITY <= 26 + 10"
      + "        AND P_SIZE BETWEEN 1 AND 15"
      + "        AND L_SHIPMODE IN ('AIR', 'AIR REG')"
      + "        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'"
      + "    )";

  public static final String TPCH19_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[OR(AND(=($2, 'Brand#32'), OR(=($3, 'SM CASE'), =($3, 'SM BOX'), =($3, 'SM PACK'), =($3, 'SM PKG')), >=($4, 7), <=($4, 17), <=($5, 5)), AND(=($2, 'Brand#35'), OR(=($3, 'MED BAG'), =($3, 'MED BOX'), =($3, 'MED PKG'), =($3, 'MED PACK')), >=($4, 15), <=($4, 25), <=($5, 10)), AND(=($2, 'Brand#24'), OR(=($3, 'LG CASE'), =($3, 'LG BOX'), =($3, 'LG PACK'), =($3, 'LG PKG')), >=($4, 26), <=($4, 36), <=($5, 15)))], scanProject=[*($0, -(1, $1))], REVENUE=[SUM($0)])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[2], outputColumns=[1, 0, 4, 5, 3, 7])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(OR(=($13, 'AIR'), =($13, 'AIR REG')), =($12, 'DELIVER IN PERSON'))], scanProject=[$2, $3, $7, $8])\n"
      + "    DruidQueryRel(table=[druid.part], scanFilter=[>=($7, 1)], scanProject=[$0, $2, $5, $7])\n";

  @Test
  public void tpch19() throws Exception
  {
    testQuery(TPCH19, TPCH19_EXPLAIN);

    if (semiJoin) {
      hook.verifyHooked(
          "p+gPfySlPkCgUbOiTngRXQ==",
          "StreamQuery{dataSource='part', filter=BoundDimFilter{1 <= P_SIZE(numeric)}, columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON' && InDimFilter{dimension='L_PARTKEY', values=[1, 10, 100, 1000, 101, 102, 103, 104, 105, 106, ..990 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}', filter=((P_BRAND=='Brand#32' && InDimFilter{dimension='P_CONTAINER', values=[SM BOX, SM CASE, SM PACK, SM PKG]} && BoundDimFilter{P_SIZE <= 5(numeric)} && BoundDimFilter{7 <= L_QUANTITY <= 17(numeric)}) || (P_BRAND=='Brand#35' && InDimFilter{dimension='P_CONTAINER', values=[MED BAG, MED BOX, MED PACK, MED PKG]} && BoundDimFilter{P_SIZE <= 10(numeric)} && BoundDimFilter{15 <= L_QUANTITY <= 25(numeric)}) || (P_BRAND=='Brand#24' && InDimFilter{dimension='P_CONTAINER', values=[LG BOX, LG CASE, LG PACK, LG PKG]} && BoundDimFilter{P_SIZE <= 15(numeric)} && BoundDimFilter{26 <= L_QUANTITY <= 36(numeric)})), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}",
          "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON' && InDimFilter{dimension='L_PARTKEY', values=[1, 10, 100, 1000, 101, 102, 103, 104, 105, 106, ..990 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "fG3htgAn7npAtEvcYzsnNg==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON' && BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](BoundDimFilter{1 <= P_SIZE(numeric)}), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=1000}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, StreamQuery{dataSource='part', filter=(BoundDimFilter{1 <= P_SIZE(numeric)} && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_PARTKEY]((InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON')), fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop, maxNumEntries=1039}), columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}], timeColumnName=__time}', filter=((P_BRAND=='Brand#32' && InDimFilter{dimension='P_CONTAINER', values=[SM BOX, SM CASE, SM PACK, SM PKG]} && BoundDimFilter{P_SIZE <= 5(numeric)} && BoundDimFilter{7 <= L_QUANTITY <= 17(numeric)}) || (P_BRAND=='Brand#35' && InDimFilter{dimension='P_CONTAINER', values=[MED BAG, MED BOX, MED PACK, MED PKG]} && BoundDimFilter{P_SIZE <= 10(numeric)} && BoundDimFilter{15 <= L_QUANTITY <= 25(numeric)}) || (P_BRAND=='Brand#24' && InDimFilter{dimension='P_CONTAINER', values=[LG BOX, LG CASE, LG PACK, LG PKG]} && BoundDimFilter{P_SIZE <= 15(numeric)} && BoundDimFilter{26 <= L_QUANTITY <= 36(numeric)})), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}",
            "TimeseriesQuery{dataSource='part', filter=BoundDimFilter{1 <= P_SIZE(numeric)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=1000}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON'), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=1039}]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON' && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
            "StreamQuery{dataSource='part', filter=(BoundDimFilter{1 <= P_SIZE(numeric)} && BloomFilter{fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop}), columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "J29XJ+aG/V0zCLyKcmV3ew==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON'), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, StreamQuery{dataSource='part', filter=BoundDimFilter{1 <= P_SIZE(numeric)}, columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}], timeColumnName=__time}', filter=((P_BRAND=='Brand#32' && InDimFilter{dimension='P_CONTAINER', values=[SM BOX, SM CASE, SM PACK, SM PKG]} && BoundDimFilter{P_SIZE <= 5(numeric)} && BoundDimFilter{7 <= L_QUANTITY <= 17(numeric)}) || (P_BRAND=='Brand#35' && InDimFilter{dimension='P_CONTAINER', values=[MED BAG, MED BOX, MED PACK, MED PKG]} && BoundDimFilter{P_SIZE <= 10(numeric)} && BoundDimFilter{15 <= L_QUANTITY <= 25(numeric)}) || (P_BRAND=='Brand#24' && InDimFilter{dimension='P_CONTAINER', values=[LG BOX, LG CASE, LG PACK, LG PKG]} && BoundDimFilter{P_SIZE <= 15(numeric)} && BoundDimFilter{26 <= L_QUANTITY <= 36(numeric)})), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON'), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
            "StreamQuery{dataSource='part', filter=BoundDimFilter{1 <= P_SIZE(numeric)}, columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}"
        );
      }
    }
  }

  public static final String TPCH20 =
      "SELECT"
      + "  S_NAME,"
      + "  S_ADDRESS"
      + " FROM"
      + "  supplier, nation"
      + " WHERE"
      + "  S_SUPPKEY IN ("
      + "    SELECT"
      + "      PS_SUPPKEY"
      + "    FROM"
      + "      partsupp"
      + "    WHERE"
      + "      PS_PARTKEY IN ("
      + "        SELECT"
      + "          P_PARTKEY"
      + "        FROM"
      + "          part"
      + "        WHERE"
      + "          P_NAME LIKE 'forest%'"
      + "        )"
      + "      AND PS_AVAILQTY > ("
      + "        SELECT"
      + "          0.5 * SUM(L_QUANTITY)"
      + "        FROM"
      + "          lineitem"
      + "        WHERE"
      + "          L_PARTKEY = PS_PARTKEY"
      + "          AND L_SUPPKEY = PS_SUPPKEY"
      + "          AND L_SHIPDATE >= '1994-01-01'"
      + "          AND L_SHIPDATE < '1995-01-01'"
      + "        )"
      + "    )"
      + "  AND S_NATIONKEY = N_NATIONKEY"
      + "  AND N_NAME = 'RUSSIA'"
      + " ORDER BY"
      + "  S_NAME";

  public static final String TPCH20_EXPLAIN =
      "DruidOuterQueryRel(sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 1, 3])\n"
      + "      DruidQueryRel(table=[druid.supplier], scanFilter=[IS NOT NULL($6)], scanProject=[$1, $3, $4, $6])\n"
      + "      DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'RUSSIA')], scanProject=[$2])\n"
      + "    DruidOuterQueryRel(scanFilter=[>($1, *(0.5:DECIMAL(2, 1), $2))], scanProject=[$0], group=[{0}])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1, 2], rightKeys=[0, 1], outputColumns=[2, 0, 5])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 1, 2])\n"
      + "          DruidQueryRel(table=[druid.partsupp], scanFilter=[AND(IS NOT NULL($2), IS NOT NULL($3))], scanProject=[$0, $2, $3])\n"
      + "          DruidQueryRel(table=[druid.part], scanFilter=[AND(LIKE($4, 'forest%'), IS NOT NULL($5))], scanProject=[$5], group=[{0}])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1994-01-01'), <($11, '1995-01-01'), IS NOT NULL($7), IS NOT NULL($14))], scanProject=[$7, $14, $8], group=[{0, 1}], agg#0=[SUM($2)])\n";

  public static final Object[][] TPCH20_RESULT = {{"Supplier#000000025", "RCQKONXMFnrodzz6w7fObFVV6CUm2q"}};

  @Test
  public void tpch20() throws Exception
  {
    testQuery(TPCH20, TPCH20_EXPLAIN, TPCH20_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "UW5nif9oUluMMHUY40LXFw==",
          "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY]}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0]}",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), sampleRatio=0.050}",
          "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[partsupp]}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01} && InDimsFilter{dimensions=[d0, d1], values=[[304, 23], [304, 41], [304, 5], [304, 9], [447, 18], [447, 38], [447, 48], [447, 8], [488, 10], [488, 2], [..20 more]]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > (0.5B * a0))'}, outputColumns=[d0]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01} && InDimsFilter{dimensions=[L_PARTKEY, L_SUPPKEY], values=[[304, 23], [304, 41], [304, 5], [304, 9], [447, 18], [447, 38], [447, 48], [447, 8], [488, 10], [488, 2], [..20 more]]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}",
          "StreamQuery{dataSource='StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && S_NATIONKEY=='22' && InDimFilter{dimension='S_SUPPKEY', values=[1, 10, 18, 22, 23, 25, 30, 31, 37, 38, ..6 more]}), columns=[S_NAME, S_ADDRESS]}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
          "StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && S_NATIONKEY=='22' && InDimFilter{dimension='S_SUPPKEY', values=[1, 10, 18, 22, 23, 25, 30, 31, 37, 38, ..6 more]}), columns=[S_NAME, S_ADDRESS]}"
      );
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "s1/yxWUgQIpLu1Hu/ATqaw==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY]}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), sampleRatio=0.050}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}), columns=[S_ADDRESS, S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL)), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > (0.5B * a0))'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL)), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > (0.5B * a0))'}, outputColumns=[d0]}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL)), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}",
            "StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}), columns=[S_ADDRESS, S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "QMo4VWG6igLuXGmxGhZAPw==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), sampleRatio=0.050}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL)), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > (0.5B * a0))'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL)), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > (0.5B * a0))'}, outputColumns=[d0]}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && !(PS_SUPPKEY==NULL)), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_NAME LIKE 'forest%' && !(P_PARTKEY==NULL)), outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=(!(L_PARTKEY==NULL) && !(L_SUPPKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE < 1995-01-01}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], outputColumns=[d0, d1, a0]}"
        );
      }
    }
  }

  public static final String TPCH20_2 =
      "WITH TMP1 AS ("
      + "  SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"
      + "),"
      + "TMP2 AS ("
      + "  SELECT S_NAME, S_ADDRESS, S_SUPPKEY"
      + "    FROM supplier, nation"
      + "    WHERE S_NATIONKEY = N_NATIONKEY"
      + "    AND N_NAME = 'RUSSIA'"    // changed 'CANADA' to 'RUSSIA'
      + "),"
      + "TMP3 AS ("
      + "  SELECT L_PARTKEY, 0.5 * SUM(L_QUANTITY) AS SUM_QUANTITY, L_SUPPKEY"
      + "    FROM lineitem, TMP2"
      + "    WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE <= '1995-01-01'"
      + "    AND L_SUPPKEY = S_SUPPKEY"
      + "    GROUP BY L_PARTKEY, L_SUPPKEY"
      + "),"
      + "TMP4 AS ("
      + "  SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY"
      + "    FROM partsupp"
      + "    WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM TMP1)"
      + "),"
      + "TMP5 AS ("
      + "  SELECT PS_SUPPKEY"
      + "    FROM TMP4, TMP3"
      + "    WHERE "
      + "      PS_PARTKEY = L_PARTKEY AND"
      + "      PS_SUPPKEY = L_SUPPKEY AND"
      + "      PS_AVAILQTY > SUM_QUANTITY"
      + ")"
      + "SELECT"
      + "    S_NAME, S_ADDRESS"
      + " FROM"
      + "    supplier"
      + " WHERE"
      + "    S_SUPPKEY IN (SELECT PS_SUPPKEY FROM TMP5)"
      + " ORDER BY"
      + "    S_NAME";

  public static final String TPCH20_2_EXPLAIN =
      "DruidOuterQueryRel(sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
      + "    DruidQueryRel(table=[druid.supplier], scanFilter=[IS NOT NULL($6)], scanProject=[$1, $3, $6])\n"
      + "    DruidOuterQueryRel(scanFilter=[>($1, $2)], scanProject=[$0], group=[{0}])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0, 1], rightKeys=[0, 2], outputColumns=[1, 2, 4])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[1, 2, 0])\n"
      + "          DruidQueryRel(table=[druid.partsupp], scanFilter=[IS NOT NULL($3)], scanProject=[$0, $2, $3])\n"
      + "          DruidQueryRel(table=[druid.part], scanFilter=[LIKE($4, 'forest%')], scanProject=[$5], group=[{0}])\n"
      + "        DruidOuterQueryRel(group=[{0, 1}], agg#0=[SUM($2)], aggregateProject=[$0, *(0.5:DECIMAL(2, 1), $2), $1])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 2, 1])\n"
      + "            DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1994-01-01'), <=($11, '1995-01-01'), IS NOT NULL($7))], scanProject=[$7, $8, $14])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
      + "              DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "              DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'RUSSIA')], scanProject=[$2])\n";

  @Test
  public void tpch20_2() throws Exception
  {
    testQuery(TPCH20_2, TPCH20_2_EXPLAIN, TPCH20_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "ppZkbzpMDWTfHnWJMT2Zeg==",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY]}",
          "StreamQuery{dataSource='supplier', filter=S_NATIONKEY=='22', columns=[S_SUPPKEY]}",
          "StreamQuery{dataSource='partsupp', filter=(!(PS_SUPPKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[partsupp]}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=InDimsFilter{dimensions=[d0, d1], values=[[304, 23], [304, 41], [304, 5], [304, 9], [447, 18], [447, 38], [447, 48], [447, 8], [488, 10], [488, 2], [..20 more]]}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], filter=InDimsFilter{dimensions=[L_PARTKEY, L_SUPPKEY], values=[[304, 23], [304, 41], [304, 5], [304, 9], [447, 18], [447, 38], [447, 48], [447, 8], [488, 10], [488, 2], [..20 more]]}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
          "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}",
          "StreamQuery{dataSource='StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && S_SUPPKEY=='25'), columns=[S_NAME, S_ADDRESS]}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
          "StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && S_SUPPKEY=='25'), columns=[S_NAME, S_ADDRESS]}"
      );
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "c8M36B1gz3IO5hcLqDG24A==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY]}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}",
            "StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY]}"
        );
      } else {
        hook.verifyHooked(
            "bfzRkllvzW8F7C3B1yBewg==",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY]}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=(!(L_PARTKEY==NULL) && BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY]}"
        );
      }
    }
  }


  public static final String TPCH21 =
      "WITH LOCATION AS ("
      + " SELECT supplier.* FROM supplier, nation WHERE"
      + " S_NATIONKEY = N_NATIONKEY AND N_NAME = 'UNITED STATES'"
      // changed 'SAUDI ARABIA' to 'UNITED STATES'
      + "),"
      + "L3 AS ("
      + "SELECT L_ORDERKEY, COUNT(DISTINCT L_SUPPKEY) AS CNTSUPP"
      + " FROM lineitem"
      + " WHERE L_RECEIPTDATE > L_COMMITDATE AND L_ORDERKEY IS NOT NULL"
      + " GROUP BY L_ORDERKEY"
      + " HAVING CNTSUPP = 1"
      + ")"
      + "SELECT S_NAME, COUNT(*) AS NUMWAIT FROM"
      + "("
      + " SELECT LI.L_SUPPKEY, LI.L_ORDERKEY"
      + " FROM lineitem LI JOIN orders O ON LI.L_ORDERKEY = O.O_ORDERKEY AND O.O_ORDERSTATUS = 'F'"
      + "     JOIN"
      + "     ("
      + "       SELECT L_ORDERKEY, COUNT(DISTINCT L_SUPPKEY) AS CNTSUPP"
      + "       FROM lineitem"
      + "       GROUP BY L_ORDERKEY"
      + "     ) L2 ON LI.L_ORDERKEY = L2.L_ORDERKEY AND"
      + "             LI.L_RECEIPTDATE > LI.L_COMMITDATE AND"
      + "             L2.CNTSUPP > 1"
      + ") L1 JOIN L3 ON L1.L_ORDERKEY = L3.L_ORDERKEY"
      + " JOIN LOCATION S ON L1.L_SUPPKEY = S.S_SUPPKEY"
      + " GROUP BY S_NAME"
      + " ORDER BY NUMWAIT DESC, S_NAME"
      + " LIMIT 100";

  public static final String TPCH21_EXPLAIN =
      "DruidOuterQueryRel(group=[{0}], NUMWAIT=[COUNT()], sort=[$1:DESC, $0:ASC], fetch=[100])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1])\n"
      + "          DruidQueryRel(table=[druid.lineitem], scanFilter=[>($9, $1)], scanProject=[$6, $14])\n"
      + "          DruidQueryRel(table=[druid.orders], scanFilter=[=($6, 'F')], scanProject=[$4])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $14], group=[{0}], CNTSUPP=[COUNT(DISTINCT $1)], aggregateFilter=[>($1, 1)], aggregateProject=[$0])\n"
      + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>($9, $1), IS NOT NULL($6))], scanProject=[$6, $14], group=[{0}], CNTSUPP=[COUNT(DISTINCT $1)], aggregateFilter=[=($1, 1)], aggregateProject=[$0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 2])\n"
      + "      DruidQueryRel(table=[druid.supplier], scanProject=[$3, $4, $6])\n"
      + "      DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'UNITED STATES')], scanProject=[$2])\n";

  public static final String TPCH21_EXPLAIN_JR =
      "DruidOuterQueryRel(group=[{0}], NUMWAIT=[COUNT()], sort=[$1:DESC, $0:ASC], fetch=[100])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[2])\n"
      + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>($9, $1), IS NOT NULL($6))], scanProject=[$6, $14], group=[{0}], CNTSUPP=[COUNT(DISTINCT $1)], aggregateFilter=[=($1, 1)], aggregateProject=[$0])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1, 2])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1, 0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1])\n"
      + "          DruidQueryRel(table=[druid.lineitem], scanFilter=[>($9, $1)], scanProject=[$6, $14])\n"
      + "          DruidQueryRel(table=[druid.orders], scanFilter=[=($6, 'F')], scanProject=[$4])\n"
      + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $14], group=[{0}], CNTSUPP=[COUNT(DISTINCT $1)], aggregateFilter=[>($1, 1)], aggregateProject=[$0])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[0, 2])\n"
      + "        DruidQueryRel(table=[druid.supplier], scanProject=[$3, $4, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'UNITED STATES')], scanProject=[$2])\n";

  public static final Object[][] TPCH21_RESULT = {
      {"Supplier#000000010", 15L},
      {"Supplier#000000019", 15L},
      {"Supplier#000000046", 15L},
      {"Supplier#000000049", 5L}
  };

  @Test
  public void tpch21() throws Exception
  {
    testQuery(TPCH21, TPCH21_EXPLAIN, TPCH21_RESULT);

    if (semiJoin) {
      hook.verifyHooked(
          "nE+p1pNu5jJnl7k1fhbyvA==",
          "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY]}",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0]}",
          "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
          "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && InDimFilter{dimension='L_ORDERKEY', values=[10018, 10021, 10022, 10048, 10274, 10305, 10434, 10437, 10438, 10528, ..534 more]}), columns=[L_SUPPKEY]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='supplier', filter=(S_NATIONKEY=='24' && InDimFilter{dimension='S_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[S_NAME, S_SUPPKEY], outputColumns=[S_NAME], localPostProcessing=RowExplodeProcessor{columns=[S_SUPPKEY]}}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='supplier', filter=(S_NATIONKEY=='24' && InDimFilter{dimension='S_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[S_NAME, S_SUPPKEY], outputColumns=[S_NAME], localPostProcessing=RowExplodeProcessor{columns=[S_SUPPKEY]}}"
      );
    } else if (broadcastJoin) {
      if (bloomFilter) {
        hook.verifyHooked(
            "wycFmaRa/AdTmJWRkx5LRQ==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=18965}), columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=18965}]}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "sC/24PlWB+4zNI5gyZHmyA==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}"
        );
      }
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "BNYFpmeKpcfWUSCu1jq5Gg==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=18965}), columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=18965}]}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "+0q4JoyV823hyUajSSSeyQ==",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], sampleRatio=0.050}",
            "DimensionSamplingQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), sampleRatio=0.050}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fieldNames=[L_SUPPKEY], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}"
        );
      }
    }
  }

  public static final String TPCH22 =
      "SELECT"
      + "  CNTRYCODE,"
      + "  COUNT(*) AS NUMCUST,"
      + "  SUM(C_ACCTBAL) AS TOTACCTBAL"
      + " FROM ("
      + "  SELECT"
      + "    SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE,"
      + "    C_ACCTBAL"
      + "  FROM"
      + "    customer"
      + "  WHERE"
      + "    SUBSTR(C_PHONE, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')"
      + "    AND C_ACCTBAL > ("
      + "      SELECT"
      + "        AVG(C_ACCTBAL)"
      + "      FROM"
      + "        customer"
      + "      WHERE"
      + "        C_ACCTBAL > 0.00"
      + "        AND SUBSTR(C_PHONE, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')"
      + "      )"
      + "    AND NOT EXISTS ("
      + "      SELECT"
      + "        *"
      + "      FROM"
      + "        orders"
      + "      WHERE"
      + "        O_CUSTKEY = C_CUSTKEY"
      + "    )"
      + "  ) AS CUSTSALE"
      + " GROUP BY"
      + "  CNTRYCODE"
      + " ORDER BY"
      + "  CNTRYCODE";

  public static final String TPCH22_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[IS NULL($2)], scanProject=[SUBSTR($0, 1, 2), $1], group=[{0}], NUMCUST=[COUNT()], TOTACCTBAL=[SUM($1)], sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[LEFT], leftKeys=[1], rightKeys=[0], outputColumns=[2, 0, 5])\n"
      + "    DruidOuterQueryRel(scanFilter=[>($0, $3)])\n"
      + "      DruidJoinRel(joinType=[INNER])\n"
      + "        DruidQueryRel(table=[druid.customer], scanFilter=[OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17'))], scanProject=[$0, $3, $7])\n"
      + "        DruidQueryRel(table=[druid.customer], scanFilter=[AND(>($0, 0.00:DECIMAL(3, 2)), OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17')))], scanProject=[$0], EXPR$0=[AVG($0)])\n"
      + "    DruidQueryRel(table=[druid.orders], scanFilter=[IS NOT NULL($2)], scanProject=[$2], group=[{0}], aggregateProject=[$0, true])\n";

  public static final String TPCH22_EXPLAIN_JR =
      "DruidOuterQueryRel(scanFilter=[IS NULL($2)], scanProject=[SUBSTR($0, 1, 2), $1], group=[{0}], NUMCUST=[COUNT()], TOTACCTBAL=[SUM($1)], sort=[$0:ASC])\n"
      + "  DruidJoinRel(joinType=[RIGHT], leftKeys=[0], rightKeys=[1], outputColumns=[4, 2, 1])\n"
      + "    DruidQueryRel(table=[druid.orders], scanFilter=[IS NOT NULL($2)], scanProject=[$2], group=[{0}], aggregateProject=[$0, true])\n"
      + "    DruidOuterQueryRel(scanFilter=[>($0, $3)])\n"
      + "      DruidJoinRel(joinType=[INNER])\n"
      + "        DruidQueryRel(table=[druid.customer], scanFilter=[OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17'))], scanProject=[$0, $3, $7])\n"
      + "        DruidQueryRel(table=[druid.customer], scanFilter=[AND(>($0, 0.00:DECIMAL(3, 2)), OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17')))], scanProject=[$0], EXPR$0=[AVG($0)])\n";

  public static final Object[][] TPCH22_RESULT = {
      {"13", 5L, 37676.7D},
      {"17", 5L, 41431.74D},
      {"18", 7L, 51351.03D},
      {"23", 2L, 18148.870000000003D},
      {"29", 6L, 47247.25D},
      {"30", 9L, 65584.23D},
      {"31", 7L, 53270.52999999999D}
  };

  @Test
  public void tpch22() throws Exception
  {
    testQuery(TPCH22, TPCH22_EXPLAIN, TPCH22_RESULT);

    if (bloomFilter) {
      hook.verifyHooked(
          "VIOCDkklrv78lVI17A10Pw==",
          "DimensionSamplingQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=!(O_CUSTKEY==NULL), sampleRatio=0.050}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE]}, TimeseriesQuery{dataSource='customer', filter=(BoundDimFilter{0.00 < C_ACCTBAL(numeric)} && InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='C_ACCTBAL'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(C_ACCTBAL > a0)'}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE, a0]}, GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=!(O_CUSTKEY==NULL), postAggregatorSpecs=[MathPostAggregator{name='p0', expression='true', finalize=true}], outputColumns=[d0, p0], $hash=true}], timeColumnName=__time}', dimensions=[ExtractionDimensionSpec{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, outputName='d0'}], filter=p0==NULL, aggregatorSpecs=[CountAggregatorFactory{name='a0'}, GenericSumAggregatorFactory{name='a1', fieldName='C_ACCTBAL', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
          "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE]}, TimeseriesQuery{dataSource='customer', filter=(BoundDimFilter{0.00 < C_ACCTBAL(numeric)} && InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='C_ACCTBAL'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(C_ACCTBAL > a0)'}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE, a0]}",
          "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE]}",
          "TimeseriesQuery{dataSource='customer', filter=(BoundDimFilter{0.00 < C_ACCTBAL(numeric)} && InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='C_ACCTBAL'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}",
          "GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=!(O_CUSTKEY==NULL), postAggregatorSpecs=[MathPostAggregator{name='p0', expression='true', finalize=true}], outputColumns=[d0, p0], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "VIOCDkklrv78lVI17A10Pw==",
          "DimensionSamplingQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=!(O_CUSTKEY==NULL), sampleRatio=0.050}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE]}, TimeseriesQuery{dataSource='customer', filter=(BoundDimFilter{0.00 < C_ACCTBAL(numeric)} && InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='C_ACCTBAL'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(C_ACCTBAL > a0)'}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE, a0]}, GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=!(O_CUSTKEY==NULL), postAggregatorSpecs=[MathPostAggregator{name='p0', expression='true', finalize=true}], outputColumns=[d0, p0], $hash=true}], timeColumnName=__time}', dimensions=[ExtractionDimensionSpec{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, outputName='d0'}], filter=p0==NULL, aggregatorSpecs=[CountAggregatorFactory{name='a0'}, GenericSumAggregatorFactory{name='a1', fieldName='C_ACCTBAL', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
          "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE]}, TimeseriesQuery{dataSource='customer', filter=(BoundDimFilter{0.00 < C_ACCTBAL(numeric)} && InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='C_ACCTBAL'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(C_ACCTBAL > a0)'}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE, a0]}",
          "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, C_PHONE]}",
          "TimeseriesQuery{dataSource='customer', filter=(BoundDimFilter{0.00 < C_ACCTBAL(numeric)} && InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count', fieldName='C_ACCTBAL'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{fieldName='a0:sum'}, FieldAccessPostAggregator{fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}",
          "GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=!(O_CUSTKEY==NULL), postAggregatorSpecs=[MathPostAggregator{name='p0', expression='true', finalize=true}], outputColumns=[d0, p0], $hash=true}"
      );
    }
  }
}
