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
import java.math.BigDecimal;
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
  public void tpch1() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT\n"
        + "    L_RETURNFLAG,\n"
        + "    L_LINESTATUS,\n"
        + " SUM(L_QUANTITY) AS SUM_QTY,\n"
        + " SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,\n"
        + " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,\n"
        + " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,\n"
        + " AVG(L_QUANTITY) AS AVG_QTY,\n"
        + " AVG(L_EXTENDEDPRICE) AS AVG_PRICE,\n"
        + " AVG(L_DISCOUNT) AS AVG_DISC,\n"
        + " COUNT(*) AS COUNT_ORDER\n"
        + " FROM\n"
        + "    lineitem\n"
        + " WHERE\n"
        + "    L_SHIPDATE <= '1998-09-16'\n"
        + " GROUP BY\n"
        + "    L_RETURNFLAG,\n"
        + "    L_LINESTATUS\n"
        + " ORDER BY\n"
        + "    L_RETURNFLAG,\n"
        + "    L_LINESTATUS",
        "DruidQueryRel(table=[druid.lineitem], "
        + "scanFilter=[<=($11, '1998-09-16')], "
        + "scanProject=[$10, $5, $8, $3, *($3, -(1, $2)), *(*($3, -(1, $2)), +(1, $15)), $2], "
        + "group=[{0, 1}], SUM_QTY=[SUM($2)], SUM_BASE_PRICE=[SUM($3)], SUM_DISC_PRICE=[SUM($4)], SUM_CHARGE=[SUM($5)], AVG_QTY=[AVG($2)], AVG_PRICE=[AVG($3)], AVG_DISC=[AVG($6)], COUNT_ORDER=[COUNT()], "
        + "sort=[$0:ASC, $1:ASC])\n",
        newGroupBy()
            .dataSource("lineitem")
            .dimensions(
                DefaultDimensionSpec.of("L_RETURNFLAG", "d0"),
                DefaultDimensionSpec.of("L_LINESTATUS", "d1")
            )
            .filters(BoundDimFilter.lte("L_SHIPDATE", "1998-09-16").withComparatorType("lexicographic"))
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
                GenericSumAggregatorFactory.ofLong("a4:sum", "L_QUANTITY"),
                CountAggregatorFactory.of("a4:count"),
                GenericSumAggregatorFactory.ofDouble("a5:sum", "L_EXTENDEDPRICE"),
                CountAggregatorFactory.of("a5:count"),
                GenericSumAggregatorFactory.ofDouble("a6:sum", "L_DISCOUNT"),
                CountAggregatorFactory.of("a6:count"),
                CountAggregatorFactory.of("a7")
            )
            .postAggregators(
                new ArithmeticPostAggregator("a4", "quotient", Arrays.asList(
                    new FieldAccessPostAggregator(null, "a4:sum"),
                    new FieldAccessPostAggregator(null, "a4:count")
                )),
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
        new Object[]{"A", "F", 189203L, 2.649171512299999E8, 2.5172256669044656E8, 2.618137696907937E8, 25L, 35407.26426490242, 0.05014435F, 7482L},
        new Object[]{"N", "F", 4654L, 6647990.519999999, 6333568.496621376, 6584905.261532691, 26L, 37139.61184357541, 0.04849162F, 179L},
        new Object[]{"N", "O", 376815L, 5.2792684456999964E8, 5.016250502495867E8, 5.2164760657695186E8, 25L, 35840.247424983005, 0.04984861F, 14730L},
        new Object[]{"R", "F", 191214L, 2.6792430413999987E8, 2.5454761805335242E8, 2.6480436570269588E8, 25L, 35972.65093179375, 0.04983217F, 7448L}
    );
  }

  @Test
  public void tpch2() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH q2_min_ps_supplycost AS (\n"
        + "SELECT\n"
        + "    P_PARTKEY AS MIN_P_PARTKEY,\n"
        + " MIN(PS_SUPPLYCOST) AS MIN_PS_SUPPLYCOST\n"
        + " FROM\n"
        + "    part,\n"
        + "    partsupp,\n"
        + "    supplier,\n"
        + "    nation,\n"
        + "    region\n"
        + " WHERE\n"
        + "    P_PARTKEY = PS_PARTKEY\n"
        + " AND S_SUPPKEY = PS_SUPPKEY\n"
        + " AND S_NATIONKEY = N_NATIONKEY\n"
        + " AND N_REGIONKEY = R_REGIONKEY\n"
        + " AND R_NAME = 'EUROPE'\n"
        + " GROUP BY\n"
        + "    P_PARTKEY\n"
        + ")\n"
        + "SELECT\n"
        + "    S_ACCTBAL,\n"
        + "    S_NAME,\n"
        + "    N_NAME,\n"
        + "    P_PARTKEY,\n"
        + "    P_MFGR,\n"
        + "    S_ADDRESS,\n"
        + "    S_PHONE,\n"
        + "    S_COMMENT\n"
        + " FROM\n"
        + "    part,\n"
        + "    supplier,\n"
        + "    partsupp,\n"
        + "    nation,\n"
        + "    region,\n"
        + "    q2_min_ps_supplycost\n"
        + " WHERE\n"
        + "    P_PARTKEY = PS_PARTKEY\n"
        + " AND S_SUPPKEY = PS_SUPPKEY\n"
        + " AND P_SIZE = 37\n"
        + " AND P_TYPE LIKE '%COPPER'\n"
        + " AND S_NATIONKEY = N_NATIONKEY\n"
        + " AND N_REGIONKEY = R_REGIONKEY\n"
        + " AND R_NAME = 'EUROPE'\n"
        + " AND PS_SUPPLYCOST = MIN_PS_SUPPLYCOST\n"
        + " AND P_PARTKEY = MIN_P_PARTKEY\n"
        + " ORDER BY\n"
        + "    S_ACCTBAL DESC,\n"
        + "    N_NAME,\n"
        + "    S_NAME,\n"
        + "    P_PARTKEY\n"
        + " LIMIT 100",
        "DruidOuterQueryRel(sort=[$0:DESC, $2:ASC, $1:ASC, $3:ASC], fetch=[100])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[13, 1], rightKeys=[1, 0], outputColumns=[4, 7, 14, 1, 0, 5, 9, 6])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[16], rightKeys=[1])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[8], rightKeys=[1])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[6], outputColumns=[0, 1, 2, 3, 7, 8, 9, 10, 11, 12, 13, 4, 5, 6])\n"
        + "          DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0])\n"
        + "            DruidQueryRel(table=[druid.part], scanFilter=[AND(=($7, 37), LIKE($8, '%COPPER'))], scanProject=[$3, $5, $7, $8])\n"
        + "            DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3, $4])\n"
        + "          DruidQueryRel(table=[druid.supplier], scanProject=[$0, $1, $2, $3, $4, $5, $6])\n"
        + "        DruidQueryRel(table=[druid.nation], scanProject=[$1, $2, $3])\n"
        + "      DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'EUROPE')], scanProject=[$1, $2])\n"
        + "    DruidOuterQueryRel(group=[{4}], MIN_PS_SUPPLYCOST=[MIN($7)])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[1], outputColumns=[8, 9, 6, 7, 0, 1, 2, 3, 4, 5])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0])\n"
        + "          DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1])\n"
        + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
        + "              DruidQueryRel(table=[druid.part], scanProject=[$5])\n"
        + "              DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3, $4])\n"
        + "            DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
        + "          DruidQueryRel(table=[druid.nation], scanProject=[$2, $3])\n"
        + "        DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'EUROPE')], scanProject=[$1, $2])\n",
        new Object[]{6820.35, "Supplier#000000007", "UNITED KINGDOM", "560", "Manufacturer#2", "s,4TicNGB4uO6PaSqNBUq", "33-990-965-2201", "s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit"},
        new Object[]{3556.47, "Supplier#000000032", "UNITED KINGDOM", "381", "Manufacturer#5", "yvoD3TtZSx1skQNCK8agk5bZlZLug", "33-484-637-7873", "usly even depths. quickly ironic theodolites s"},
        new Object[]{2972.26, "Supplier#000000016", "RUSSIA", "396", "Manufacturer#3", "YjP5C55zHDXL7LalK27zfQnwejdpin4AMpvh", "32-822-502-4215", "ously express ideas haggle quickly dugouts? fu"}
    );

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "LywLiesX07BH/wMNg7m2ZQ==",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE]}",
            "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=partsupp, rightJoinColumns=[PS_PARTKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, P_SIZE:long, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=partsupp, rightJoinColumns=[PS_PARTKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, P_SIZE:long, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, S_SUPPKEY:dimension.string}}]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}"
        );
      } else if (bloomFilter) {
        hook.verifyHooked(
            "r2sYwAytHZhzj19EAB+EJQ==",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}, StreamQuery{dataSource='partsupp', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((P_SIZE=='37' && P_TYPE LIKE '%COPPER')), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=4}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "TimeseriesQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "+cIOOIiql7t3ajcRSAZP+g==",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}"
        );
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "LywLiesX07BH/wMNg7m2ZQ==",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE]}",
            "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=partsupp, rightJoinColumns=[PS_PARTKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, P_SIZE:long, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=partsupp, rightJoinColumns=[PS_PARTKEY]}, hashLeft=true, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string, P_SIZE:long, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, S_SUPPKEY:dimension.string}}]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "r2sYwAytHZhzj19EAB+EJQ==",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}, StreamQuery{dataSource='partsupp', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((P_SIZE=='37' && P_TYPE LIKE '%COPPER')), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=4}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
              "TimeseriesQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
              "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
              "StreamQuery{dataSource='partsupp', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "+cIOOIiql7t3ajcRSAZP+g==",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
              "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), columns=[P_MFGR, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='part', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_NAME, R_REGIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  @Test
  public void tpch3() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "select\n"
        + "    L_ORDERKEY,\n"
        + "    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,\n"
        + "    O_ORDERDATE,\n"
        + "    O_SHIPPRIORITY\n"
        + " from\n"
        + "    customer,\n"
        + "    orders,\n"
        + "    lineitem\n"
        + " where\n"
        + "    C_MKTSEGMENT = 'BUILDING'\n"
        + "    and C_CUSTKEY = O_CUSTKEY\n"
        + "    and L_ORDERKEY = O_ORDERKEY\n"
        + "    and O_ORDERDATE < '1995-03-22'\n"
        + "    and L_SHIPDATE > '1995-03-22'\n"
        + " group by\n"
        + "    L_ORDERKEY,\n"
        + "    O_ORDERDATE,\n"
        + "    O_SHIPPRIORITY\n"
        + " order by\n"
        + "    revenue desc,\n"
        + "    O_ORDERDATE\n"
        + " limit 10",
        "DruidOuterQueryRel(scanProject=[$0, $1, $2, *($3, -(1, $4))], group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$3:DESC, $1:ASC], fetch=[10], sortProject=[$0, $3, $1, $2])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[2], outputColumns=[8, 3, 5, 7, 6])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
        + "      DruidQueryRel(table=[druid.customer], scanFilter=[=($4, 'BUILDING')], scanProject=[$3, $4])\n"
        + "      DruidQueryRel(table=[druid.orders], scanFilter=[<($3, '1995-03-22')], scanProject=[$2, $3, $4, $7])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[>($11, '1995-03-22')], scanProject=[$2, $3, $6, $11])\n",
        new Object[]{"26304", 358077.0152279817D, "1995-03-20", 0L},
        new Object[]{"928", 289800.9607996043D, "1995-03-02", 0L},
        new Object[]{"4327", 187634.62862386403D, "1995-03-16", 0L},
        new Object[]{"20453", 176905.6235388234D, "1995-03-11", 0L},
        new Object[]{"20486", 171516.90596939923D, "1995-03-06", 0L},
        new Object[]{"18820", 163812.8043091065D, "1995-02-12", 0L},
        new Object[]{"16096", 147838.6416906625D, "1995-01-20", 0L},
        new Object[]{"3749", 135109.43370970472D, "1995-02-24", 0L},
        new Object[]{"19365", 126378.68876224649D, "1995-01-17", 0L},
        new Object[]{"6560", 123264.19097787395D, "1995-01-05", 0L}
    );
    if (bloomFilter) {
      hook.verifyHooked(
          "jaFAJeIcjzyLCVYw9f7oaw==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=(C_MKTSEGMENT=='BUILDING' && BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=917}), columns=[C_CUSTKEY, C_MKTSEGMENT], $hash=true}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)} && BloomDimFilter.Factory{bloomSource=$view:customer[C_CUSTKEY](C_MKTSEGMENT=='BUILDING'), fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='O_CUSTKEY'}], groupingSets=Noop, maxNumEntries=155}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
          "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=917}]}",
          "TimeseriesQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=155}]}",
          "StreamQuery{dataSource='customer', filter=(C_MKTSEGMENT=='BUILDING' && BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}), columns=[C_CUSTKEY, C_MKTSEGMENT], $hash=true}",
          "StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)} && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='O_CUSTKEY'}], groupingSets=Noop}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE]}"
      );
    } else {
      hook.verifyHooked(
          "5po7jkrMaBTG4OqHlfGEVA==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY, C_MKTSEGMENT], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
          "StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY, C_MKTSEGMENT], $hash=true}",
          "StreamQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE]}"
      );
    }
  }

  @Test
  public void tpch4() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "select"
        + " O_ORDERPRIORITY, count(*) as order_count from orders as o"
        + " where O_ORDERDATE >= '1996-05-01' and O_ORDERDATE < '1996-08-01'"
        + "   and exists ( select 1 from lineitem where L_ORDERKEY = o.O_ORDERKEY and L_COMMITDATE < L_RECEIPTDATE)"
        + " group by O_ORDERPRIORITY order by O_ORDERPRIORITY",
        "DruidOuterQueryRel(group=[{0}], order_count=[COUNT()], sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2])\n"
        + "    DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1996-05-01'), <($3, '1996-08-01'))], scanProject=[$3, $4, $5])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(<($1, $9), IS NOT NULL($6))], scanProject=[$6], group=[{0}])\n",
        new Object[]{"1-URGENT", 53L},
        new Object[]{"2-HIGH", 40L},
        new Object[]{"3-MEDIUM", 50L},
        new Object[]{"4-NOT SPECIFIED", 59L},
        new Object[]{"5-LOW", 53L}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "at6h+xy1rZz8mA+fFIAohQ==",
          "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='orders', filter=(BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)} && InDimFilter{dimension='O_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..6896 more]}), columns=[O_ORDERPRIORITY]}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)} && InDimFilter{dimension='O_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..6896 more]}), columns=[O_ORDERPRIORITY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "qVyUcjMFHu4ZIaHZGeV3EQ==",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}, columns=[O_ORDERDATE, O_ORDERKEY, O_ORDERPRIORITY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL) && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, maxNumEntries=287}), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=287}]}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}, columns=[O_ORDERDATE, O_ORDERKEY, O_ORDERPRIORITY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL) && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop}), outputColumns=[d0]}"
        );
      } else {
        hook.verifyHooked(
            "zuHZIayjmtt2GIqrO7F5Zg==",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}, columns=[O_ORDERDATE, O_ORDERKEY, O_ORDERPRIORITY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}, columns=[O_ORDERDATE, O_ORDERKEY, O_ORDERPRIORITY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}"
        );
      }
    }
  }

  @Test
  public void tpch5() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT\n"
        + "    N_NAME,\n"
        + " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE\n"
        + " FROM\n"
        + "    customer,\n"
        + "    orders,\n"
        + "    lineitem,\n"
        + "    supplier,\n"
        + "    nation,\n"
        + "    region\n"
        + " WHERE\n"
        + "    C_CUSTKEY = O_CUSTKEY\n"
        + " AND L_ORDERKEY = O_ORDERKEY\n"
        + " AND L_SUPPKEY = S_SUPPKEY\n"
        + " AND C_NATIONKEY = S_NATIONKEY\n"
        + " AND S_NATIONKEY = N_NATIONKEY\n"
        + " AND N_REGIONKEY = R_REGIONKEY\n"
        + " AND R_NAME = 'AFRICA'\n"
        + " AND O_ORDERDATE >= '1993-01-01'\n"
        + " AND O_ORDERDATE < '1994-01-01'\n"
        + " GROUP BY\n"
        + "    N_NAME\n"
        + " ORDER BY\n"
        + "    REVENUE DESC",
        "DruidOuterQueryRel(scanProject=[$0, *($1, -(1, $2))], group=[{0}], REVENUE=[SUM($1)], sort=[$1:DESC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[13], rightKeys=[1], outputColumns=[11, 6, 5])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[9], rightKeys=[1])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[1, 8], rightKeys=[0, 1])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[2])\n"
        + "          DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
        + "            DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
        + "            DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-01-01'), <($3, '1994-01-01'))], scanProject=[$2, $3, $4])\n"
        + "          DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $14])\n"
        + "        DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
        + "      DruidQueryRel(table=[druid.nation], scanProject=[$1, $2, $3])\n"
        + "    DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AFRICA')], scanProject=[$1, $2])\n",
        new Object[]{"KENYA", 523154.4750718259D},
        new Object[]{"MOROCCO", 218260.09096727896D},
        new Object[]{"ETHIOPIA", 167163.61263319192D},
        new Object[]{"ALGERIA", 157068.92618799844D},
        new Object[]{"MOZAMBIQUE", 151814.8570359957D}
    );
    if (semiJoin) {
      if (bloomFilter) {
        hook.verifyHooked(
            "AP3vs4xnX+JQeMbxmEUDaw==",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_NAME, R_REGIONKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=1129}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[0]}, columns=[N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=1129}]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[0]}, columns=[N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}",
            "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "baJ6Nby52w+k0S0MLRSKJQ==",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_NAME, R_REGIONKEY]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[0]}, columns=[N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[0]}, columns=[N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}",
            "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}"
        );
      }
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "elKwJ7Z/OLSS6xxI6CR57g==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=1129}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=1129}]}",
            "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_NAME, R_REGIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "HjZbPSYyTUIy4px81rnkdQ==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_NAME, R_REGIONKEY], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch6() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT\n"
        + " SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE\n"
        + " FROM\n"
        + "    lineitem\n"
        + " WHERE\n"
        + "    L_SHIPDATE >= '1993-01-01'\n"
        + " AND L_SHIPDATE < '1994-01-01'\n"
        + " AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01\n"
        + " AND L_QUANTITY < 25",
        "DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1993-01-01'), <($11, '1994-01-01'), >=($2, 0.05:DECIMAL(4, 2)), <=($2, 0.07:DECIMAL(4, 2)), <($8, 25))], scanProject=[*($3, $2)], REVENUE=[SUM($0)])\n",
        new Object[]{635343.2898368868}
    );
    hook.verifyHooked(
        "aWNxGUjkZNlb/DPaOALY1A==",
        "TimeseriesQuery{dataSource='lineitem', filter=(BoundDimFilter{L_QUANTITY < 25(numeric)} && BoundDimFilter{0.05 <= L_DISCOUNT <= 0.07(numeric)} && BoundDimFilter{1993-01-01 <= L_SHIPDATE < 1994-01-01(lexicographic)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * L_DISCOUNT)', inputType='double'}], outputColumns=[a0]}"
    );
  }

  protected static final String TPCH7 =
      "SELECT\n"
      + "    SUPP_NATION,\n"
      + "    CUST_NATION,\n"
      + "    L_YEAR,\n"
      + "    SUM(VOLUME) AS REVENUE\n"
      + " FROM\n"
      + "    (\n"
      + "        SELECT\n"
      + "            N1.N_NAME AS SUPP_NATION,\n"
      + "            N2.N_NAME AS CUST_NATION,\n"
      + "            YEAR(L_SHIPDATE) AS L_YEAR,\n"
      + "            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME\n"
      + "        FROM\n"
      + "            supplier,\n"
      + "            lineitem,\n"
      + "            orders,\n"
      + "            customer,\n"
      + "            nation N1,\n"
      + "            nation N2\n"
      + "        WHERE\n"
      + "            S_SUPPKEY = L_SUPPKEY\n"
      + "            AND O_ORDERKEY = L_ORDERKEY\n"
      + "            AND C_CUSTKEY = O_CUSTKEY\n"
      + "            AND S_NATIONKEY = N1.N_NATIONKEY\n"
      + "            AND C_NATIONKEY = N2.N_NATIONKEY\n"
      + "            AND (\n"
      + "                (N1.N_NAME = 'KENYA' AND N2.N_NAME = 'PERU')\n"
      + "                OR (N1.N_NAME = 'PERU' AND N2.N_NAME = 'KENYA')\n"
      + "            )\n"
      + "            AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'\n"
      + "    ) AS SHIPPING\n"
      + " GROUP BY\n"
      + "    SUPP_NATION,\n"
      + "    CUST_NATION,\n"
      + "    L_YEAR\n"
      + " ORDER BY\n"
      + "    SUPP_NATION,\n"
      + "    CUST_NATION,\n"
      + "    L_YEAR";

  public static final String TPCH7_EXPLAIN =
      "DruidOuterQueryRel(scanFilter=[OR(AND(=($0, 'KENYA'), =($1, 'PERU')), AND(=($0, 'PERU'), =($1, 'KENYA')))], scanProject=[$0, $1, YEAR($2), *($3, -(1, $4))], group=[{0, 1, 2}], REVENUE=[SUM($3)], sort=[$0:ASC, $1:ASC, $2:ASC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[13, 11, 5, 3, 2])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[10], rightKeys=[1])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[1])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[4])\n"
      + "            DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "            DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1995-01-01'), <=($11, '1996-12-31'))], scanProject=[$2, $3, $6, $11, $14])\n"
      + "          DruidQueryRel(table=[druid.orders], scanProject=[$2, $4])\n"
      + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "      DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
      + "    DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n";

  public static final Object[][] TPCH7_RESULT = {
      {"KENYA", "PERU", 1995L, 155808.41736393946D},
      {"KENYA", "PERU", 1996L, 335577.4810472458D},
      {"PERU", "KENYA", 1995L, 243818.19482950834D},
      {"PERU", "KENYA", 1996L, 105976.76512348771D}
  };

  @Test
  public void tpch7() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        TPCH7,
        TPCH7_EXPLAIN,
        TPCH7_RESULT
    );
    if (broadcastJoin) {
      hook.verifyHooked(
          "RjWm3VtxhTW/x9AWeUN4cA==",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME0', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=((N_NAME0=='KENYA' && N_NAME=='PERU') || (N_NAME0=='PERU' && N_NAME=='KENYA')), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
          "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "LZlumCNc7Z1md9h/E8YfKg==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME0', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=((N_NAME0=='KENYA' && N_NAME=='PERU') || (N_NAME0=='PERU' && N_NAME=='KENYA')), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
          "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY]}",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    }
  }

  public static final String TPCH8 =
      "SELECT\n"
      + "    O_YEAR,\n"
      + " SUM(CASE\n"
      + "        WHEN NATION = 'ROMANIA' THEN VOLUME\n"
      + "        ELSE 0\n"
      + "    END) / SUM(VOLUME) AS MKT_SHARE\n"
      + " FROM\n"
      + "    (\n"
      + " SELECT\n"
      + "            YEAR(O_ORDERDATE) AS O_YEAR,\n"
      + "            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,\n"
      + " N2.N_NAME AS NATION\n"
      + " FROM\n"
      + "            part,\n"
      + "            supplier,\n"
      + "            lineitem,\n"
      + "            orders,\n"
      + "            customer,\n"
      + "            nation N1,\n"
      + "            nation N2,\n"
      + "            region\n"
      + " WHERE\n"
      + "            P_PARTKEY = L_PARTKEY\n"
      + " AND S_SUPPKEY = L_SUPPKEY\n"
      + " AND L_ORDERKEY = O_ORDERKEY\n"
      + " AND O_CUSTKEY = C_CUSTKEY\n"
      + " AND C_NATIONKEY = N1.N_NATIONKEY\n"
      + " AND N1.N_REGIONKEY = R_REGIONKEY\n"
      + " AND R_NAME = 'AMERICA'\n"
      + " AND S_NATIONKEY = N2.N_NATIONKEY\n"
      + " AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'\n"
      + " AND P_TYPE = 'ECONOMY BURNISHED NICKEL'\n"
      + "    ) AS ALL_NATIONS\n"
      + " GROUP BY\n"
      + "    O_YEAR\n"
      + " ORDER BY\n"
      + "    O_YEAR";

  public static final String TPCH8_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[YEAR($0), CASE(=($1, 'ROMANIA'), *($2, -(1, $3)), 0:DOUBLE), *($2, -(1, $3))], group=[{0}], agg#0=[SUM($1)], agg#1=[SUM($2)], sort=[$0:ASC], sortProject=[$0, /($1, $2)])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[15], rightKeys=[1], outputColumns=[10, 16, 5, 4])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[13], rightKeys=[0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[9], rightKeys=[0])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[2])\n"
      + "            DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[1], outputColumns=[5, 6, 7, 8, 0, 1, 2, 3, 4])\n"
      + "              DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0])\n"
      + "                DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $7, $14])\n"
      + "                DruidQueryRel(table=[druid.part], scanFilter=[=($8, 'ECONOMY BURNISHED NICKEL')], scanProject=[$5, $8])\n"
      + "              DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "            DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1995-01-01'), <=($3, '1996-12-31'))], scanProject=[$2, $3, $4])\n"
      + "          DruidQueryRel(table=[druid.customer], scanProject=[$3, $6])\n"
      + "        DruidQueryRel(table=[druid.nation], scanProject=[$2, $3])\n"
      + "      DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
      + "    DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AMERICA')], scanProject=[$1, $2])\n";

  public static final Object[][] TPCH8_RESULT = {
      {1995L, 0.15367145767949628D},
      {1996L, 0.3838133760159879D}
  };

  @Test
  public void tpch8() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        TPCH8,
        TPCH8_EXPLAIN,
        TPCH8_RESULT
    );

    if (semiJoin) {
      if (broadcastJoin) {
        if (bloomFilter) {
          hook.verifyHooked(
              "RXa5UBoug1oVS18YVnbMrw==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY]}",
              "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}",
              "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "9yKr98MKgwV6S6wVDc2QIw==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY]}",
              "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "Fb6CCajXhEcbJVSNlAFOcw==",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY]}",
              "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](P_TYPE=='ECONOMY BURNISHED NICKEL'), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=7}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "TimeseriesQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=7}]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "aOoBoNdtp/TIgC2Upl10mA==",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY]}",
              "GroupByQuery{dataSource='StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', filter=InDimFilter{dimension='N_REGIONKEY', values=[1]}, columns=[O_ORDERDATE, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT]}",
              "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      }
    } else {
      if (broadcastJoin) {
        if (bloomFilter) {
          hook.verifyHooked(
              "GNJMecljAxaILnXGb/rvhg==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "2o+pnqyRXFG9CFXXI2cnEw==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string, P_TYPE:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}"
          );
        }
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "jXQAYawYoStss8LZBOns6w==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](P_TYPE=='ECONOMY BURNISHED NICKEL'), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=7}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "TimeseriesQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=7}]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "xT8IQ0q7X3BucfU/I44BMA==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='s0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, s0]}",
              "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY, P_TYPE], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_NAME, R_REGIONKEY], $hash=true}"
          );
        }
      }
    }
  }

  public static final String TPCH9 =
      "SELECT\n"
      + "    NATION,\n"
      + "    O_YEAR,\n"
      + "    SUM(AMOUNT) AS SUM_PROFIT\n"
      + " FROM\n"
      + "    (\n"
      + "        SELECT\n"
      + "            N_NAME AS NATION,\n"
      + "            YEAR(O_ORDERDATE) AS O_YEAR,\n"
      + "            L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT\n"
      + "        FROM\n"
      + "            part,\n"
      + "            supplier,\n"
      + "            lineitem,\n"
      + "            partsupp,\n"
      + "            orders,\n"
      + "            nation\n"
      + "        WHERE\n"
      + "            S_SUPPKEY = L_SUPPKEY\n"
      + "            AND PS_SUPPKEY = L_SUPPKEY\n"
      + "            AND PS_PARTKEY = L_PARTKEY\n"
      + "            AND P_PARTKEY = L_PARTKEY\n"
      + "            AND O_ORDERKEY = L_ORDERKEY\n"
      + "            AND S_NATIONKEY = N_NATIONKEY\n"
      + "            AND P_NAME LIKE '%plum%'\n"
      + "    ) AS PROFIT\n"
      + " GROUP BY\n"
      + "    NATION,\n"
      + "    O_YEAR\n"
      + " ORDER BY\n"
      + "    NATION,\n"
      + "    O_YEAR DESC";

  public static final String TPCH9_EXPLAIN =
      "DruidOuterQueryRel(scanProject=[$0, YEAR($1), -(*($2, -(1, $3)), *($4, $5))], group=[{0, 1}], SUM_PROFIT=[SUM($2)], sort=[$0:ASC, $1:DESC])\n"
      + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[1], outputColumns=[15, 13, 5, 4, 12, 8])\n"
      + "    DruidJoinRel(joinType=[INNER], leftKeys=[6], rightKeys=[1])\n"
      + "      DruidJoinRel(joinType=[INNER], leftKeys=[9, 7], rightKeys=[1, 0])\n"
      + "        DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[1], outputColumns=[0, 1, 8, 9, 2, 3, 4, 5, 6, 7])\n"
      + "          DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[3])\n"
      + "            DruidQueryRel(table=[druid.part], scanFilter=[LIKE($4, '%plum%')], scanProject=[$4, $5])\n"
      + "            DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $6, $7, $8, $14])\n"
      + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
      + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3, $4])\n"
      + "      DruidQueryRel(table=[druid.orders], scanProject=[$3, $4])\n"
      + "    DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n";

  public static final Object[][] TPCH9_RESULT = {
      {"ALGERIA", 1998L, 114041.26288207975D},
      {"ALGERIA", 1997L, 420005.5203654439D},
      {"ALGERIA", 1996L, 179435.92888346483D},
      {"ALGERIA", 1995L, 582584.3183930825D},
      {"ALGERIA", 1994L, 460802.84925473155D},
      {"ALGERIA", 1993L, 358757.83164962556D},
      {"ALGERIA", 1992L, 196711.9832104973D},
      {"ARGENTINA", 1998L, 108010.48797064778D},
      {"ARGENTINA", 1997L, 38692.829331616085D},
      {"ARGENTINA", 1996L, 56161.95963088006D},
      {"ARGENTINA", 1995L, 206313.3982588728D},
      {"ARGENTINA", 1994L, 138654.5828448139D},
      {"ARGENTINA", 1993L, 130070.15644093126D},
      {"ARGENTINA", 1992L, 249187.6225541356D},
      {"BRAZIL", 1997L, 275368.15491910925D},
      {"BRAZIL", 1996L, 121697.6010062104D},
      {"BRAZIL", 1995L, 180382.19198142894D},
      {"BRAZIL", 1994L, 135981.81329815474D},
      {"BRAZIL", 1993L, 92990.53442113772D},
      {"BRAZIL", 1992L, 132591.4652138137D},
      {"CANADA", 1998L, 203860.59596039646D},
      {"CANADA", 1997L, 321769.0730324794D},
      {"CANADA", 1996L, 171418.1153069712D},
      {"CANADA", 1995L, 335742.0634719974D},
      {"CANADA", 1994L, 111252.62846753643D},
      {"CANADA", 1993L, 195046.8248396663D},
      {"CANADA", 1992L, 290137.69375805295D},
      {"CHINA", 1998L, 172477.83204571763D},
      {"CHINA", 1997L, 275949.41903671867D},
      {"CHINA", 1996L, 262160.86886519537D},
      {"CHINA", 1995L, 311497.5085121061D},
      {"CHINA", 1994L, 163460.95307904793D},
      {"CHINA", 1993L, 180435.7027487494D},
      {"CHINA", 1992L, 330379.5508661473D},
      {"EGYPT", 1998L, 21087.458950298802D},
      {"EGYPT", 1997L, 103924.7150228204D},
      {"EGYPT", 1996L, 100910.742700351D},
      {"EGYPT", 1995L, 79938.60535488653D},
      {"EGYPT", 1994L, 187349.3030446754D},
      {"EGYPT", 1993L, 330374.52637260495D},
      {"EGYPT", 1992L, 280424.466604101D},
      {"ETHIOPIA", 1998L, 194613.5621839631D},
      {"ETHIOPIA", 1997L, 220107.25268530956D},
      {"ETHIOPIA", 1996L, 158622.32201870752D},
      {"ETHIOPIA", 1995L, 146433.78034954268D},
      {"ETHIOPIA", 1994L, 223731.00827797136D},
      {"ETHIOPIA", 1993L, 392406.45956127666D},
      {"ETHIOPIA", 1992L, 120304.05524324537D},
      {"GERMANY", 1998L, 106323.37565803422D},
      {"GERMANY", 1997L, 92601.54000000001D},
      {"GERMANY", 1996L, 198944.05598116558D},
      {"GERMANY", 1995L, 165687.04067567262D},
      {"GERMANY", 1994L, 226676.94357343644D},
      {"GERMANY", 1993L, 141024.68808797607D},
      {"GERMANY", 1992L, 293949.9785120052D},
      {"INDIA", 1998L, 126584.38706637183D},
      {"INDIA", 1997L, 242388.40911733187D},
      {"INDIA", 1996L, 263227.16703907255D},
      {"INDIA", 1995L, 205509.06789985023D},
      {"INDIA", 1994L, 361137.8302702983D},
      {"INDIA", 1993L, 283929.8668777271D},
      {"INDIA", 1992L, 341885.8311905579D},
      {"INDONESIA", 1998L, 274430.05162858486D},
      {"INDONESIA", 1997L, 465366.50635826826D},
      {"INDONESIA", 1996L, 500014.30167926016D},
      {"INDONESIA", 1995L, 424459.63056589704D},
      {"INDONESIA", 1994L, 346039.4309281166D},
      {"INDONESIA", 1993L, 450136.5637882498D},
      {"INDONESIA", 1992L, 602251.414583133D},
      {"IRAN", 1998L, 131147.61823914346D},
      {"IRAN", 1997L, 87582.15097435769D},
      {"IRAN", 1996L, 95232.70604957585D},
      {"IRAN", 1995L, 115417.67062810008D},
      {"IRAN", 1994L, 190750.94539575384D},
      {"IRAN", 1993L, 78173.58147189885D},
      {"IRAN", 1992L, 9445.441430400575D},
      {"IRAQ", 1998L, 64116.222065007096D},
      {"IRAQ", 1997L, 53046.80409555316D},
      {"IRAQ", 1996L, 98945.09816294358D},
      {"IRAQ", 1994L, -791.6299371585264D},
      {"IRAQ", 1993L, 112985.29805446045D},
      {"IRAQ", 1992L, 90281.52294340223D},
      {"JAPAN", 1998L, 134707.8442967207D},
      {"JAPAN", 1997L, 187434.71473944635D},
      {"JAPAN", 1996L, 130783.96095723026D},
      {"JAPAN", 1995L, 245886.58956717473D},
      {"JAPAN", 1994L, 96861.93096909914D},
      {"JAPAN", 1993L, 91508.39774376526D},
      {"JAPAN", 1992L, 319633.41638581344D},
      {"JORDAN", 1998L, 84023.6913846031D},
      {"JORDAN", 1997L, 248273.9293701095D},
      {"JORDAN", 1996L, 303736.134965854D},
      {"JORDAN", 1995L, 269849.51809366734D},
      {"JORDAN", 1994L, 82437.45704854291D},
      {"JORDAN", 1993L, 290887.21199729946D},
      {"JORDAN", 1992L, 275791.7712003958D},
      {"KENYA", 1998L, 74049.85009683366D},
      {"KENYA", 1997L, 311392.67448551237D},
      {"KENYA", 1996L, 185216.46649997216D},
      {"KENYA", 1995L, 80162.49574048087D},
      {"KENYA", 1994L, 302921.1920338205D},
      {"KENYA", 1993L, 325086.9664950555D},
      {"KENYA", 1992L, 343416.78546852164D},
      {"MOROCCO", 1998L, 119855.49339878328D},
      {"MOROCCO", 1997L, 290008.63337356696D},
      {"MOROCCO", 1996L, 14184.126619798131D},
      {"MOROCCO", 1995L, 69843.47769951589D},
      {"MOROCCO", 1994L, 191099.55208847D},
      {"MOROCCO", 1993L, 137202.08287584715D},
      {"MOROCCO", 1992L, 66594.12967929705D},
      {"MOZAMBIQUE", 1998L, 117097.67474634535D},
      {"MOZAMBIQUE", 1997L, 363205.0374246483D},
      {"MOZAMBIQUE", 1996L, 311449.2716963856D},
      {"MOZAMBIQUE", 1995L, 473208.39547215303D},
      {"MOZAMBIQUE", 1994L, 442759.0845858489D},
      {"MOZAMBIQUE", 1993L, 440542.98936795373D},
      {"MOZAMBIQUE", 1992L, 287795.5268082155D},
      {"PERU", 1998L, 102725.66279401525D},
      {"PERU", 1997L, 171472.8264013625D},
      {"PERU", 1996L, 294416.15261718613D},
      {"PERU", 1995L, 112348.73268373786D},
      {"PERU", 1994L, 95837.18593006684D},
      {"PERU", 1993L, 138317.5969789736D},
      {"PERU", 1992L, 85667.16847534657D},
      {"ROMANIA", 1998L, 2421.287401699462D},
      {"ROMANIA", 1997L, 102189.50098745803D},
      {"ROMANIA", 1996L, 81265.36594303243D},
      {"ROMANIA", 1995L, 47749.04802742277D},
      {"ROMANIA", 1994L, 35394.23633686883D},
      {"ROMANIA", 1993L, 42641.98851210193D},
      {"ROMANIA", 1992L, 49277.804907966856D},
      {"RUSSIA", 1998L, 548958.6482764422D},
      {"RUSSIA", 1997L, 466773.90985315753D},
      {"RUSSIA", 1996L, 901266.0330275361D},
      {"RUSSIA", 1995L, 803254.3646324245D},
      {"RUSSIA", 1994L, 932974.120513519D},
      {"RUSSIA", 1993L, 843491.4803470026D},
      {"RUSSIA", 1992L, 876831.2496177027D},
      {"UNITED KINGDOM", 1998L, 81480.06686721234D},
      {"UNITED KINGDOM", 1997L, 58282.63452262785D},
      {"UNITED KINGDOM", 1996L, 134110.58770714886D},
      {"UNITED KINGDOM", 1995L, 83918.57284126579D},
      {"UNITED KINGDOM", 1994L, 70544.89821118998D},
      {"UNITED KINGDOM", 1993L, 55681.247072249615D},
      {"UNITED KINGDOM", 1992L, 31602.86316145718D},
      {"UNITED STATES", 1998L, 196681.86753583295D},
      {"UNITED STATES", 1997L, 311459.70298314217D},
      {"UNITED STATES", 1996L, 451144.5765293425D},
      {"UNITED STATES", 1995L, 481350.94638033805D},
      {"UNITED STATES", 1994L, 473742.82106392196D},
      {"UNITED STATES", 1993L, 324866.8118531974D},
      {"UNITED STATES", 1992L, 343496.2652782099D},
      {"VIETNAM", 1998L, 198132.13110275078D},
      {"VIETNAM", 1997L, 426951.29134074517D},
      {"VIETNAM", 1996L, 610135.1674077166D},
      {"VIETNAM", 1995L, 316695.85186926863D},
      {"VIETNAM", 1994L, 489111.948197652D},
      {"VIETNAM", 1993L, 343970.28961719247D},
      {"VIETNAM", 1992L, 352275.066762814D}
  };

  @Test
  public void tpch9() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        TPCH9,
        TPCH9_EXPLAIN,
        TPCH9_RESULT
    );

    if (broadcastJoin) {
      hook.verifyHooked(
          "RDhhM15yFhdYJx7H4H487w==",
          "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_NAME, P_PARTKEY]}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=lineitem, rightJoinColumns=[L_PARTKEY]}, hashLeft=true, hashSignature={P_NAME:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
          "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part, leftJoinColumns=[P_PARTKEY], rightAlias=lineitem, rightJoinColumns=[L_PARTKEY]}, hashLeft=true, hashSignature={P_NAME:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=part+lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}]}",
          "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], $hash=true}",
          "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    } else if (bloomFilter) {
      hook.verifyHooked(
          "dqjBmS8Kx3037+nuvevu7w==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_NAME, P_PARTKEY], $hash=true}, StreamQuery{dataSource='lineitem', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](P_NAME LIKE '%plum%'), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=49}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
          "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE '%plum%', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=49}]}",
          "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_NAME, P_PARTKEY], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
          "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "JPbiijgAMRqOqPD5wDJ1lw==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_NAME, P_PARTKEY], $hash=true}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d1:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='((L_EXTENDEDPRICE * (1 - L_DISCOUNT)) - (PS_SUPPLYCOST * L_QUANTITY))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=descending}], limit=-1}, outputColumns=[d0, d1, a0]}",
          "StreamQuery{dataSource='part', filter=P_NAME LIKE '%plum%', columns=[P_NAME, P_PARTKEY], $hash=true}",
          "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], $hash=true}",
          "StreamQuery{dataSource='orders', columns=[O_ORDERDATE, O_ORDERKEY]}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    }
  }

  @Test
  public void tpch10() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT\n"
        + "    C_CUSTKEY,\n"
        + "    C_NAME,\n"
        + "    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,\n"
        + "    C_ACCTBAL,\n"
        + "    N_NAME,\n"
        + "    C_ADDRESS,\n"
        + "    C_PHONE,\n"
        + "    C_COMMENT\n"
        + " FROM\n"
        + "    customer,\n"
        + "    orders,\n"
        + "    lineitem,\n"
        + "    nation\n"
        + " WHERE\n"
        + "    C_CUSTKEY = O_CUSTKEY\n"
        + "    AND L_ORDERKEY = O_ORDERKEY\n"
        + "    AND O_ORDERDATE >= '1993-07-01'\n"
        + "    AND O_ORDERDATE < '1993-10-01'\n"
        + "    AND L_RETURNFLAG = 'R'\n"
        + "    AND C_NATIONKEY = N_NATIONKEY\n"
        + " GROUP BY\n"
        + "    C_CUSTKEY,\n"
        + "    C_NAME,\n"
        + "    C_ACCTBAL,\n"
        + "    C_PHONE,\n"
        + "    N_NAME,\n"
        + "    C_ADDRESS,\n"
        + "    C_COMMENT\n"
        + " ORDER BY\n"
        + "    REVENUE DESC\n"
        + " LIMIT 20",
        "DruidOuterQueryRel(scanProject=[$0, $1, $2, $3, $4, $5, $6, *($7, -(1, $8))], group=[{0, 1, 2, 3, 4, 5, 6}], REVENUE=[SUM($7)], sort=[$7:DESC], fetch=[20], sortProject=[$0, $1, $7, $2, $4, $5, $3, $6])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1], outputColumns=[3, 4, 0, 6, 14, 1, 2, 11, 10])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[9], rightKeys=[2])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0])\n"
        + "        DruidQueryRel(table=[druid.customer], scanProject=[$0, $1, $2, $3, $5, $6, $7])\n"
        + "        DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-07-01'), <($3, '1993-10-01'))], scanProject=[$2, $3, $4])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[=($10, 'R')], scanProject=[$2, $3, $6, $10])\n"
        + "    DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n",
        new Object[]{"22", "Customer#000000022", 376659.3379452322D, 591.98D, "CANADA", "QI6p41,FNs5k7RZoCCVPUTkUdYpB", "13-806-545-9701", "s nod furiously above the furiously ironic ideas. "},
        new Object[]{"217", "Customer#000000217", 337546.5029809665D, 378.33D, "UNITED KINGDOM", "YIy05RMdthrXqdfnNKud", "33-159-298-3849", "ven frays wake according to the carefully "},
        new Object[]{"715", "Customer#000000715", 327733.10233764054D, 85.05D, "ROMANIA", "9qLvF42uxUarKl4I 2pEKOMNJmo8Ro5EK", "29-500-408-6392", "hins boost quickly. quickly regular epitaphs haggle fluffily quickly bold pinto beans. regular"},
        new Object[]{"55", "Customer#000000055", 325304.2184793751D, 4572.11D, "IRAN", "zIRBR4KNEl HzaiV3a i9n6elrxzDEh8r8pDom", "20-180-440-8525", "ully unusual packages wake bravely bold packages. unusual requests boost deposits! blithely ironic packages ab"},
        new Object[]{"19", "Customer#000000019", 295856.25229804683D, 8914.71D, "CHINA", "uc,3bHIx84H,wdrmLOjVsiqXCq2tr", "28-396-526-5053", " nag. furiously careful packages are slyly at the accounts. furiously regular in"},
        new Object[]{"686", "Customer#000000686", 284498.96677950415D, 5503.36D, "FRANCE", "1j C80VWHe ITCVCV", "16-682-293-3599", " even deposits print quickly. foxes wake. furiously ironic asymptotes across the bold foxes"},
        new Object[]{"202", "Customer#000000202", 280435.6192224468D, 2237.64D, "GERMANY", "Q0uJ1frCbi9yvu", "17-905-805-4635", "fully along the carefully pending Tiresias; special packages along the carefully special deposits try to"},
        new Object[]{"679", "Customer#000000679", 268885.680341735D, 1394.44D, "IRAN", "IJf1FlZL9I9m,rvofcoKy5pRUOjUQV", "20-146-696-9508", "ely pending frays boost carefully"},
        new Object[]{"448", "Customer#000000448", 260133.3756423737D, 8117.27D, "UNITED STATES", "BH4vtnDpabk0NgoGNJWu4OUXnidfJ", "34-985-422-6009", "unts. final pinto beans boost carefully. furiously even foxes according to the express, regular pa"},
        new Object[]{"394", "Customer#000000394", 245405.0088580988D, 5200.96D, "UNITED KINGDOM", "nxW1jt,MQvImdr z72gAt1bslnfEipCh,bKZN", "33-422-600-6936", " instructions. carefully special ideas after the fluffily unusual r"},
        new Object[]{"64", "Customer#000000064", 245401.5889329308D, -646.64D, "CANADA", "MbCeGY20kaKK3oalJD,OT", "13-558-731-7204", "structions after the quietly ironic theodolites cajole be"},
        new Object[]{"559", "Customer#000000559", 243818.187256828D, 5872.94D, "GERMANY", "A3ACFoVbP,gPe xknVJMWC,wmRxb Nmg fWFS,UP", "17-395-429-6655", "al accounts cajole carefully across the accounts. furiously pending pinto beans across the "},
        new Object[]{"586", "Customer#000000586", 242057.2150677127D, 5134.35D, "IRAQ", "vGaA9XBtn,hlswFhSjLIXGlLEDD2flE8UXwj", "21-239-369-7791", "above the blithely express ideas. slyly r"},
        new Object[]{"721", "Customer#000000721", 234606.65694861457D, 3420.64D, "VIETNAM", "N6hr4gV9EkPBuE3Ayu ", "31-174-552-2949", "ar instructions. packages haggle stealthily ironic deposits. even platelets detect quickly. even sheaves along"},
        new Object[]{"65", "Customer#000000065", 228551.89613367125D, 8795.16D, "UNITED KINGDOM", "RGT yzQ0y4l0H90P783LG4U95bXQFDRXbWa1sl,X", "33-733-623-5267", "y final foxes serve carefully. theodolites are carefully. pending i"},
        new Object[]{"352", "Customer#000000352", 226905.6798173411D, 6257.88D, "INDONESIA", "HqhIE5GRTK0dFtWpJUQENU4aa1bwdsUBEWtzUw", "19-906-158-8420", "ts are. blithely special requests wake. furiously bold packages among the blithely eve"},
        new Object[]{"79", "Customer#000000079", 220721.16073114896D, 5121.28D, "MOROCCO", "n5hH2ftkVRwW8idtD,BmM2", "25-147-850-4166", "es. packages haggle furiously. regular, special requests poach after the quickly express ideas. blithely pending re"},
        new Object[]{"710", "Customer#000000710", 217848.30989936687D, 7412.12D, "RUSSIA", "OCLSZuXw1AEK NLvlofMkuK,YNe,bJD40a", "32-459-427-9559", "ges integrate express, even ideas"},
        new Object[]{"484", "Customer#000000484", 213702.96280260698D, 4245.0D, "SAUDI ARABIA", "ismzlUzrqRMRGWmCEUUjkBsi", "30-777-953-8902", "y against the express, even packages. blithely pending pearls haggle furiously above the fur"},
        new Object[]{"292", "Customer#000000292", 203414.1759173521D, 2975.43D, "IRAQ", "hCXh3vxC4uje9", "21-457-910-2923", "usly regular, ironic accounts. blithely regular platelets are carefully. blithely unusual ideas affi"}
    );
    if (bloomFilter) {
      hook.verifyHooked(
          "J4nBag7dgZPnq44bO9ZMEQ==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=286}, columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_RETURNFLAG]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
          "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=286}]}",
          "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
          "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_RETURNFLAG]}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "dBNEZJWaPUTYkDqLa0RAUw==",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_RETURNFLAG]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
          "StreamQuery{dataSource='customer', columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
          "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_RETURNFLAG]}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
      );
    }
  }

  @Test
  public void tpch11() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT\n"
        + "  *\n"
        + " FROM (\n"
        + "  SELECT\n"
        + "    PS_PARTKEY,\n"
        + "    SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS PART_VALUE\n"
        + "  FROM\n"
        + "    partsupp,\n"
        + "    supplier,\n"
        + "    nation\n"
        + "  WHERE\n"
        + "    PS_SUPPKEY = S_SUPPKEY\n"
        + "    AND S_NATIONKEY = N_NATIONKEY\n"
        + "    AND N_NAME = 'GERMANY'\n"
        + "  GROUP BY\n"
        + "    PS_PARTKEY\n"
        + " ) AS inner_query\n"
        + " WHERE\n"
        + "  PART_VALUE > (\n"
        + "    SELECT\n"
        + "      SUM(PS_SUPPLYCOST * PS_AVAILQTY)\n"
        + "    FROM\n"
        + "      partsupp,\n"
        + "      supplier,\n"
        + "      nation\n"
        + "    WHERE\n"
        + "      PS_SUPPKEY = S_SUPPKEY\n"
        + "      AND S_NATIONKEY = N_NATIONKEY\n"
        + "      AND N_NAME = 'GERMANY'\n"
        + "  ) * 0.0001\n"
        + " ORDER BY PART_VALUE DESC",
        "DruidOuterQueryRel(scanFilter=[>($1, *($2, 0.0001:DECIMAL(5, 4)))], sort=[$1:DESC], sortProject=[$0, $1])\n"
        + "  DruidJoinRel(joinType=[LEFT])\n"
        + "    DruidOuterQueryRel(scanProject=[$0, *($1, $2)], group=[{0}], PART_VALUE=[SUM($1)])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[2], outputColumns=[5, 7, 4])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0])\n"
        + "          DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$1, $2])\n"
        + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
        + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $2, $3, $4])\n"
        + "    DruidOuterQueryRel(scanProject=[*($0, $1)], EXPR$0=[SUM($0)])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[1], outputColumns=[6, 4])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0])\n"
        + "          DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$1, $2])\n"
        + "          DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
        + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $3, $4])\n",
        new Object[]{"657", 7952582.180000001D},
        new Object[]{"527", 7799597.1899999995D},
        new Object[]{"570", 7665801.33D},
        new Object[]{"93", 7645677.04D},
        new Object[]{"187", 7363442.4D},
        new Object[]{"715", 7215116.4799999995D},
        new Object[]{"549", 7115227.5D},
        new Object[]{"143", 7037648.64D},
        new Object[]{"989", 6970889.640000001D},
        new Object[]{"69", 6970845.15D},
        new Object[]{"115", 6706677.33D},
        new Object[]{"668", 6702794.699999999D},
        new Object[]{"943", 6458641.7D},
        new Object[]{"619", 6381886.600000001D},
        new Object[]{"751", 6334687.3D},
        new Object[]{"493", 6144995.15D},
        new Object[]{"730", 6100067.88D},
        new Object[]{"693", 5819806.220000001D},
        new Object[]{"19", 5593808.0D},
        new Object[]{"386", 5475929.1899999995D},
        new Object[]{"82", 5434136.829999999D},
        new Object[]{"942", 5409015.8D},
        new Object[]{"182", 5191583.7299999995D},
        new Object[]{"543", 5189101.68D},
        new Object[]{"743", 5039271.42D},
        new Object[]{"993", 4973063.04D},
        new Object[]{"732", 4932809.82D},
        new Object[]{"163", 4901152.25D},
        new Object[]{"211", 4879076.01D},
        new Object[]{"516", 4676882.0D},
        new Object[]{"216", 4561143.8D},
        new Object[]{"826", 4410041.96D},
        new Object[]{"895", 4345555.02D},
        new Object[]{"597", 4341311.640000001D},
        new Object[]{"482", 4315726.819999999D},
        new Object[]{"198", 4315049.0D},
        new Object[]{"403", 4283221.600000001D},
        new Object[]{"981", 4259584.13D},
        new Object[]{"442", 4107626.12D},
        new Object[]{"521", 3913058.16D},
        new Object[]{"864", 3728780.16D},
        new Object[]{"582", 3717165.4000000004D},
        new Object[]{"837", 3640184.0100000002D},
        new Object[]{"423", 3602031.8000000003D},
        new Object[]{"293", 3463498.8D},
        new Object[]{"902", 3459429.6D},
        new Object[]{"832", 3447746.46D},
        new Object[]{"922", 3387665.52D},
        new Object[]{"933", 3368071.5D},
        new Object[]{"682", 3333963.96D},
        new Object[]{"140", 3155253.92D},
        new Object[]{"394", 3118101.5599999996D},
        new Object[]{"167", 3034999.03D},
        new Object[]{"382", 3014555.96D},
        new Object[]{"451", 2926228.56D},
        new Object[]{"7", 2834026.8D},
        new Object[]{"789", 2782828.96D},
        new Object[]{"704", 2761460.85D},
        new Object[]{"422", 2710606.7800000003D},
        new Object[]{"970", 2702851.9099999997D},
        new Object[]{"250", 2647099.56D},
        new Object[]{"43", 2587359.58D},
        new Object[]{"717", 2574344.56D},
        new Object[]{"443", 2382150.05D},
        new Object[]{"882", 2321004.56D},
        new Object[]{"245", 2319450.9D},
        new Object[]{"346", 2318315.7600000002D},
        new Object[]{"480", 2316197.2399999998D},
        new Object[]{"608", 2225519.48D},
        new Object[]{"856", 2211867.0D},
        new Object[]{"328", 2208471.1900000004D},
        new Object[]{"8", 2172003.9D},
        new Object[]{"848", 2136067.6D},
        new Object[]{"132", 2125100.6799999997D},
        new Object[]{"621", 2115063.27D},
        new Object[]{"67", 2107088.16D},
        new Object[]{"265", 2093560.02D},
        new Object[]{"193", 2077700.32D},
        new Object[]{"118", 2059808.86D},
        new Object[]{"298", 2026981.74D},
        new Object[]{"355", 2010156.61D},
        new Object[]{"559", 1987422.84D},
        new Object[]{"782", 1866799.55D},
        new Object[]{"574", 1797527.3D},
        new Object[]{"80", 1740096.97D},
        new Object[]{"532", 1682311.48D},
        new Object[]{"243", 1603235.16D},
        new Object[]{"762", 1594238.3D},
        new Object[]{"893", 1533657.58D},
        new Object[]{"643", 1512838.8D},
        new Object[]{"393", 1484779.2000000002D},
        new Object[]{"129", 1450282.92D},
        new Object[]{"412", 1446605.2D},
        new Object[]{"276", 1414562.67D},
        new Object[]{"632", 1408087.26D},
        new Object[]{"46", 1361500.28D},
        new Object[]{"104", 1340876.41D},
        new Object[]{"292", 1327114.13D},
        new Object[]{"741", 1270376.7999999998D},
        new Object[]{"227", 1235815.6800000002D},
        new Object[]{"259", 1178367.68D},
        new Object[]{"793", 1081096.2D},
        new Object[]{"932", 980620.6799999999D},
        new Object[]{"325", 971188.85D},
        new Object[]{"32", 894484.09D},
        new Object[]{"809", 858034.3400000001D},
        new Object[]{"962", 846619.25D},
        new Object[]{"885", 836872.4D},
        new Object[]{"874", 834634.7100000001D},
        new Object[]{"20", 832802.32D},
        new Object[]{"374", 729413.96D},
        new Object[]{"490", 720683.6D},
        new Object[]{"178", 695176.94D},
        new Object[]{"433", 680530.32D},
        new Object[]{"339", 654195.71D},
        new Object[]{"563", 643298.76D},
        new Object[]{"375", 601371.5499999999D},
        new Object[]{"843", 589964.5499999999D},
        new Object[]{"755", 554786.94D},
        new Object[]{"469", 499663.14999999997D},
        new Object[]{"903", 446803.76D},
        new Object[]{"913", 425798.64999999997D},
        new Object[]{"634", 420249.7D},
        new Object[]{"363", 400413.19999999995D},
        new Object[]{"706", 392540.80000000005D},
        new Object[]{"1000", 384893.60000000003D},
        new Object[]{"593", 381772.8D},
        new Object[]{"815", 377968.08D},
        new Object[]{"432", 370528.51999999996D},
        new Object[]{"472", 295403.24D},
        new Object[]{"152", 278490.62D},
        new Object[]{"610", 272143.99D},
        new Object[]{"766", 259972.93D},
        new Object[]{"31", 236051.49D},
        new Object[]{"307", 224806.82D},
        new Object[]{"538", 223747.85000000003D},
        new Object[]{"804", 222522.30000000002D},
        new Object[]{"232", 211879.92D},
        new Object[]{"778", 204061.15000000002D},
        new Object[]{"586", 185146.12D},
        new Object[]{"314", 171835.30000000002D},
        new Object[]{"982", 155171.73D},
        new Object[]{"234", 151548.0D},
        new Object[]{"54", 138952.64D},
        new Object[]{"951", 127107.90000000001D},
        new Object[]{"510", 125887.92D},
        new Object[]{"332", 112181.3D},
        new Object[]{"56", 69545.7D},
        new Object[]{"343", 56511.840000000004D},
        new Object[]{"461", 54348.78D}
    );
    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "BEdFK3KGl/Jg0qeGaHtQ0Q==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation, leftJoinColumns=[N_NATIONKEY], rightAlias=supplier, rightJoinColumns=[S_NATIONKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation, leftJoinColumns=[N_NATIONKEY], rightAlias=supplier, rightJoinColumns=[S_NATIONKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_SUPPLYCOST, PS_AVAILQTY]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0, a00], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}], outputColumns=[d0, a0]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY]}",
            "TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_SUPPLYCOST, PS_AVAILQTY]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_SUPPLYCOST, PS_AVAILQTY]}"
        );
      } else {
        hook.verifyHooked(
            "/28hUM72yD0Kkt6tG5tnqw==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_SUPPLYCOST, PS_AVAILQTY]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0, a00], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}], outputColumns=[d0, a0]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY]}",
            "TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_SUPPLYCOST, PS_AVAILQTY]}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_SUPPKEY', values=[33, 44]}, columns=[PS_SUPPLYCOST, PS_AVAILQTY]}"
        );
      }
    } else if (broadcastJoin) {
      if (bloomFilter) {
        hook.verifyHooked(
            "3IotJqstj9oFmUekW9J5ig==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation, leftJoinColumns=[N_NATIONKEY], rightAlias=supplier, rightJoinColumns=[S_NATIONKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation, leftJoinColumns=[N_NATIONKEY], rightAlias=supplier, rightJoinColumns=[S_NATIONKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0, a00], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}], outputColumns=[d0, a0]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
            "TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}"
        );
      } else {
        hook.verifyHooked(
            "3IotJqstj9oFmUekW9J5ig==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation, leftJoinColumns=[N_NATIONKEY], rightAlias=supplier, rightJoinColumns=[S_NATIONKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation, leftJoinColumns=[N_NATIONKEY], rightAlias=supplier, rightJoinColumns=[S_NATIONKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0, a00], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}], outputColumns=[d0, a0]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
            "TimeseriesQuery{dataSource='StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fieldNames=[PS_SUPPKEY], groupingSets=Noop}, columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=nation+supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=partsupp, rightJoinColumns=[PS_SUPPKEY]}, hashLeft=true, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}"
        );
      }
    } else {
      hook.verifyHooked(
          "VFkGdOUN6voriPZ3uPJmNA==",
          "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}, TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}], timeColumnName=__time}', filter=MathExprFilter{expression='(a0 > (a00 * 0.0001B))'}, columns=[d0, a0, a00], orderingSpecs=[OrderByColumnSpec{dimension='a0', direction=descending}], outputColumns=[d0, a0]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(PS_SUPPLYCOST * PS_AVAILQTY)', inputType='double'}], outputColumns=[a0]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='GERMANY', columns=[N_NAME, N_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_SUPPKEY, PS_SUPPLYCOST]}"
      );
    }
  }

  @Test
  public void tpch12() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "select\n"
        + "    L_SHIPMODE,\n"
        + "    sum(case\n"
        + "        when O_ORDERPRIORITY = '1-URGENT'\n"
        + "            or O_ORDERPRIORITY = '2-HIGH'\n"
        + "            then 1\n"
        + "        else 0\n"
        + "    end) as high_line_count,\n"
        + "    sum(case\n"
        + "        when O_ORDERPRIORITY <> '1-URGENT'\n"
        + "            and O_ORDERPRIORITY <> '2-HIGH'\n"
        + "            then 1\n"
        + "        else 0\n"
        + "    end) as low_line_count\n"
        + " from\n"
        + "    orders,\n"
        + "    lineitem\n"
        + " where\n"
        + "    O_ORDERKEY = L_ORDERKEY\n"
        + "    and L_SHIPMODE in ('REG AIR', 'MAIL')\n"
        + "    and L_COMMITDATE < L_RECEIPTDATE\n"
        + "    and L_SHIPDATE < L_COMMITDATE\n"
        + "    and L_RECEIPTDATE >= '1995-01-01'\n"
        + "    and L_RECEIPTDATE < '1996-01-01'\n"
        + " group by\n"
        + "    L_SHIPMODE\n"
        + " order by\n"
        + "    L_SHIPMODE",
        "DruidOuterQueryRel(scanProject=[$0, CASE(OR(=($1, '1-URGENT'), =($1, '2-HIGH')), 1, 0), CASE(AND(<>($1, '1-URGENT'), <>($1, '2-HIGH')), 1, 0), IS TRUE(OR(=($1, '1-URGENT'), =($1, '2-HIGH'))), IS TRUE(AND(<>($1, '1-URGENT'), <>($1, '2-HIGH')))], group=[{0}], high_line_count=[COUNT() FILTER $3], low_line_count=[COUNT() FILTER $4], sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[6, 1])\n"
        + "    DruidQueryRel(table=[druid.orders], scanProject=[$4, $5])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(OR(=($13, 'REG AIR'), =($13, 'MAIL')), <($1, $9), <($11, $1), >=($9, '1995-01-01'), <($9, '1996-01-01'))], scanProject=[$1, $6, $9, $11, $13])\n",
        new Object[]{"MAIL", 34L, 44L},
        new Object[]{"REG AIR", 37L, 43L}
    );
    if (bloomFilter) {
      hook.verifyHooked(
          "IT1PSqVNOhWT800J+7dJgg==",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY]((InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)})), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=158}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}, StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SHIPDATE, L_SHIPMODE], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{, delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{, delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
          "TimeseriesQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=158}]}",
          "StreamQuery{dataSource='orders', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}",
          "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SHIPDATE, L_SHIPMODE], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "3lfF9idK8o7a4dFY3LvgzQ==",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', columns=[O_ORDERKEY, O_ORDERPRIORITY]}, StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SHIPDATE, L_SHIPMODE], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{, delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{, delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
          "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, O_ORDERPRIORITY]}",
          "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SHIPDATE, L_SHIPMODE], $hash=true}"
      );
    }
  }

  @Test
  public void tpch13() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "select\n"
        + "    c_count,\n"
        + "    count(*) as custdist\n"
        + " from (\n"
        + "   select\n"
        + "      C_CUSTKEY,\n"
        + "      count(O_ORDERKEY) as c_count\n"
        + "   from\n"
        + "      customer left outer join orders on C_CUSTKEY = O_CUSTKEY and O_COMMENT not like '%unusual%accounts%'\n"
        + "   group by C_CUSTKEY\n"
        + " ) c_orders\n"
        + " group by c_count\n"
        + " order by custdist desc, c_count desc",
        "DruidOuterQueryRel(group=[{0}], custdist=[COUNT()], sort=[$1:DESC, $0:DESC])\n"
        + "  DruidOuterQueryRel(group=[{0}], c_count=[COUNT($3)], aggregateProject=[$1])\n"
        + "    DruidJoinRel(joinType=[LEFT], leftKeys=[0], rightKeys=[1])\n"
        + "      DruidQueryRel(table=[druid.customer], scanProject=[$3])\n"
        + "      DruidQueryRel(table=[druid.orders], scanFilter=[NOT(LIKE($1, '%unusual%accounts%'))], scanProject=[$1, $2, $4])\n",
        new Object[]{0L, 250L},
        new Object[]{12L, 34L},
        new Object[]{11L, 34L},
        new Object[]{19L, 29L},
        new Object[]{14L, 29L},
        new Object[]{8L, 28L},
        new Object[]{7L, 26L},
        new Object[]{20L, 25L},
        new Object[]{17L, 24L},
        new Object[]{13L, 24L},
        new Object[]{9L, 24L},
        new Object[]{18L, 23L},
        new Object[]{16L, 23L},
        new Object[]{15L, 22L},
        new Object[]{21L, 21L},
        new Object[]{10L, 19L},
        new Object[]{6L, 18L},
        new Object[]{23L, 17L},
        new Object[]{22L, 17L},
        new Object[]{27L, 9L},
        new Object[]{26L, 9L},
        new Object[]{24L, 9L},
        new Object[]{5L, 9L},
        new Object[]{4L, 8L},
        new Object[]{25L, 6L},
        new Object[]{30L, 4L},
        new Object[]{29L, 4L},
        new Object[]{28L, 2L},
        new Object[]{3L, 2L},
        new Object[]{2L, 1L}
    );
    hook.verifyHooked(
        "58JWbIH3PcUlOpmkuywz/A==",
        "GroupByQuery{dataSource='GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}, StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_COMMENT, O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0', fieldName='O_ORDERKEY'}], outputColumns=[a0]}', dimensions=[DefaultDimensionSpec{dimension='a0', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=descending}], limit=-1}, outputColumns=[d0, _a0]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}, StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_COMMENT, O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0', fieldName='O_ORDERKEY'}], outputColumns=[a0]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_COMMENT, O_CUSTKEY, O_ORDERKEY], $hash=true}"
    );
  }

  @Test
  public void tpch14() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "select\n"
        + " 100.00 * sum(case\n"
        + "    when P_TYPE like 'PROMO%'\n"
        + "      then L_EXTENDEDPRICE * (1 - L_DISCOUNT)\n"
        + "    else 0\n"
        + "  end) / sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as promo_revenue\n"
        + " from\n"
        + "  lineitem,\n"
        + "  part\n"
        + " where\n"
        + "  L_PARTKEY = P_PARTKEY\n"
        + " and L_SHIPDATE >= '1995-08-01'\n"
        + " and L_SHIPDATE < '1995-09-01'",
        "DruidOuterQueryRel(scanProject=[CASE(LIKE($0, 'PROMO%'), *($1, -(1, $2)), 0:DOUBLE), *($1, -(1, $2))], agg#0=[SUM($0)], agg#1=[SUM($1)], aggregateProject=[/(*(100.00:DECIMAL(5, 2), $0), $1)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[5, 1, 0])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1995-08-01'), <($11, '1995-09-01'))], scanProject=[$2, $3, $7, $11])\n"
        + "    DruidQueryRel(table=[druid.part], scanProject=[$5, $8])\n",
        new Object[]{21.62198225363824}
    );
    List<String> expected;
    if (bloomFilter) {
      expected = Arrays.asList(
          "1GnPq1OKfTeENvIY0z9Aow==",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_SHIPDATE], $hash=true}, StreamQuery{dataSource='part', filter=BloomDimFilter.Factory{bloomSource=$view:lineitem[L_PARTKEY](BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop, maxNumEntries=408}, columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
          "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=408}]}",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_SHIPDATE], $hash=true}",
          "StreamQuery{dataSource='part', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop}, columns=[P_PARTKEY, P_TYPE]}"
      );
    } else {
      expected = Arrays.asList(
          "hs+L3X6VpqaTZ/RRAsj43Q==",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_SHIPDATE], $hash=true}, StreamQuery{dataSource='part', columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_SHIPDATE], $hash=true}",
          "StreamQuery{dataSource='part', columns=[P_PARTKEY, P_TYPE]}"
      );
    }
    hook.verifyHooked(expected);
  }

  @Test
  public void tpch15() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH revenue_cached AS (\n"
        + " SELECT\n"
        + "    L_SUPPKEY AS SUPPLIER_NO,\n"
        + " SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE\n"
        + " FROM\n"
        + "    lineitem\n"
        + " WHERE\n"
        + "    L_SHIPDATE >= '1996-01-01'\n"
        + " AND L_SHIPDATE < '1996-04-01'\n"
        + " GROUP BY L_SUPPKEY\n"
        + "),\n"
        + "max_revenue_cached AS (\n"
        + " SELECT\n"
        + " MAX(TOTAL_REVENUE) AS MAX_REVENUE\n"
        + " FROM\n"
        + "    revenue_cached\n"
        + ")\n"
        + " SELECT\n"
        + "    S_SUPPKEY,\n"
        + "    S_NAME,\n"
        + "    S_ADDRESS,\n"
        + "    S_PHONE,\n"
        + "    TOTAL_REVENUE\n"
        + " FROM\n"
        + "    supplier,\n"
        + "    revenue_cached,\n"
        + "    max_revenue_cached\n"
        + " WHERE\n"
        + "    S_SUPPKEY = SUPPLIER_NO\n"
        + " AND TOTAL_REVENUE = MAX_REVENUE\n"
        + " ORDER BY S_SUPPKEY",
        "DruidOuterQueryRel(sort=[$6:ASC], sortProject=[$6, $4, $3, $5, $2])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[0], outputColumns=[6, 4, 5, 0, 1, 2, 3])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0])\n"
        + "      DruidQueryRel(table=[druid.supplier], scanProject=[$1, $3, $5, $6])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)])\n"
        + "    DruidOuterQueryRel(MAX_REVENUE=[MAX($0)])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)], aggregateProject=[$1])\n",
        new Object[]{"6", "Supplier#000000006", "tQxuVm7s7CnK", "24-696-997-4969", 1080265.1420867585D}
    );
    hook.verifyHooked(
        "MWgjPLuX2NKb+GVAYUJakw==",
        "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
        "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}, TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0]}], timeColumnName=__time}', columns=[_a0, d0, a0, S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}], outputColumns=[S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, a0]}",
        "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}",
        "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}",
        "TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0]}",
        "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}"
    );
  }

  @Test
  public void tpch16() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "select\n"
        + "    P_BRAND,\n"
        + "    P_TYPE,\n"
        + "    P_SIZE,\n"
        + "    count(distinct PS_SUPPKEY) as supplier_cnt\n"
        + " from\n"
        + "    partsupp,\n"
        + "    part\n"
        + " where\n"
        + "    P_PARTKEY = PS_PARTKEY\n"
        + "    and P_BRAND <> 'Brand#34'\n"
        + "    and P_TYPE not like 'ECONOMY BRUSHED%'\n"
        + "    and P_SIZE in (22, 14, 27, 49, 21, 33, 35, 28)\n"
        + "    and partsupp.PS_SUPPKEY not in (\n"
        + "        select\n"
        + "            S_SUPPKEY\n"
        + "        from\n"
        + "            supplier\n"
        + "        where\n"
        + "            S_COMMENT like '%Customer%Complaints%'\n"
        + "    )\n"
        + " group by\n"
        + "    P_BRAND,\n"
        + "    P_TYPE,\n"
        + "    P_SIZE\n"
        + " order by\n"
        + "    supplier_cnt desc,\n"
        + "    P_BRAND,\n"
        + "    P_TYPE,\n"
        + "    P_SIZE",
        "DruidOuterQueryRel(scanFilter=[OR(=($6, 0), AND(IS NULL($9), >=($7, $6), IS NOT NULL($1)))], group=[{2, 4, 5}], supplier_cnt=[COUNT(DISTINCT $1)], sort=[$3:DESC, $0:ASC, $2:ASC, $1:ASC], sortProject=[$0, $2, $1, $3])\n"
        + "  DruidJoinRel(joinType=[LEFT], leftKeys=[1], rightKeys=[0])\n"
        + "    DruidJoinRel(joinType=[INNER])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
        + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3])\n"
        + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(<>($0, 'Brand#34'), OR(=($7, 22), =($7, 14), =($7, 27), =($7, 49), =($7, 21), =($7, 33), =($7, 35), =($7, 28)), NOT(LIKE($8, 'ECONOMY BRUSHED%')))], scanProject=[$0, $5, $7, $8])\n"
        + "      DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6], c=[COUNT()], ck=[COUNT($0)])\n"
        + "    DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6, true], group=[{0, 1}])\n"
    );
    if (bloomFilter) {
      hook.verifyHooked(
          "VN0iL9PWViSSDlsE6bmXDg==",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((!(P_BRAND=='Brand#34') && InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_TYPE LIKE 'ECONOMY BRUSHED%'))), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=149}, columns=[PS_PARTKEY, PS_SUPPKEY]}, StreamQuery{dataSource='part', filter=(!(P_BRAND=='Brand#34') && InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d2'}], filter=(a0=='0' || (d1==NULL && MathExprFilter{expression='(a1 >= a0)'} && !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fields=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='PS_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}], limit=-1}, outputColumns=[_d0, _d2, _d1, _a0]}",
          "TimeseriesQuery{dataSource='part', filter=(!(P_BRAND=='Brand#34') && InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=149}]}",
          "StreamQuery{dataSource='partsupp', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY]}",
          "StreamQuery{dataSource='part', filter=(!(P_BRAND=='Brand#34') && InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
          "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "HseP6/T8y/FaTX0PkiC3tQ==",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY]}, StreamQuery{dataSource='part', filter=(!(P_BRAND=='Brand#34') && InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d2'}], filter=(a0=='0' || (d1==NULL && MathExprFilter{expression='(a1 >= a0)'} && !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fields=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='PS_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}], limit=-1}, outputColumns=[_d0, _d2, _d1, _a0]}",
          "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY]}",
          "StreamQuery{dataSource='part', filter=(!(P_BRAND=='Brand#34') && InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
          "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
      );
    }
  }

  @Test
  public void tpch17() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH Q17_PART AS (\n"
        + "  SELECT P_PARTKEY FROM part WHERE\n"
        + "  P_BRAND = 'Brand#31'\n"      // changed 23 to 31
        + "  AND P_CONTAINER = 'MED BOX'\n"
        + "),\n"
        + "Q17_AVG AS (\n"
        + "  SELECT L_PARTKEY AS T_PARTKEY, 0.2 * AVG(L_QUANTITY) AS T_AVG_QUANTITY\n"
        + "  FROM lineitem\n"
        + "  WHERE L_PARTKEY IN (SELECT P_PARTKEY FROM Q17_PART)\n"
        + "  GROUP BY L_PARTKEY\n"
        + "),\n"
        + "Q17_PRICE AS (\n"
        + "  SELECT\n"
        + "  L_QUANTITY,\n"
        + "  L_PARTKEY,\n"
        + "  L_EXTENDEDPRICE\n"
        + "  FROM\n"
        + "  lineitem\n"
        + "  WHERE\n"
        + "  L_PARTKEY IN (SELECT P_PARTKEY FROM Q17_PART)\n"
        + ")\n"
        + " SELECT CAST(SUM(L_EXTENDEDPRICE) / 7.0 AS DECIMAL(32,2)) AS AVG_YEARLY\n"
        + " FROM Q17_AVG, Q17_PRICE\n"
        + " WHERE\n"
        + " T_PARTKEY = L_PARTKEY AND L_QUANTITY < T_AVG_QUANTITY",
        "DruidOuterQueryRel(scanFilter=[<($2, $1)], agg#0=[SUM($4)], aggregateProject=[CAST(/($0, 7.0:DECIMAL(2, 1))):DECIMAL(19, 2)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
        + "    DruidOuterQueryRel(group=[{0}], agg#0=[AVG($1)], aggregateProject=[$0, *(0.2:DECIMAL(2, 1), $1)])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1])\n"
        + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$7, $8])\n"
        + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 1, 0])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$3, $7, $8])\n"
        + "      DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n",
        new Object[] {new BigDecimal(4923)}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "5WzbPrMHdyIsoHy0G3gKfg==",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0]}",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0]}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0]}, StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_QUANTITY, L_PARTKEY, L_EXTENDEDPRICE], $hash=true}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < p0)'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='CAST((a0 / 7.0B), 'decimal')', finalize=true}], outputColumns=[p0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0]}",
          "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}",
          "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_QUANTITY, L_PARTKEY, L_EXTENDEDPRICE], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "xeEsXsDHDaV+8dKapYjVEg==",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0]}, CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < p0)'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='CAST((a0 / 7.0B), 'decimal')', finalize=true}], outputColumns=[p0]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0]}",
          "StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}",
          "StreamQuery{dataSource='lineitem', columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}"
      );
    }
  }

  @Test
  public void tpch18() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH q18_tmp_cached AS (\n"
        + "SELECT\n"
        + "    L_ORDERKEY,\n"
        + " SUM(L_QUANTITY) AS T_SUM_QUANTITY\n"
        + " FROM\n"
        + "    lineitem\n"
        + " WHERE\n"
        + "    L_ORDERKEY IS NOT NULL\n"
        + " GROUP BY\n"
        + "    L_ORDERKEY\n"
        + ")\n"
        + "SELECT\n"
        + "    C_NAME,\n"
        + "    C_CUSTKEY,\n"
        + "    O_ORDERKEY,\n"
        + "    O_ORDERDATE,\n"
        + "    O_TOTALPRICE,\n"
        + " SUM(L_QUANTITY)\n"
        + " FROM\n"
        + "    customer,\n"
        + "    orders,\n"
        + "    q18_tmp_cached T,\n"
        + "    lineitem L\n"
        + " WHERE\n"
        + "    C_CUSTKEY = O_CUSTKEY\n"
        + " AND O_ORDERKEY = T.L_ORDERKEY\n"
        + " AND O_ORDERKEY IS NOT NULL\n"
        + " AND T.T_SUM_QUANTITY > 300\n"
        + " AND O_ORDERKEY = L.L_ORDERKEY\n"
        + " AND L.L_ORDERKEY IS NOT NULL\n"
        + " GROUP BY\n"
        + "    C_NAME,\n"
        + "    C_CUSTKEY,\n"
        + "    O_ORDERKEY,\n"
        + "    O_ORDERDATE,\n"
        + "    O_TOTALPRICE\n"
        + " ORDER BY\n"
        + "    O_TOTALPRICE DESC,\n"
        + "    O_ORDERDATE\n"
        + " LIMIT 100",
        "DruidOuterQueryRel(group=[{0, 1, 3, 4, 5}], EXPR$5=[SUM($9)], sort=[$4:DESC, $2:ASC], fetch=[100], sortProject=[$1, $0, $3, $2, $4, $5])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0])\n"
        + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $5])\n"
        + "        DruidQueryRel(table=[druid.orders], scanProject=[$2, $3, $4, $8])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[IS NOT NULL($6)], scanProject=[$6, $8], group=[{0}], T_SUM_QUANTITY=[SUM($1)], aggregateFilter=[>($1, 300)])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8])\n",
        new Object[]{"Customer#000000334", "334", "29158", "1995-10-21", 441562.47D, 305L},
        new Object[]{"Customer#000000089", "89", "6882", "1997-04-09", 389430.93D, 303L}
    );
    hook.verifyHooked(
        "4LuDV4IEAKNN/MwAWdcpeg==",
        "TimeseriesQuery{dataSource='lineitem', filter=!(L_ORDERKEY==NULL), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
        "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=!(L_ORDERKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0, a0], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='_d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='_d1'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='_d2'}, DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='_d3'}, DefaultDimensionSpec{dimension='O_TOTALPRICE', outputName='_d4'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='_a0', fieldName='L_QUANTITY', inputType='long'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_d4', direction=descending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=100}, outputColumns=[_d1, _d0, _d3, _d2, _d4, _a0]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}",
        "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}",
        "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=!(L_ORDERKEY==NULL), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0, a0], $hash=true}",
        "StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}"
    );
  }

  @Test
  public void tpch19() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "SELECT\n"
        + "    SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE\n"
        + " FROM\n"
        + "    lineitem,\n"
        + "    part\n"
        + " WHERE\n"
        + "    (\n"
        + "        P_PARTKEY = L_PARTKEY\n"
        + "        AND P_BRAND = 'Brand#32'\n"
        + "        AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
        + "        AND L_QUANTITY >= 7 AND L_QUANTITY <= 7 + 10\n"
        + "        AND P_SIZE BETWEEN 1 AND 5\n"
        + "        AND L_SHIPMODE IN ('AIR', 'AIR REG')\n"
        + "        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'\n"
        + "    )\n"
        + "    OR\n"
        + "    (\n"
        + "        P_PARTKEY = L_PARTKEY\n"
        + "        AND P_BRAND = 'Brand#35'\n"
        + "        AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
        + "        AND L_QUANTITY >= 15 AND L_QUANTITY <= 15 + 10\n"
        + "        AND P_SIZE BETWEEN 1 AND 10\n"
        + "        AND L_SHIPMODE IN ('AIR', 'AIR REG')\n"
        + "        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'\n"
        + "    )\n"
        + "    OR\n"
        + "    (\n"
        + "        P_PARTKEY = L_PARTKEY\n"
        + "        AND P_BRAND = 'Brand#24'\n"
        + "        AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
        + "        AND L_QUANTITY >= 26 AND L_QUANTITY <= 26 + 10\n"
        + "        AND P_SIZE BETWEEN 1 AND 15\n"
        + "        AND L_SHIPMODE IN ('AIR', 'AIR REG')\n"
        + "        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'\n"
        + "    )",
        "DruidOuterQueryRel(scanFilter=[AND(>=($9, 1), OR(=($5, 'AIR'), =($5, 'AIR REG')), =($4, 'DELIVER IN PERSON'), OR(AND(=($6, 'Brand#32'), OR(=($7, 'SM CASE'), =($7, 'SM BOX'), =($7, 'SM PACK'), =($7, 'SM PKG')), >=($3, 7), <=($3, 17), <=($9, 5)), AND(=($6, 'Brand#35'), OR(=($7, 'MED BAG'), =($7, 'MED BOX'), =($7, 'MED PKG'), =($7, 'MED PACK')), >=($3, 15), <=($3, 25), <=($9, 10)), AND(=($6, 'Brand#24'), OR(=($7, 'LG CASE'), =($7, 'LG BOX'), =($7, 'LG PACK'), =($7, 'LG PKG')), >=($3, 26), <=($3, 36), <=($9, 15))))], scanProject=[*($1, -(1, $0))], REVENUE=[SUM($0)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[2])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanProject=[$2, $3, $7, $8, $12, $13])\n"
        + "    DruidQueryRel(table=[druid.part], scanProject=[$0, $2, $5, $7])\n"
    );
    hook.verifyHooked(
        "zXp9aWvgDdRZRZV1BeyTMw==",
        "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY, L_SHIPINSTRUCT, L_SHIPMODE]}, StreamQuery{dataSource='part', columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}], timeColumnName=__time}', filter=(BoundDimFilter{1 <= P_SIZE(numeric)} && InDimFilter{dimension='L_SHIPMODE', values=[AIR, AIR REG]} && L_SHIPINSTRUCT=='DELIVER IN PERSON' && ((P_BRAND=='Brand#32' && InDimFilter{dimension='P_CONTAINER', values=[SM BOX, SM CASE, SM PACK, SM PKG]} && BoundDimFilter{P_SIZE <= 5(numeric)} && BoundDimFilter{7 <= L_QUANTITY <= 17(numeric)}) || (P_BRAND=='Brand#35' && InDimFilter{dimension='P_CONTAINER', values=[MED BAG, MED BOX, MED PACK, MED PKG]} && BoundDimFilter{P_SIZE <= 10(numeric)} && BoundDimFilter{15 <= L_QUANTITY <= 25(numeric)}) || (P_BRAND=='Brand#24' && InDimFilter{dimension='P_CONTAINER', values=[LG BOX, LG CASE, LG PACK, LG PKG]} && BoundDimFilter{P_SIZE <= 15(numeric)} && BoundDimFilter{26 <= L_QUANTITY <= 36(numeric)}))), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}",
        "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY, L_SHIPINSTRUCT, L_SHIPMODE]}",
        "StreamQuery{dataSource='part', columns=[P_BRAND, P_CONTAINER, P_PARTKEY, P_SIZE], $hash=true}"
    );
  }

  @Test
  public void tpch20() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH TMP1 AS (\n"
        + "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'\n"
        + "),\n"
        + "TMP2 AS (\n"
        + "SELECT S_NAME, S_ADDRESS, S_SUPPKEY\n"
        + " FROM supplier, nation\n"
        + " WHERE S_NATIONKEY = N_NATIONKEY\n"
        + " AND N_NAME = 'RUSSIA'\n"    // changed 'CANADA' to 'RUSSIA'
        + "),\n"
        + "TMP3 AS (\n"
        + "SELECT L_PARTKEY, 0.5 * SUM(L_QUANTITY) AS SUM_QUANTITY, L_SUPPKEY\n"
        + " FROM lineitem, TMP2\n"
        + " WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE <= '1995-01-01'\n"
        + " AND L_SUPPKEY = S_SUPPKEY\n"
        + " GROUP BY L_PARTKEY, L_SUPPKEY\n"
        + "),\n"
        + "TMP4 AS (\n"
        + "SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY\n"
        + " FROM partsupp\n"
        + " WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM TMP1)\n"
        + "),\n"
        + "TMP5 AS (\n"
        + "SELECT\n"
        + "    PS_SUPPKEY\n"
        + " FROM\n"
        + "    TMP4, TMP3\n"
        + " WHERE\n"
        + "    PS_PARTKEY = L_PARTKEY\n"
        + " AND PS_SUPPKEY = L_SUPPKEY\n"
        + " AND PS_AVAILQTY > SUM_QUANTITY\n"
        + ")\n"
        + "SELECT\n"
        + "    S_NAME,\n"
        + "    S_ADDRESS\n"
        + " FROM\n"
        + "    supplier\n"
        + " WHERE\n"
        + "    S_SUPPKEY IN (SELECT PS_SUPPKEY FROM TMP5)\n"
        + " ORDER BY S_NAME",
        "DruidOuterQueryRel(sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
        + "    DruidQueryRel(table=[druid.supplier], scanProject=[$1, $3, $6])\n"
        + "    DruidOuterQueryRel(scanFilter=[>($1, $2)], group=[{0}])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0, 1], rightKeys=[0, 2], outputColumns=[1, 2, 4])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[1, 2, 0])\n"
        + "          DruidQueryRel(table=[druid.partsupp], scanProject=[$0, $2, $3])\n"
        + "          DruidQueryRel(table=[druid.part], scanFilter=[LIKE($4, 'forest%')], scanProject=[$5], group=[{0}])\n"
        + "        DruidOuterQueryRel(group=[{0, 1}], agg#0=[SUM($2)], aggregateProject=[$0, *(0.5:DECIMAL(2, 1), $2), $1])\n"
        + "          DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 3, 1])\n"
        + "            DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1994-01-01'), <=($11, '1995-01-01'))], scanProject=[$7, $8, $11, $14])\n"
        + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[1])\n"
        + "              DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
        + "              DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'RUSSIA')], scanProject=[$1, $2])\n",
        new Object[] {"Supplier#000000025", "RCQKONXMFnrodzz6w7fObFVV6CUm2q"}
    );
    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "0pqbTcE8L3Wth/tUNnBsEg==",
            "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[22]}, columns=[S_SUPPKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}",
            "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "0pqbTcE8L3Wth/tUNnBsEg==",
              "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
              "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0]}",
              "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY]}",
              "StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[22]}, columns=[S_SUPPKEY]}",
              "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
              "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
              "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}",
              "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "0pqbTcE8L3Wth/tUNnBsEg==",
              "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
              "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0]}",
              "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY]}",
              "StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[22]}, columns=[S_SUPPKEY]}",
              "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
              "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
              "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}",
              "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}, columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
          );
        }
      }
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "pYfYQXiTzb0wk5FrWDS0zQ==",
            "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
            "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}",
            "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "Y4piWzMABIkf9XMihiF+xA==",
            "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
            "StreamQuery{dataSource='partsupp', columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SHIPDATE, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch21() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH LOCATION AS (\n"
        + " SELECT supplier.* FROM supplier, nation WHERE\n"
        + " S_NATIONKEY = N_NATIONKEY AND N_NAME = 'UNITED STATES'\n"   // changed 'SAUDI ARABIA' to 'UNITED STATES'
        + "),\n"
        + "L3 AS (\n"
        + "SELECT L_ORDERKEY, COUNT(DISTINCT L_SUPPKEY) AS CNTSUPP\n"
        + " FROM lineitem\n"
        + " WHERE L_RECEIPTDATE > L_COMMITDATE AND L_ORDERKEY IS NOT NULL\n"
        + " GROUP BY L_ORDERKEY\n"
        + " HAVING CNTSUPP = 1\n"
        + ")\n"
        + "SELECT S_NAME, COUNT(*) AS NUMWAIT FROM\n"
        + "(\n"
        + " SELECT LI.L_SUPPKEY, LI.L_ORDERKEY\n"
        + " FROM lineitem LI JOIN orders O ON LI.L_ORDERKEY = O.O_ORDERKEY AND O.O_ORDERSTATUS = 'F'\n"
        + "     JOIN\n"
        + "     (\n"
        + "       SELECT L_ORDERKEY, COUNT(DISTINCT L_SUPPKEY) AS CNTSUPP\n"
        + "       FROM lineitem\n"
        + "       GROUP BY L_ORDERKEY\n"
        + "     ) L2 ON LI.L_ORDERKEY = L2.L_ORDERKEY AND\n"
        + "             LI.L_RECEIPTDATE > LI.L_COMMITDATE AND\n"
        + "             L2.CNTSUPP > 1\n"
        + ") L1 JOIN L3 ON L1.L_ORDERKEY = L3.L_ORDERKEY\n"
        + " JOIN LOCATION S ON L1.L_SUPPKEY = S.S_SUPPKEY\n"
        + " GROUP BY S_NAME\n"
        + " ORDER BY NUMWAIT DESC, S_NAME\n"
        + " LIMIT 100",
        "DruidOuterQueryRel(group=[{2}], NUMWAIT=[COUNT()], sort=[$1:DESC, $0:ASC], fetch=[100])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[5, 3])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
        + "          DruidQueryRel(table=[druid.orders], scanFilter=[=($6, 'F')], scanProject=[$4, $6])\n"
        + "          DruidQueryRel(table=[druid.lineitem], scanFilter=[>($9, $1)], scanProject=[$1, $6, $9, $14])\n"
        + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $14], group=[{0}], CNTSUPP=[COUNT(DISTINCT $1)], aggregateFilter=[>($1, 1)])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[1], outputColumns=[0, 2])\n"
        + "        DruidQueryRel(table=[druid.supplier], scanProject=[$3, $4, $6])\n"
        + "        DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'UNITED STATES')], scanProject=[$1, $2])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>($9, $1), IS NOT NULL($6))], scanProject=[$6, $14], group=[{0}], CNTSUPP=[COUNT(DISTINCT $1)], aggregateFilter=[=($1, 1)])\n",
        new Object[]{"Supplier#000000010", 15L},
        new Object[]{"Supplier#000000019", 15L},
        new Object[]{"Supplier#000000046", 15L},
        new Object[]{"Supplier#000000049", 5L}
    );
    if (semiJoin) {
      if (bloomFilter) {
        hook.verifyHooked(
            "ISYrKSd+shAh7DIQCR9haw==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY]}",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=4741}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}', filter=InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10048, ..6432 more]}, columns=[L_SUPPKEY, L_ORDERKEY]}, StreamQuery{dataSource='supplier', filter=S_NATIONKEY=='24', columns=[S_NAME, S_SUPPKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='_d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=100}, outputColumns=[_d0, _a0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=4741}]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}', filter=InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10048, ..6432 more]}, columns=[L_SUPPKEY, L_ORDERKEY]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', filter=S_NATIONKEY=='24', columns=[S_NAME, S_SUPPKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}"
        );
      } else {
        hook.verifyHooked(
            "z8CVLBM/vyy2W4YdcV6pFg==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY]}",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}', filter=InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10048, ..6432 more]}, columns=[L_SUPPKEY, L_ORDERKEY]}, StreamQuery{dataSource='supplier', filter=S_NATIONKEY=='24', columns=[S_NAME, S_SUPPKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='_d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=100}, outputColumns=[_d0, _a0]}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}', filter=InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10048, ..6432 more]}, columns=[L_SUPPKEY, L_ORDERKEY]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', filter=S_NATIONKEY=='24', columns=[S_NAME, S_SUPPKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}"
        );
      }
    } else if (broadcastJoin) {
      if (bloomFilter) {
        hook.verifyHooked(
            "dFblkfKINnsjlVFOvlR9SA==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY]}",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=4741}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}, $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='_d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=100}, outputColumns=[_d0, _a0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=4741}]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}, $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}"
        );
      } else {
        hook.verifyHooked(
            "Jr/AWY0GR0ckBDvjD8/2nA==",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY]}",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}, $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='_d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=100}, outputColumns=[_d0, _a0]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0], $hash=true}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string}}, $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0], $hash=true}"
        );
      }
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "9wthVWjsfJ9e1qoTo22xoA==",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=4741}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0]}], timeColumnName=__time}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='_d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=100}, outputColumns=[_d0, _a0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=4741}]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0]}"
        );
      } else {
        hook.verifyHooked(
            "4agEMHc2HJcvMSKV0Zb6bw==",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}, StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0], $hash=true}], timeColumnName=__time}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='_d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}], limit=100}, outputColumns=[_d0, _a0]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY, O_ORDERSTATUS], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_COMMITDATE, L_ORDERKEY, L_RECEIPTDATE, L_SUPPKEY]}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0, a0], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0, a0], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch22() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_JOIN_ENABLED,
        "WITH q22_customer_tmp_cached AS (\n"
        + " SELECT\n"
        + "    C_ACCTBAL,\n"
        + "    C_CUSTKEY,\n"
        + "    SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE\n"
        + " FROM\n"
        + "    customer\n"
        + " WHERE\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '13' OR\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '31' OR\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '23' OR\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '29' OR\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '30' OR\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '18' OR\n"
        + "    SUBSTR(C_PHONE, 1, 2) = '17'\n"
        + "),\n"
        + "q22_customer_tmp1_cached AS (\n"
        + " SELECT\n"
        + "    AVG(C_ACCTBAL) AS AVG_ACCTBAL\n"
        + " FROM\n"
        + "    q22_customer_tmp_cached\n"
        + " WHERE\n"
        + "    C_ACCTBAL > 0.00\n"
        + "),\n"
        + "q22_orders_tmp_cached AS (\n"
        + " SELECT\n"
        + "    O_CUSTKEY\n"
        + " FROM\n"
        + "    orders\n"
        + " GROUP BY\n"
        + "    O_CUSTKEY\n"
        + ")\n"
        + "SELECT\n"
        + "    CNTRYCODE,\n"
        + "    COUNT(1) AS NUMCUST,\n"
        + "    SUM(C_ACCTBAL) AS TOTACCTBAL\n"
        + " FROM (\n"
        + "    SELECT\n"
        + "        CNTRYCODE,\n"
        + "        C_ACCTBAL,\n"
        + "        AVG_ACCTBAL\n"
        + "    FROM\n"
        + "        q22_customer_tmp1_cached CT1 CROSS JOIN (\n"
        + "            SELECT\n"
        + "                CNTRYCODE,\n"
        + "                C_ACCTBAL\n"
        + "            FROM\n"
        + "                q22_orders_tmp_cached OT\n"
        + "                RIGHT OUTER JOIN q22_customer_tmp_cached CT\n"
        + "                ON CT.C_CUSTKEY = OT.O_CUSTKEY\n"
        + "            WHERE\n"
        + "                O_CUSTKEY IS NULL\n"
        + "        ) CT2\n"
        + ") A\n"
        + " WHERE\n"
        + "    C_ACCTBAL > AVG_ACCTBAL\n"
        + " GROUP BY\n"
        + "    CNTRYCODE\n"
        + " ORDER BY\n"
        + "    CNTRYCODE",
        "DruidOuterQueryRel(scanFilter=[>($1, $2)], group=[{0}], NUMCUST=[COUNT()], TOTACCTBAL=[SUM($1)], sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], outputColumns=[1, 2, 0])\n"
        + "    DruidQueryRel(table=[druid.customer], scanFilter=[AND(OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17')), >($0, 0.00:DECIMAL(3, 2)))], scanProject=[$0], AVG_ACCTBAL=[AVG($0)])\n"
        + "    DruidOuterQueryRel(scanFilter=[IS NULL($0)], scanProject=[$3, $1])\n"
        + "      DruidJoinRel(joinType=[RIGHT], leftKeys=[0], rightKeys=[1])\n"
        + "        DruidQueryRel(table=[druid.orders], scanProject=[$2], group=[{0}])\n"
        + "        DruidQueryRel(table=[druid.customer], scanFilter=[OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17'))], scanProject=[$0, $3, SUBSTR($7, 1, 2)])\n",
        new Object[]{"13", 5L, 37676.7D},
        new Object[]{"17", 5L, 41431.74D},
        new Object[]{"18", 7L, 51351.03D},
        new Object[]{"23", 2L, 18148.870000000003D},
        new Object[]{"29", 6L, 47247.25D},
        new Object[]{"30", 9L, 65584.23D},
        new Object[]{"31", 7L, 53270.52999999999D}
    );

    if (bloomFilter) {
      hook.verifyHooked(
          "AAOIcfCeVrRdKtKMHlp4ig==",
          "TimeseriesQuery{dataSource='orders', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='CommonJoin{queries=[TimeseriesQuery{dataSource='customer', filter=(InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]} && BoundDimFilter{0.00 < C_ACCTBAL(numeric)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}, StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=BloomDimFilter.Factory{bloomSource=$view:customer[C_CUSTKEY]([ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}])(InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}), fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], groupingSets=Noop, maxNumEntries=217}, outputColumns=[d0], $hash=true}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, v0], virtualColumns=[ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}', filter=d0==NULL, columns=[v0, C_ACCTBAL]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='v0', outputName='d0'}], filter=MathExprFilter{expression='(C_ACCTBAL > a0)'}, aggregatorSpecs=[CountAggregatorFactory{name='_a0'}, GenericSumAggregatorFactory{name='_a1', fieldName='C_ACCTBAL', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, _a0, _a1]}",
          "TimeseriesQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=217}]}",
          "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], groupingSets=Noop}, outputColumns=[d0], $hash=true}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, v0], virtualColumns=[ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}', filter=d0==NULL, columns=[v0, C_ACCTBAL]}",
          "GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], groupingSets=Noop}, outputColumns=[d0], $hash=true}",
          "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, v0], virtualColumns=[ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
          "TimeseriesQuery{dataSource='customer', filter=(InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]} && BoundDimFilter{0.00 < C_ACCTBAL(numeric)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}"
      );
    } else {
      hook.verifyHooked(
          "wAZjgv21igel4f07UCwROg==",
          "TimeseriesQuery{dataSource='orders', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='CommonJoin{queries=[TimeseriesQuery{dataSource='customer', filter=(InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]} && BoundDimFilter{0.00 < C_ACCTBAL(numeric)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}, StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], outputColumns=[d0], $hash=true}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, v0], virtualColumns=[ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}', filter=d0==NULL, columns=[v0, C_ACCTBAL]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='v0', outputName='d0'}], filter=MathExprFilter{expression='(C_ACCTBAL > a0)'}, aggregatorSpecs=[CountAggregatorFactory{name='_a0'}, GenericSumAggregatorFactory{name='_a1', fieldName='C_ACCTBAL', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, _a0, _a1]}",
          "StreamQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], outputColumns=[d0], $hash=true}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, v0], virtualColumns=[ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}], timeColumnName=__time}', filter=d0==NULL, columns=[v0, C_ACCTBAL]}",
          "GroupByQuery{dataSource='orders', dimensions=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='d0'}], outputColumns=[d0], $hash=true}",
          "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]}, columns=[C_ACCTBAL, C_CUSTKEY, v0], virtualColumns=[ExprVirtualColumn{expression='substring(C_PHONE, 0, 2)', outputName='v0'}], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
          "TimeseriesQuery{dataSource='customer', filter=(InDimFilter{dimension='C_PHONE', extractionFn=SubstringDimExtractionFn{index=0, end=2}, values=[13, 17, 18, 23, 29, 30, 31]} && BoundDimFilter{0.00 < C_ACCTBAL(numeric)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='C_ACCTBAL', inputType='double'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}], outputColumns=[a0], $hash=true}"
      );
    }
  }
}
