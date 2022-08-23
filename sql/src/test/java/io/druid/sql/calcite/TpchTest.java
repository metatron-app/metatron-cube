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

  @Test
  public void tpch1() throws Exception
  {
    testQuery(
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
        + "    L_LINESTATUS"
        ,
        "DruidQueryRel(table=[druid.lineitem], "
        + "scanFilter=[<=($11, '1998-09-16')], "
        + "scanProject=[$10, $5, $8, $3, *($3, -(1, $2)), *(*($3, -(1, $2)), +(1, $15)), $2], "
        + "group=[{0, 1}], SUM_QTY=[SUM($2)], SUM_BASE_PRICE=[SUM($3)], SUM_DISC_PRICE=[SUM($4)], SUM_CHARGE=[SUM($5)], AVG_QTY=[AVG($2)], AVG_PRICE=[AVG($3)], AVG_DISC=[AVG($6)], COUNT_ORDER=[COUNT()], "
        + "sort=[$0:ASC, $1:ASC])\n"
        ,
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
        new Object[]{"A", "F", 189203L, 2.649171512299999E8D, 2.517225664892047E8D, 2.6181376653715158E8D, 25L, 35407.26426490242D, 0.05014435F, 7482L},
        new Object[]{"N", "F", 4654L, 6647990.519999999D, 6333568.498901443D, 6584905.203860926D, 26L, 37139.61184357541D, 0.04849162F, 179L},
        new Object[]{"N", "O", 376815L, 5.2792684456999964E8D, 5.016250498442685E8D, 5.216476002629087E8D, 25L, 35840.247424983005D, 0.04984861F, 14730L},
        new Object[]{"R", "F", 191214L, 2.6792430413999987E8D, 2.5454761771048513E8D, 2.648043625528718E8D, 25L, 35972.65093179375D, 0.04983217F, 7448L}
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

  public static final Object[][] TPCH2_RESULT = {
      {6820.35, "Supplier#000000007", "UNITED KINGDOM", "560", "Manufacturer#2", "s,4TicNGB4uO6PaSqNBUq", "33-990-965-2201", "s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit"},
      {3556.47, "Supplier#000000032", "UNITED KINGDOM", "381", "Manufacturer#5", "yvoD3TtZSx1skQNCK8agk5bZlZLug", "33-484-637-7873", "usly even depths. quickly ironic theodolites s"},
      {2972.26, "Supplier#000000016", "RUSSIA", "396", "Manufacturer#3", "YjP5C55zHDXL7LalK27zfQnwejdpin4AMpvh", "32-822-502-4215", "ously express ideas haggle quickly dugouts? fu"}
  };

  @Test
  public void tpch2() throws Exception
  {
    testQuery(
        TPCH2,
        TPCH2_EXPLAIN,
        TPCH2_RESULT
    );

    if (broadcastJoin) {
      hook.verifyHooked(
          "zXh8Snd3G9L+YpAe4R+avw==",
          "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
          "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY]}",
          "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
          "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY]}",
          "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY]}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY]}",
          "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
          "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}], $hash=true}, GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}]}",
          "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomFilter{fieldNames=[PS_PARTKEY], groupingSets=Noop}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp, leftJoinColumns=[PS_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_MFGR:dimension.string, P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part, leftJoinColumns=[PS_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_ACCTBAL:double, S_ADDRESS:dimension.string, S_COMMENT:string, S_NAME:dimension.string, S_NATIONKEY:dimension.string, S_PHONE:dimension.string, S_SUPPKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NAME:dimension.string, N_NATIONKEY:dimension.string, N_REGIONKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=partsupp+part+supplier+nation, leftJoinColumns=[N_REGIONKEY], rightAlias=region, rightJoinColumns=[R_REGIONKEY]}, hashLeft=false, hashSignature={R_REGIONKEY:dimension.string}}], $hash=true}"
      );
    } else {
      if (semiJoin) {
        hook.verifyHooked(
            "GnTJH8kgt4PleKt/06fJAQ==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY]}",
            "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[381, 396, 501, 560]} && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[381, 396, 501, 560]} && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && InDimFilter{dimension='PS_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
            "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='3', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "m/MgiGXRk+fyYGd4lcImhg==",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((P_SIZE=='37' && P_TYPE LIKE '%COPPER')), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=4}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomDimFilter.Factory{bloomSource=$view:region[R_REGIONKEY](R_NAME=='EUROPE'), fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
              "TimeseriesQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER'), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=4}]}",
              "TimeseriesQuery{dataSource='region', filter=R_NAME=='EUROPE', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[R_REGIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "StreamQuery{dataSource='partsupp', filter=(!(PS_PARTKEY==NULL) && BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}",
              "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "msFru0pWvekijICRPnRg8A==",
              "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='EUROPE', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericMinAggregatorFactory{name='a0', fieldName='PS_SUPPLYCOST', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}', columns=[S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT], orderingSpecs=[OrderByColumnSpec{dimension='S_ACCTBAL', direction=descending}, OrderByColumnSpec{dimension='N_NAME', direction=ascending}, OrderByColumnSpec{dimension='S_NAME', direction=ascending}, OrderByColumnSpec{dimension='P_PARTKEY', direction=ascending}], limitSpec=LimitSpec{columns=[], limit=100}}",
              "StreamQuery{dataSource='partsupp', filter=!(PS_PARTKEY==NULL), columns=[PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST]}",
              "StreamQuery{dataSource='part', filter=(P_SIZE=='37' && P_TYPE LIKE '%COPPER' && !(P_PARTKEY==NULL)), columns=[P_MFGR, P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_NATIONKEY, S_PHONE, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
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

  @Test
  public void tpch3() throws Exception
  {
    testQuery(
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
        + " LIMIT 10"
        ,
        "DruidOuterQueryRel(scanProject=[$0, $1, $2, *($3, -(1, $4))], group=[{0, 1, 2}], revenue=[SUM($3)], sort=[$3:DESC, $1:ASC], fetch=[10], sortProject=[$0, $3, $1, $2])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[2], outputColumns=[5, 0, 2, 4, 3])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[2, 3, 4])\n"
        + "      DruidQueryRel(table=[druid.customer], scanFilter=[=($4, 'BUILDING')], scanProject=[$3])\n"
        + "      DruidQueryRel(table=[druid.orders], scanFilter=[<($3, '1995-03-22')], scanProject=[$2, $3, $4, $7])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[>($11, '1995-03-22')], scanProject=[$2, $3, $6])\n"
        ,
        new Object[]{"26304", 358077.0173104012D, "1995-03-20", 0L},
        new Object[]{"928", 289800.9609201348D, "1995-03-02", 0L},
        new Object[]{"4327", 187634.62951172946D, "1995-03-16", 0L},
        new Object[]{"20453", 176905.62356464745D, "1995-03-11", 0L},
        new Object[]{"20486", 171516.90474127472D, "1995-03-06", 0L},
        new Object[]{"18820", 163812.80363306048D, "1995-02-12", 0L},
        new Object[]{"16096", 147838.64354118108D, "1995-01-20", 0L},
        new Object[]{"3749", 135109.4343535584D, "1995-02-24", 0L},
        new Object[]{"19365", 126378.68811139763D, "1995-01-17", 0L},
        new Object[]{"6560", 123264.19084395052D, "1995-01-05", 0L}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "Blgjz6MecQd7jixTMdq7cA==",
          "StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY]}",
          "StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 102, 103, 108, 109, 11, 113, 116, 121, 123, ..145 more]}), columns=[O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[orders]}, StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)} && InDimFilter{dimension='L_ORDERKEY', values=[10016, 10144, 10146, 10149, 10150, 10182, 1030, 10305, 10310, 10433, ..740 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
          "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)} && InDimFilter{dimension='L_ORDERKEY', values=[10016, 10144, 10146, 10149, 10150, 10182, 1030, 10305, 10310, 10433, ..740 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "4tO32OBYQYlo2pxvhb51Xg==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=(C_MKTSEGMENT=='BUILDING' && BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=3669}), columns=[C_CUSTKEY], $hash=true}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)} && BloomDimFilter.Factory{bloomSource=$view:customer[C_CUSTKEY](C_MKTSEGMENT=='BUILDING'), fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='O_CUSTKEY'}], groupingSets=Noop, maxNumEntries=155} && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=16164}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=3669}]}",
            "TimeseriesQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[C_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=155}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=16164}]}",
            "StreamQuery{dataSource='customer', filter=(C_MKTSEGMENT=='BUILDING' && BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}), columns=[C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=(BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)} && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_CUSTKEY', outputName='O_CUSTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
        );
      } else {
        hook.verifyHooked(
            "SB0aHgW0L8FZNENMHI06iA==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d1'}, DefaultDimensionSpec{dimension='O_SHIPPRIORITY', outputName='d2'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d1', direction=ascending}], limit=10}, outputColumns=[d0, a0, d1, d2]}",
            "StreamQuery{dataSource='customer', filter=C_MKTSEGMENT=='BUILDING', columns=[C_CUSTKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{O_ORDERDATE < 1995-03-22(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_SHIPPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-03-22 < L_SHIPDATE(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
        );
      }
    }
  }

  @Test
  public void tpch4() throws Exception
  {
    testQuery(
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
        + " ORDER BY O_ORDERPRIORITY"
        ,
        "DruidOuterQueryRel(group=[{0}], order_count=[COUNT()], sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
        + "    DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1996-05-01'), <($3, '1996-08-01'), IS NOT NULL($4))], scanProject=[$4, $5])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(<($1, $9), IS NOT NULL($6))], scanProject=[$6], group=[{0}])\n"
        ,
        new Object[]{"1-URGENT", 53L},
        new Object[]{"2-HIGH", 40L},
        new Object[]{"3-MEDIUM", 50L},
        new Object[]{"4-NOT SPECIFIED", 59L},
        new Object[]{"5-LOW", 53L}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "F5KZc6xhFuvp6J/P3NqWCg==",
          "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)} && InDimFilter{dimension='O_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..6896 more]}), columns=[O_ORDERPRIORITY]}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)} && InDimFilter{dimension='O_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..6896 more]}), columns=[O_ORDERPRIORITY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "MOFDpdFXrMDqW1OtUCQoiA==",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL) && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, maxNumEntries=287}), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=287}]}",
            "StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL) && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop}), outputColumns=[d0]}"
        );
      } else {
        hook.verifyHooked(
            "dZJbz52sNQSTloJ3R0VmRA==",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='O_ORDERPRIORITY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='orders', filter=(!(O_ORDERKEY==NULL) && BoundDimFilter{1996-05-01 <= O_ORDERDATE < 1996-08-01(lexicographic)}), columns=[O_ORDERKEY, O_ORDERPRIORITY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && !(L_ORDERKEY==NULL)), outputColumns=[d0]}"
        );
      }
    }
  }

  @Test
  public void tpch5() throws Exception
  {
    testQuery(
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
        + "    REVENUE DESC"
        ,
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
        + "    DruidQueryRel(table=[druid.region], scanFilter=[=($1, 'AFRICA')], scanProject=[$2])\n"
        ,
        new Object[]{"KENYA", 523154.4756433582D},
        new Object[]{"MOROCCO", 218260.09130795716D},
        new Object[]{"ETHIOPIA", 167163.611216833D},
        new Object[]{"ALGERIA", 157068.92507587554D},
        new Object[]{"MOZAMBIQUE", 151814.85730328262D}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "UKWNwelipQwZuHyO+mfj1Q==",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[customer]}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERKEY]}",
          "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
          "StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "K8tmjEl9gqzG2I7YRhIG+g==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=1129}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomDimFilter.Factory{bloomSource=$view:region[R_REGIONKEY](R_NAME=='AFRICA'), fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=1129}]}",
            "TimeseriesQuery{dataSource='region', filter=R_NAME=='AFRICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[R_REGIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
            "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "/c1wBhPC3oDuwcQk4QJTyg==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=-1}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-01-01 <= O_ORDERDATE < 1994-01-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY]}",
            "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AFRICA', columns=[R_REGIONKEY], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch6() throws Exception
  {
    testQuery(
        "SELECT" 
        + "    SUM(L_EXTENDEDPRICE * L_DISCOUNT) as REVENUE"
        + " FROM" 
        + "    lineitem"
        + " WHERE"
        + "    L_SHIPDATE >= '1993-01-01' AND" 
        + "    L_SHIPDATE < '1994-01-01' AND" 
        + "    L_DISCOUNT BETWEEN 0.06 - 0.01 and 0.06 + 0.01 AND"
        + "    L_QUANTITY < 25"
        ,
        "DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1993-01-01'), <($11, '1994-01-01'), >=($2, 0.05:DECIMAL(4, 2)), <=($2, 0.07:DECIMAL(4, 2)), <($8, 25))], scanProject=[*($3, $2)], REVENUE=[SUM($0)])\n"
        ,
        new Object[]{419743.2357189747}
    );
    hook.verifyHooked(
        "N7buYl+k+PqT6VGz3Uj3/g==",
        "TimeseriesQuery{dataSource='lineitem', filter=(BoundDimFilter{L_QUANTITY < 25(numeric)} && BoundDimFilter{0.05 <= L_DISCOUNT <= 0.07(numeric)} && BoundDimFilter{1993-01-01 <= L_SHIPDATE < 1994-01-01(lexicographic)}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * L_DISCOUNT)', inputType='double'}], outputColumns=[a0]}"
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

  public static final Object[][] TPCH7_RESULT = {
      {"KENYA", "PERU", 1995L, 155808.41826545357D},
      {"KENYA", "PERU", 1996L, 335577.4780717748D},
      {"PERU", "KENYA", 1995L, 243818.1961560595D},
      {"PERU", "KENYA", 1996L, 105976.7656660986D}
  };

  @Test
  public void tpch7() throws Exception
  {
    testQuery(
        TPCH7,
        TPCH7_EXPLAIN,
        TPCH7_RESULT
    );
    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "CENyMyekGZPltTml/ws9rg==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, MaterializedQuery{dataSource=[orders]}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
            "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "WV34K7RtVFdaql+iURSmrQ==",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
            "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[supplier]}, StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[orders]}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && InDimFilter{dimension='L_ORDERKEY', values=[1, 100, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, ..7490 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
            "StreamQuery{dataSource='customer', filter=InDimFilter{dimension='C_NATIONKEY', values=[14, 17]}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      }
    } else {
      if (broadcastJoin) {
        if (bloomFilter) {
          hook.verifyHooked(
              "38WEsrQxlJE2AWMVgiLJbA==",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}), fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop, maxNumEntries=2}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "71zZkEx308ySWN5lSaliFQ==",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_SUPPKEY], rightAlias=lineitem, rightJoinColumns=[L_SUPPKEY]}, hashLeft=true, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "l8tSjawpHgH9eovMSU1FXQ==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}), fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop, maxNumEntries=2}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:nation[N_NATIONKEY](InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}), fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop, maxNumEntries=2}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "TimeseriesQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "TimeseriesQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[N_NATIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=2}]}",
              "StreamQuery{dataSource='supplier', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='S_NATIONKEY', outputName='S_NATIONKEY'}], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_NATIONKEY', outputName='C_NATIONKEY'}], groupingSets=Noop}, columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "6cwnnzZucdJZXL1mFcsp4Q==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}, StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}], timeColumnName=__time}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=InDimFilter{dimension='N_NAME', values=[KENYA, PERU]}, columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='N_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='N_NAME0', outputName='d1'}, DefaultDimensionSpec{dimension='d2:v', outputName='d2'}], filter=(InDimFilter{dimension='N_NAME', values=[KENYA, PERU]} && (N_NAME0=='PERU' || N_NAME=='PERU') && (N_NAME=='KENYA' || N_NAME0=='KENYA') && InDimFilter{dimension='N_NAME0', values=[KENYA, PERU]}), virtualColumns=[ExprVirtualColumn{expression='YEAR(L_SHIPDATE)', outputName='d2:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}, OrderByColumnSpec{dimension='d1', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=-1}, outputColumns=[d0, d1, d2, a0]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-01-01 <= L_SHIPDATE <= 1996-12-31(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_SHIPDATE, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERKEY]}",
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

  public static final Object[][] TPCH8_RESULT = {
      {1995L, 0.15367145767949628D},
      {1996L, 0.3838133760159879D}
  };

  @Test
  public void tpch8() throws Exception
  {
    testQuery(
        TPCH8,
        TPCH8_EXPLAIN,
        TPCH8_RESULT
    );

    if (semiJoin) {
      if (broadcastJoin) {
        hook.verifyHooked(
            "bSZ5lohsrxYouMw6jFvapg==",
            "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}",
            "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='O_ORDERKEY', values=[10114, 10242, 10245, 103, 10310, 10338, 10498, 10658, 10851, 11040, ..199 more]} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[customer]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
            "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='O_ORDERKEY', values=[10114, 10242, 10245, 103, 10310, 10338, 10498, 10658, 10851, 11040, ..199 more]} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
            "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "a0+blep4sjdROwQkXbnJ7g==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[customer]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "JiefeXPTfdDm6HJEIGWoAA==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, MaterializedQuery{dataSource=[supplier]}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[customer]}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_PARTKEY', values=[215, 345, 349, 51, 53, 666, 722]} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='orders', filter=(BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)} && InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='nation', filter=N_REGIONKEY=='1', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
          );
        }
      }
    } else {
      if (broadcastJoin) {
        if (bloomFilter) {
          hook.verifyHooked(
              "xyaWJHVrwKboQ1m5qDTXjw==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomDimFilter.Factory{bloomSource=$view:region[R_REGIONKEY](R_NAME=='AMERICA'), fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "TimeseriesQuery{dataSource='region', filter=R_NAME=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[R_REGIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "JiFmWkHCfrJdBfCvbrD1hw==",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY]}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "StreamQuery{dataSource='lineitem', filter=BloomFilter{fieldNames=[L_PARTKEY], groupingSets=Noop}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY], localPostProcessing=ListPostProcessingOperator[BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_PARTKEY], rightAlias=part, rightJoinColumns=[P_PARTKEY]}, hashLeft=false, hashSignature={P_PARTKEY:dimension.string}}, BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem+part, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_NATIONKEY:dimension.string, S_SUPPKEY:dimension.string}}], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        }
      } else {
        if (bloomFilter) {
          hook.verifyHooked(
              "/DL9G2M2STTWTAa1yUzGCg==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY](P_TYPE=='ECONOMY BURNISHED NICKEL'), fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop, maxNumEntries=7} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=2263}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', filter=BloomDimFilter.Factory{bloomSource=$view:region[R_REGIONKEY](R_NAME=='AMERICA'), fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop, maxNumEntries=1}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "TimeseriesQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=7}]}",
              "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=2263}]}",
              "TimeseriesQuery{dataSource='region', filter=R_NAME=='AMERICA', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[R_REGIONKEY], groupingSets=Noop, byRow=true, maxNumEntries=1}]}",
              "StreamQuery{dataSource='lineitem', filter=(BloomFilter{fields=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='L_PARTKEY'}], groupingSets=Noop} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='N_REGIONKEY', outputName='N_REGIONKEY'}], groupingSets=Noop}, columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}"
          );
        } else {
          hook.verifyHooked(
              "RU2amWb026/H8fsoq1eRaw==",
              "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}, StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='region', filter=R_NAME=='AMERICA', columns=[R_REGIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='d0:v', outputName='d0'}], virtualColumns=[ExprVirtualColumn{expression='YEAR(O_ORDERDATE)', outputName='d0:v'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case((N_NAME == 'ROMANIA'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(a0 / a1)', finalize=true}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, p0]}",
              "StreamQuery{dataSource='lineitem', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY, L_PARTKEY, L_SUPPKEY]}",
              "StreamQuery{dataSource='part', filter=P_TYPE=='ECONOMY BURNISHED NICKEL', columns=[P_PARTKEY], $hash=true}",
              "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY], $hash=true}",
              "StreamQuery{dataSource='orders', filter=BoundDimFilter{1995-01-01 <= O_ORDERDATE <= 1996-12-31(lexicographic)}, columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY]}",
              "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NATIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NATIONKEY, N_REGIONKEY], $hash=true}",
              "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
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

  public static final Object[][] TPCH9_RESULT = {
      {"ALGERIA", 1998L, 114041.26030389729D},
      {"ALGERIA", 1997L, 420005.5196960158D},
      {"ALGERIA", 1996L, 179435.92986568867D},
      {"ALGERIA", 1995L, 582584.3273152972D},
      {"ALGERIA", 1994L, 460802.84585832D},
      {"ALGERIA", 1993L, 358757.8330225676D},
      {"ALGERIA", 1992L, 196711.98163813291D},
      {"ARGENTINA", 1998L, 108010.48914558589D},
      {"ARGENTINA", 1997L, 38692.82840696454D},
      {"ARGENTINA", 1996L, 56161.96091754914D},
      {"ARGENTINA", 1995L, 206313.398783111D},
      {"ARGENTINA", 1994L, 138654.58356833278D},
      {"ARGENTINA", 1993L, 130070.15324769974D},
      {"ARGENTINA", 1992L, 249187.62450698193D},
      {"BRAZIL", 1997L, 275368.1544864571D},
      {"BRAZIL", 1996L, 121697.60182550666D},
      {"BRAZIL", 1995L, 180382.1936285472D},
      {"BRAZIL", 1994L, 135981.81291187106D},
      {"BRAZIL", 1993L, 92990.53491969705D},
      {"BRAZIL", 1992L, 132591.46454048812D},
      {"CANADA", 1998L, 203860.59923854948D},
      {"CANADA", 1997L, 321769.07278031466D},
      {"CANADA", 1996L, 171418.11807058338D},
      {"CANADA", 1995L, 335742.06487398804D},
      {"CANADA", 1994L, 111252.62975068866D},
      {"CANADA", 1993L, 195046.82526221155D},
      {"CANADA", 1992L, 290137.6926700413D},
      {"CHINA", 1998L, 172477.83238787178D},
      {"CHINA", 1997L, 275949.41915031796D},
      {"CHINA", 1996L, 262160.86919998226D},
      {"CHINA", 1995L, 311497.5106262428D},
      {"CHINA", 1994L, 163460.95393187582D},
      {"CHINA", 1993L, 180435.70739745855D},
      {"CHINA", 1992L, 330379.5476246345D},
      {"EGYPT", 1998L, 21087.45820478082D},
      {"EGYPT", 1997L, 103924.71271479249D},
      {"EGYPT", 1996L, 100910.74221206187D},
      {"EGYPT", 1995L, 79938.6035951674D},
      {"EGYPT", 1994L, 187349.30180459976D},
      {"EGYPT", 1993L, 330374.529838863D},
      {"EGYPT", 1992L, 280424.4704564249D},
      {"ETHIOPIA", 1998L, 194613.56150964915D},
      {"ETHIOPIA", 1997L, 220107.2496837282D},
      {"ETHIOPIA", 1996L, 158622.32755086065D},
      {"ETHIOPIA", 1995L, 146433.78099538267D},
      {"ETHIOPIA", 1994L, 223731.00869492054D},
      {"ETHIOPIA", 1993L, 392406.4558510614D},
      {"ETHIOPIA", 1992L, 120304.05713816821D},
      {"GERMANY", 1998L, 106323.3786909008D},
      {"GERMANY", 1997L, 92601.54000000001D},
      {"GERMANY", 1996L, 198944.05857446848D},
      {"GERMANY", 1995L, 165687.03981354833D},
      {"GERMANY", 1994L, 226676.94488913118D},
      {"GERMANY", 1993L, 141024.68987526838D},
      {"GERMANY", 1992L, 293949.9781136233D},
      {"INDIA", 1998L, 126584.3842398006D},
      {"INDIA", 1997L, 242388.4046601951D},
      {"INDIA", 1996L, 263227.1655768627D},
      {"INDIA", 1995L, 205509.0660050738D},
      {"INDIA", 1994L, 361137.83015613194D},
      {"INDIA", 1993L, 283929.86863369995D},
      {"INDIA", 1992L, 341885.8270686496D},
      {"INDONESIA", 1998L, 274430.05509148D},
      {"INDONESIA", 1997L, 465366.5118179262D},
      {"INDONESIA", 1996L, 500014.2962179356D},
      {"INDONESIA", 1995L, 424459.62825218006D},
      {"INDONESIA", 1994L, 346039.4265588528D},
      {"INDONESIA", 1993L, 450136.56612199725D},
      {"INDONESIA", 1992L, 602251.4160785652D},
      {"IRAN", 1998L, 131147.61450470984D},
      {"IRAN", 1997L, 87582.14874230146D},
      {"IRAN", 1996L, 95232.70590280532D},
      {"IRAN", 1995L, 115417.6692933458D},
      {"IRAN", 1994L, 190750.94298363506D},
      {"IRAN", 1993L, 78173.58062207936D},
      {"IRAN", 1992L, 9445.441345954541D},
      {"IRAQ", 1998L, 64116.22477363587D},
      {"IRAQ", 1997L, 53046.80687995434D},
      {"IRAQ", 1996L, 98945.09801960111D},
      {"IRAQ", 1994L, -791.629413479568D},
      {"IRAQ", 1993L, 112985.2972689104D},
      {"IRAQ", 1992L, 90281.51992891669D},
      {"JAPAN", 1998L, 134707.84294367553D},
      {"JAPAN", 1997L, 187434.7084983492D},
      {"JAPAN", 1996L, 130783.95952647389D},
      {"JAPAN", 1995L, 245886.58398801027D},
      {"JAPAN", 1994L, 96861.93079289672D},
      {"JAPAN", 1993L, 91508.3956084758D},
      {"JAPAN", 1992L, 319633.412598049D},
      {"JORDAN", 1998L, 84023.69245237112D},
      {"JORDAN", 1997L, 248273.9257530522D},
      {"JORDAN", 1996L, 303736.1332046127D},
      {"JORDAN", 1995L, 269849.52374407044D},
      {"JORDAN", 1994L, 82437.45666268229D},
      {"JORDAN", 1993L, 290887.20637964306D},
      {"JORDAN", 1992L, 275791.7683112568D},
      {"KENYA", 1998L, 74049.84995142937D},
      {"KENYA", 1997L, 311392.6756942439D},
      {"KENYA", 1996L, 185216.46588993492D},
      {"KENYA", 1995L, 80162.49504043101D},
      {"KENYA", 1994L, 302921.19519311254D},
      {"KENYA", 1993L, 325086.9646280855D},
      {"KENYA", 1992L, 343416.7887258017D},
      {"MOROCCO", 1998L, 119855.49532182992D},
      {"MOROCCO", 1997L, 290008.62978701474D},
      {"MOROCCO", 1996L, 14184.126959370373D},
      {"MOROCCO", 1995L, 69843.47478906036D},
      {"MOROCCO", 1994L, 191099.54942738713D},
      {"MOROCCO", 1993L, 137202.08173092187D},
      {"MOROCCO", 1992L, 66594.12980273308D},
      {"MOZAMBIQUE", 1998L, 117097.67470478592D},
      {"MOZAMBIQUE", 1997L, 363205.0272618228D},
      {"MOZAMBIQUE", 1996L, 311449.27139029326D},
      {"MOZAMBIQUE", 1995L, 473208.3942105938D},
      {"MOZAMBIQUE", 1994L, 442759.0781167674D},
      {"MOZAMBIQUE", 1993L, 440542.99224279815D},
      {"MOZAMBIQUE", 1992L, 287795.51908457343D},
      {"PERU", 1998L, 102725.66072352171D},
      {"PERU", 1997L, 171472.82874866007D},
      {"PERU", 1996L, 294416.149034543D},
      {"PERU", 1995L, 112348.73500679017D},
      {"PERU", 1994L, 95837.18646295546D},
      {"PERU", 1993L, 138317.59775996328D},
      {"PERU", 1992L, 85667.16805092811D},
      {"ROMANIA", 1998L, 2421.2874725103384D},
      {"ROMANIA", 1997L, 102189.5005055058D},
      {"ROMANIA", 1996L, 81265.36530949711D},
      {"ROMANIA", 1995L, 47749.046498327254D},
      {"ROMANIA", 1994L, 35394.23816981554D},
      {"ROMANIA", 1993L, 42641.986276656404D},
      {"ROMANIA", 1992L, 49277.804435415266D},
      {"RUSSIA", 1998L, 548958.650213117D},
      {"RUSSIA", 1997L, 466773.9072422556D},
      {"RUSSIA", 1996L, 901266.0350757325D},
      {"RUSSIA", 1995L, 803254.3702657815D},
      {"RUSSIA", 1994L, 932974.109281249D},
      {"RUSSIA", 1993L, 843491.4828173231D},
      {"RUSSIA", 1992L, 876831.2552419436D},
      {"UNITED KINGDOM", 1998L, 81480.06851940393D},
      {"UNITED KINGDOM", 1997L, 58282.63399619222D},
      {"UNITED KINGDOM", 1996L, 134110.5876093769D},
      {"UNITED KINGDOM", 1995L, 83918.57245126486D},
      {"UNITED KINGDOM", 1994L, 70544.89867743969D},
      {"UNITED KINGDOM", 1993L, 55681.24919359922D},
      {"UNITED KINGDOM", 1992L, 31602.863201971057D},
      {"UNITED STATES", 1998L, 196681.8699206197D},
      {"UNITED STATES", 1997L, 311459.70588090236D},
      {"UNITED STATES", 1996L, 451144.5775869352D},
      {"UNITED STATES", 1995L, 481350.9459593015D},
      {"UNITED STATES", 1994L, 473742.8177572065D},
      {"UNITED STATES", 1993L, 324866.8101639927D},
      {"UNITED STATES", 1992L, 343496.2612194723D},
      {"VIETNAM", 1998L, 198132.12613428713D},
      {"VIETNAM", 1997L, 426951.29093836364D},
      {"VIETNAM", 1996L, 610135.1697260014D},
      {"VIETNAM", 1995L, 316695.84854961873D},
      {"VIETNAM", 1994L, 489111.94571378885D},
      {"VIETNAM", 1993L, 343970.29225169297D},
      {"VIETNAM", 1992L, 352275.06760803703D}
  };

  @Test
  public void tpch9() throws Exception
  {
    testQuery(
        TPCH9,
        TPCH9_EXPLAIN,
        TPCH9_RESULT
    );

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

  @Test
  public void tpch10() throws Exception
  {
    testQuery(
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
        + " LIMIT 20"
        ,
        "DruidOuterQueryRel(scanProject=[$0, $1, $2, $3, $4, $5, $6, *($7, -(1, $8))], group=[{0, 1, 2, 3, 4, 5, 6}], REVENUE=[SUM($7)], sort=[$7:DESC], fetch=[20], sortProject=[$0, $1, $7, $2, $4, $5, $3, $6])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[5], rightKeys=[1], outputColumns=[3, 4, 0, 6, 9, 1, 2, 8, 7])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[7], rightKeys=[2], outputColumns=[0, 1, 2, 3, 4, 5, 6, 8, 9])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 5, 6, 8])\n"
        + "        DruidQueryRel(table=[druid.customer], scanProject=[$0, $1, $2, $3, $5, $6, $7])\n"
        + "        DruidQueryRel(table=[druid.orders], scanFilter=[AND(>=($3, '1993-07-01'), <($3, '1993-10-01'))], scanProject=[$2, $4])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[=($10, 'R')], scanProject=[$2, $3, $6])\n"
        + "    DruidQueryRel(table=[druid.nation], scanProject=[$1, $2])\n"
        ,
        new Object[]{"22", "Customer#000000022", 376659.3354837346D, 591.98D, "CANADA", "QI6p41,FNs5k7RZoCCVPUTkUdYpB", "13-806-545-9701", "s nod furiously above the furiously ironic ideas. "},
        new Object[]{"217", "Customer#000000217", 337546.5034711528D, 378.33D, "UNITED KINGDOM", "YIy05RMdthrXqdfnNKud", "33-159-298-3849", "ven frays wake according to the carefully "},
        new Object[]{"715", "Customer#000000715", 327733.10297188995D, 85.05D, "ROMANIA", "9qLvF42uxUarKl4I 2pEKOMNJmo8Ro5EK", "29-500-408-6392", "hins boost quickly. quickly regular epitaphs haggle fluffily quickly bold pinto beans. regular"},
        new Object[]{"55", "Customer#000000055", 325304.2209204144D, 4572.11D, "IRAN", "zIRBR4KNEl HzaiV3a i9n6elrxzDEh8r8pDom", "20-180-440-8525", "ully unusual packages wake bravely bold packages. unusual requests boost deposits! blithely ironic packages ab"},
        new Object[]{"19", "Customer#000000019", 295856.2535585952D, 8914.71D, "CHINA", "uc,3bHIx84H,wdrmLOjVsiqXCq2tr", "28-396-526-5053", " nag. furiously careful packages are slyly at the accounts. furiously regular in"},
        new Object[]{"686", "Customer#000000686", 284498.96497825027D, 5503.36D, "FRANCE", "1j C80VWHe ITCVCV", "16-682-293-3599", " even deposits print quickly. foxes wake. furiously ironic asymptotes across the bold foxes"},
        new Object[]{"202", "Customer#000000202", 280435.6208994395D, 2237.64D, "GERMANY", "Q0uJ1frCbi9yvu", "17-905-805-4635", "fully along the carefully pending Tiresias; special packages along the carefully special deposits try to"},
        new Object[]{"679", "Customer#000000679", 268885.6798972541D, 1394.44D, "IRAN", "IJf1FlZL9I9m,rvofcoKy5pRUOjUQV", "20-146-696-9508", "ely pending frays boost carefully"},
        new Object[]{"448", "Customer#000000448", 260133.37580457923D, 8117.27D, "UNITED STATES", "BH4vtnDpabk0NgoGNJWu4OUXnidfJ", "34-985-422-6009", "unts. final pinto beans boost carefully. furiously even foxes according to the express, regular pa"},
        new Object[]{"394", "Customer#000000394", 245405.0083951986D, 5200.96D, "UNITED KINGDOM", "nxW1jt,MQvImdr z72gAt1bslnfEipCh,bKZN", "33-422-600-6936", " instructions. carefully special ideas after the fluffily unusual r"},
        new Object[]{"64", "Customer#000000064", 245401.590318259D, -646.64D, "CANADA", "MbCeGY20kaKK3oalJD,OT", "13-558-731-7204", "structions after the quietly ironic theodolites cajole be"},
        new Object[]{"559", "Customer#000000559", 243818.18628837942D, 5872.94D, "GERMANY", "A3ACFoVbP,gPe xknVJMWC,wmRxb Nmg fWFS,UP", "17-395-429-6655", "al accounts cajole carefully across the accounts. furiously pending pinto beans across the "},
        new Object[]{"586", "Customer#000000586", 242057.21668825627D, 5134.35D, "IRAQ", "vGaA9XBtn,hlswFhSjLIXGlLEDD2flE8UXwj", "21-239-369-7791", "above the blithely express ideas. slyly r"},
        new Object[]{"721", "Customer#000000721", 234606.65834806324D, 3420.64D, "VIETNAM", "N6hr4gV9EkPBuE3Ayu ", "31-174-552-2949", "ar instructions. packages haggle stealthily ironic deposits. even platelets detect quickly. even sheaves along"},
        new Object[]{"65", "Customer#000000065", 228551.8947478521D, 8795.16D, "UNITED KINGDOM", "RGT yzQ0y4l0H90P783LG4U95bXQFDRXbWa1sl,X", "33-733-623-5267", "y final foxes serve carefully. theodolites are carefully. pending i"},
        new Object[]{"352", "Customer#000000352", 226905.6785498762D, 6257.88D, "INDONESIA", "HqhIE5GRTK0dFtWpJUQENU4aa1bwdsUBEWtzUw", "19-906-158-8420", "ts are. blithely special requests wake. furiously bold packages among the blithely eve"},
        new Object[]{"79", "Customer#000000079", 220721.15706590773D, 5121.28D, "MOROCCO", "n5hH2ftkVRwW8idtD,BmM2", "25-147-850-4166", "es. packages haggle furiously. regular, special requests poach after the quickly express ideas. blithely pending re"},
        new Object[]{"710", "Customer#000000710", 217848.3094086754D, 7412.12D, "RUSSIA", "OCLSZuXw1AEK NLvlofMkuK,YNe,bJD40a", "32-459-427-9559", "ges integrate express, even ideas"},
        new Object[]{"484", "Customer#000000484", 213702.96149329422D, 4245.0D, "SAUDI ARABIA", "ismzlUzrqRMRGWmCEUUjkBsi", "30-777-953-8902", "y against the express, even packages. blithely pending pearls haggle furiously above the fur"},
        new Object[]{"292", "Customer#000000292", 203414.17572621108D, 2975.43D, "IRAQ", "hCXh3vxC4uje9", "21-457-910-2923", "usly regular, ironic accounts. blithely regular platelets are carefully. blithely unusual ideas affi"}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "k1AwsHGmFH63SG9BzItU3g==",
          "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
          "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=(InDimFilter{dimension='C_CUSTKEY', values=[1, 115, 121, 122, 124, 127, 128, 13, 131, 136, ..195 more]} && InDimFilter{dimension='C_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}), columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, MaterializedQuery{dataSource=[orders]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}, MaterializedQuery{dataSource=[nation]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
          "StreamQuery{dataSource='customer', filter=(InDimFilter{dimension='C_CUSTKEY', values=[1, 115, 121, 122, 124, 127, 128, 13, 131, 136, ..195 more]} && InDimFilter{dimension='C_NATIONKEY', values=[0, 1, 10, 11, 12, 13, 14, 15, 16, 17, ..15 more]}), columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
          "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "fX7saqJ41KDJpyiv/2f7tg==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', filter=BloomDimFilter.Factory{bloomSource=$view:orders[O_CUSTKEY](BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop, maxNumEntries=286}, columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
            "TimeseriesQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_CUSTKEY], groupingSets=Noop, byRow=true, maxNumEntries=286}]}",
            "StreamQuery{dataSource='customer', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='C_CUSTKEY'}], groupingSets=Noop}, columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "pwu/c42IL3knlLeF8YX8Bw==",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}, StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}], timeColumnName=__time}, StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='C_NAME', outputName='d1'}, DefaultDimensionSpec{dimension='C_ACCTBAL', outputName='d2'}, DefaultDimensionSpec{dimension='C_PHONE', outputName='d3'}, DefaultDimensionSpec{dimension='N_NAME', outputName='d4'}, DefaultDimensionSpec{dimension='C_ADDRESS', outputName='d5'}, DefaultDimensionSpec{dimension='C_COMMENT', outputName='d6'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}], limit=20}, outputColumns=[d0, d1, a0, d2, d4, d5, d3, d6]}",
            "StreamQuery{dataSource='customer', columns=[C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE]}",
            "StreamQuery{dataSource='orders', filter=BoundDimFilter{1993-07-01 <= O_ORDERDATE < 1993-10-01(lexicographic)}, columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=L_RETURNFLAG=='R', columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_ORDERKEY]}",
            "StreamQuery{dataSource='nation', columns=[N_NAME, N_NATIONKEY], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch11() throws Exception
  {
    testQuery(
        "SELECT"
        + "  *"
        + " FROM ("
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
        + " ORDER BY PART_VALUE DESC"
        ,
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
        + "        DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'GERMANY')], scanProject=[$2])\n"
        ,
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

  @Test
  public void tpch12() throws Exception
  {
    testQuery(
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
        + "    L_SHIPMODE"
        ,
        "DruidOuterQueryRel(scanProject=[$0, CASE(OR(=($1, '1-URGENT'), =($1, '2-HIGH')), 1, 0), CASE(AND(<>($1, '1-URGENT'), <>($1, '2-HIGH')), 1, 0), IS TRUE(OR(=($1, '1-URGENT'), =($1, '2-HIGH'))), IS TRUE(AND(<>($1, '1-URGENT'), <>($1, '2-HIGH')))], group=[{0}], high_line_count=[COUNT() FILTER $3], low_line_count=[COUNT() FILTER $4], sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[3, 1])\n"
        + "    DruidQueryRel(table=[druid.orders], scanProject=[$4, $5])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(OR(=($13, 'REG AIR'), =($13, 'MAIL')), <($1, $9), <($11, $1), >=($9, '1995-01-01'), <($9, '1996-01-01'))], scanProject=[$6, $13])\n"
        ,
        new Object[]{"MAIL", 34L, 44L},
        new Object[]{"REG AIR", 37L, 43L}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "ji9roIJVcfQOM+n/nVVC1w==",
          "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=InDimFilter{dimension='O_ORDERKEY', values=[10145, 10213, 10535, 10624, 10695, 10884, 11270, 11584, 11936, 12037, ..137 more]}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}, MaterializedQuery{dataSource=[lineitem]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
          "StreamQuery{dataSource='orders', filter=InDimFilter{dimension='O_ORDERKEY', values=[10145, 10213, 10535, 10624, 10695, 10884, 11270, 11584, 11936, 12037, ..137 more]}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "uI1dS3F0AokMscrNZ0kS7A==",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', filter=BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY]((InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)})), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=158}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}, StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
            "TimeseriesQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=158}]}",
            "StreamQuery{dataSource='orders', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}, columns=[O_ORDERKEY, O_ORDERPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "7A/f1FBvtgYsxXx4AAwRkQ==",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='orders', columns=[O_ORDERKEY, O_ORDERPRIORITY]}, StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_SHIPMODE', outputName='d0'}], aggregatorSpecs=[FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a0'}, filter=InDimFilter{dimension='O_ORDERPRIORITY', values=[1-URGENT, 2-HIGH]}}, FilteredAggregatorFactory{delegate=CountAggregatorFactory{name='a1'}, filter=(!(O_ORDERPRIORITY=='1-URGENT') && !(O_ORDERPRIORITY=='2-HIGH'))}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d0', direction=ascending}], limit=-1}, outputColumns=[d0, a0, a1]}",
            "StreamQuery{dataSource='orders', columns=[O_ORDERKEY, O_ORDERPRIORITY]}",
            "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_SHIPMODE', values=[MAIL, REG AIR]} && MathExprFilter{expression='(L_COMMITDATE < L_RECEIPTDATE)'} && MathExprFilter{expression='(L_SHIPDATE < L_COMMITDATE)'} && BoundDimFilter{1995-01-01 <= L_RECEIPTDATE < 1996-01-01(lexicographic)}), columns=[L_ORDERKEY, L_SHIPMODE], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch13() throws Exception
  {
    testQuery(
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
        + " ORDER BY custdist desc, c_count desc"
        ,
        "DruidOuterQueryRel(group=[{0}], custdist=[COUNT()], sort=[$1:DESC, $0:DESC])\n"
        + "  DruidOuterQueryRel(group=[{0}], c_count=[COUNT($1)], aggregateProject=[$1])\n"
        + "    DruidJoinRel(joinType=[LEFT], leftKeys=[0], rightKeys=[0], outputColumns=[0, 2])\n"
        + "      DruidQueryRel(table=[druid.customer], scanProject=[$3])\n"
        + "      DruidQueryRel(table=[druid.orders], scanFilter=[NOT(LIKE($1, '%unusual%accounts%'))], scanProject=[$2, $4])\n"
        ,
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
        "uw4p4RoQJJxhhsuTH6w9Jw==",
        "GroupByQuery{dataSource='GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}, StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0', fieldName='O_ORDERKEY'}], outputColumns=[a0]}', dimensions=[DefaultDimensionSpec{dimension='a0', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='_a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=descending}], limit=-1}, outputColumns=[d0, _a0]}",
        "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}, StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0', fieldName='O_ORDERKEY'}], outputColumns=[a0]}",
        "StreamQuery{dataSource='customer', columns=[C_CUSTKEY], orderingSpecs=[OrderByColumnSpec{dimension='C_CUSTKEY', direction=ascending}]}",
        "StreamQuery{dataSource='orders', filter=!(O_COMMENT LIKE '%unusual%accounts%'), columns=[O_CUSTKEY, O_ORDERKEY], $hash=true}"
    );
  }

  @Test
  public void tpch14() throws Exception
  {
    testQuery(
        "SELECT"
        + "    100.00 * sum(case when P_TYPE like 'PROMO%' then L_EXTENDEDPRICE * (1 - L_DISCOUNT) else 0 end) / sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as promo_revenue"
        + " FROM"
        + "    lineitem, part"
        + " WHERE"
        + "    L_PARTKEY = P_PARTKEY AND" 
        + "    L_SHIPDATE >= '1995-08-01' AND" 
        + "    L_SHIPDATE < '1995-09-01'"
        ,
        "DruidOuterQueryRel(scanProject=[CASE(LIKE($0, 'PROMO%'), *($1, -(1, $2)), 0:DOUBLE), *($1, -(1, $2))], agg#0=[SUM($0)], agg#1=[SUM($1)], aggregateProject=[/(*(100.00:DECIMAL(5, 2), $0), $1)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[4, 1, 0])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1995-08-01'), <($11, '1995-09-01'))], scanProject=[$2, $3, $7])\n"
        + "    DruidQueryRel(table=[druid.part], scanProject=[$5, $8])\n"
        ,
        new Object[]{21.62198225363824}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "Hd5He7jMK+gpRhdWbBM8BA==",
          "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[MaterializedQuery{dataSource=[lineitem]}, StreamQuery{dataSource='part', filter=InDimFilter{dimension='P_PARTKEY', values=[102, 107, 109, 11, 110, 113, 12, 123, 124, 125, ..335 more]}, columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
          "StreamQuery{dataSource='part', filter=InDimFilter{dimension='P_PARTKEY', values=[102, 107, 109, 11, 110, 113, 12, 123, 124, 125, ..335 more]}, columns=[P_PARTKEY, P_TYPE]}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "gjramO2DXF28YkuBC/QDrA==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}, StreamQuery{dataSource='part', filter=BloomDimFilter.Factory{bloomSource=$view:lineitem[L_PARTKEY](BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}), fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop, maxNumEntries=408}, columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
            "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=408}]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='part', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='P_PARTKEY'}], groupingSets=Noop}, columns=[P_PARTKEY, P_TYPE]}"
        );
      } else {
        hook.verifyHooked(
            "IcKUhtIVXw4+jCV6/rbj9Q==",
            "TimeseriesQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}, StreamQuery{dataSource='part', columns=[P_PARTKEY, P_TYPE]}], timeColumnName=__time}', aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='case(like(P_TYPE,'PROMO%'),(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),0)', inputType='double'}, GenericSumAggregatorFactory{name='a1', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='((100.00B * a0) / a1)', finalize=true}], outputColumns=[p0]}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1995-08-01 <= L_SHIPDATE < 1995-09-01(lexicographic)}, columns=[L_DISCOUNT, L_EXTENDEDPRICE, L_PARTKEY], $hash=true}",
            "StreamQuery{dataSource='part', columns=[P_PARTKEY, P_TYPE]}"
        );
      }
    }
  }

  @Test
  public void tpch15() throws Exception
  {
    testQuery(
        "WITH revenue_cached AS ("
        + " SELECT"
        + "    L_SUPPKEY AS SUPPLIER_NO,"
        + "    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE"
        + " FROM"
        + "    lineitem"
        + " WHERE"
        + "    L_SHIPDATE >= '1996-01-01' AND" 
        + "    L_SHIPDATE < '1996-04-01'"
        + " GROUP BY L_SUPPKEY"
        + "),"
        + "max_revenue_cached AS ("
        + " SELECT MAX(TOTAL_REVENUE) AS MAX_REVENUE FROM revenue_cached"
        + ")"
        + " SELECT"
        + "    S_SUPPKEY,"
        + "    S_NAME,"
        + "    S_ADDRESS,"
        + "    S_PHONE,"
        + "    TOTAL_REVENUE"
        + " FROM"
        + "    supplier,"
        + "    revenue_cached,"
        + "    max_revenue_cached"
        + " WHERE"
        + "    S_SUPPKEY = SUPPLIER_NO AND" 
        + "    TOTAL_REVENUE = MAX_REVENUE"
        + " ORDER BY S_SUPPKEY"
        ,
        "DruidOuterQueryRel(sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[4], rightKeys=[0], outputColumns=[3, 1, 0, 2, 4])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 5])\n"
        + "      DruidQueryRel(table=[druid.supplier], scanProject=[$1, $3, $5, $6])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)])\n"
        + "    DruidOuterQueryRel(MAX_REVENUE=[MAX($0)])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1996-01-01'), <($11, '1996-04-01'))], scanProject=[$14, *($3, -(1, $2))], group=[{0}], TOTAL_REVENUE=[SUM($1)], aggregateProject=[$1])\n"
        ,
        new Object[]{"6", "Supplier#000000006", "tQxuVm7s7CnK", "24-696-997-4969", 1080265.1368512397D}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "ZqGhPsOOVAxF4R3zHnNrAw==",
          "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}",
          "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[supplier]}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=(BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}, TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0]}], timeColumnName=__time}', columns=[S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, a0], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=(BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}",
          "TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}"
      );
    } else {
      hook.verifyHooked(
          "7z1K6uYWrA8PhdAqJltL3A==",
          "TimeseriesQuery{dataSource='lineitem', filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "StreamQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}], timeColumnName=__time}, TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0]}], timeColumnName=__time}', columns=[S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, a0], orderingSpecs=[OrderByColumnSpec{dimension='S_SUPPKEY', direction=ascending}]}",
          "StreamQuery{dataSource='supplier', columns=[S_ADDRESS, S_NAME, S_PHONE, S_SUPPKEY], $hash=true}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[d0, a0]}",
          "TimeseriesQuery{dataSource='GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}', aggregatorSpecs=[GenericMaxAggregatorFactory{name='_a0', fieldName='a0', inputType='double'}], outputColumns=[_a0]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d0'}], filter=BoundDimFilter{1996-01-01 <= L_SHIPDATE < 1996-04-01(lexicographic)}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldExpression='(L_EXTENDEDPRICE * (1 - L_DISCOUNT))', inputType='double'}], outputColumns=[a0]}"
      );
    }
  }

  @Test
  public void tpch16() throws Exception
  {
    testQuery(
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
        + "    supplier_cnt desc, P_BRAND, P_TYPE, P_SIZE"
        ,
        "DruidOuterQueryRel(scanFilter=[OR(=($4, 0), AND(IS NULL($5), >=($6, $4), IS NOT NULL($3)))], scanProject=[$0, $1, $2, $3], group=[{0, 1, 2}], supplier_cnt=[COUNT(DISTINCT $3)], sort=[$3:DESC, $0:ASC, $1:ASC, $2:ASC])\n"
        + "  DruidJoinRel(joinType=[LEFT], leftKeys=[1], rightKeys=[0], outputColumns=[2, 5, 4, 1, 6, 9, 7])\n"
        + "    DruidJoinRel(joinType=[INNER])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1])\n"
        + "        DruidQueryRel(table=[druid.partsupp], scanProject=[$2, $3])\n"
        + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(OR(=($7, 22), =($7, 14), =($7, 27), =($7, 49), =($7, 21), =($7, 33), =($7, 35), =($7, 28)), <>($0, 'Brand#34'), NOT(LIKE($8, 'ECONOMY BRUSHED%')))], scanProject=[$0, $5, $7, $8])\n"
        + "      DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6], c=[COUNT()], ck=[COUNT($0)])\n"
        + "    DruidQueryRel(table=[druid.supplier], scanFilter=[LIKE($2, '%Customer%Complaints%')], scanProject=[$6, true], group=[{0, 1}])\n"
    );
    if (semiJoin) {
      hook.verifyHooked(
          "qL3KPIdAAS9iMJfHacix6g==",
          "StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[105, 106, 111, 116, 127, 134, 14, 150, 155, 162, ..139 more]}, columns=[PS_PARTKEY, PS_SUPPKEY]}, MaterializedQuery{dataSource=[part]}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d2'}], filter=((a0=='0' || d1==NULL) && (a0=='0' || MathExprFilter{expression='(a1 >= a0)'}) && (a0=='0' || !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fields=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='PS_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _d2, _a0]}",
          "StreamQuery{dataSource='partsupp', filter=InDimFilter{dimension='PS_PARTKEY', values=[105, 106, 111, 116, 127, 134, 14, 150, 155, 162, ..139 more]}, columns=[PS_PARTKEY, PS_SUPPKEY]}",
          "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
          "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
      );
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "QhQ99NkRUpniGYQwuiCQ0g==",
            "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=BloomDimFilter.Factory{bloomSource=$view:part[P_PARTKEY]((InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%'))), fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop, maxNumEntries=149}, columns=[PS_PARTKEY, PS_SUPPKEY]}, StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d2'}], filter=((a0=='0' || d1==NULL) && (a0=='0' || MathExprFilter{expression='(a1 >= a0)'}) && (a0=='0' || !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fields=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='PS_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _d2, _a0]}",
            "TimeseriesQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[P_PARTKEY], groupingSets=Noop, byRow=true, maxNumEntries=149}]}",
            "StreamQuery{dataSource='partsupp', filter=BloomFilter{fields=[DefaultDimensionSpec{dimension='PS_PARTKEY', outputName='PS_PARTKEY'}], groupingSets=Noop}, columns=[PS_PARTKEY, PS_SUPPKEY]}",
            "StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
            "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
            "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "zdaPOjyshJE9bsRAvEtMQw==",
            "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY]}, StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}], timeColumnName=__time}, TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='P_BRAND', outputName='_d0'}, DefaultDimensionSpec{dimension='P_TYPE', outputName='_d1'}, DefaultDimensionSpec{dimension='P_SIZE', outputName='_d2'}], filter=((a0=='0' || d1==NULL) && (a0=='0' || MathExprFilter{expression='(a1 >= a0)'}) && (a0=='0' || !(PS_SUPPKEY==NULL))), aggregatorSpecs=[CardinalityAggregatorFactory{name='_a0', fields=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='PS_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='_a0', direction=descending}, OrderByColumnSpec{dimension='_d0', direction=ascending}, OrderByColumnSpec{dimension='_d1', direction=ascending}, OrderByColumnSpec{dimension='_d2', direction=ascending}], limit=-1}, outputColumns=[_d0, _d1, _d2, _a0]}",
            "StreamQuery{dataSource='partsupp', columns=[PS_PARTKEY, PS_SUPPKEY]}",
            "StreamQuery{dataSource='part', filter=(InDimFilter{dimension='P_SIZE', values=[14, 21, 22, 27, 28, 33, 35, 49]} && !(P_BRAND=='Brand#34') && !(P_TYPE LIKE 'ECONOMY BRUSHED%')), columns=[P_BRAND, P_PARTKEY, P_SIZE, P_TYPE], $hash=true}",
            "TimeseriesQuery{dataSource='supplier', filter=S_COMMENT LIKE '%Customer%Complaints%', aggregatorSpecs=[CountAggregatorFactory{name='a0'}, CountAggregatorFactory{name='a1', fieldName='S_SUPPKEY'}], outputColumns=[a0, a1], $hash=true}",
            "GroupByQuery{dataSource='supplier', dimensions=[DefaultDimensionSpec{dimension='S_SUPPKEY', outputName='d0'}, DefaultDimensionSpec{dimension='d1:v', outputName='d1'}], filter=S_COMMENT LIKE '%Customer%Complaints%', virtualColumns=[ExprVirtualColumn{expression='true', outputName='d1:v'}], outputColumns=[d0, d1], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch17() throws Exception
  {
    testQuery(
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
        + "    CAST(SUM(L_EXTENDEDPRICE) / 7.0 AS DECIMAL(32,2)) AS AVG_YEARLY"
        + " FROM" 
        + "    Q17_AVG, Q17_PRICE"
        + " WHERE"
        + "    T_PARTKEY = L_PARTKEY AND L_QUANTITY < T_AVG_QUANTITY"
        ,
        "DruidOuterQueryRel(scanFilter=[<($1, $2)], scanProject=[$0], agg#0=[SUM($0)], aggregateProject=[CAST(/($0, 7.0:DECIMAL(2, 1))):DECIMAL(19, 2)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[1], outputColumns=[4, 2, 1])\n"
        + "    DruidOuterQueryRel(group=[{0}], agg#0=[AVG($1)], aggregateProject=[$0, *(0.2:DECIMAL(2, 1), $1)])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1])\n"
        + "        DruidQueryRel(table=[druid.lineitem], scanProject=[$7, $8])\n"
        + "        DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[2, 1, 0])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$3, $7, $8])\n"
        + "      DruidQueryRel(table=[druid.part], scanFilter=[AND(=($0, 'Brand#31'), =($2, 'MED BOX'))], scanProject=[$5], group=[{0}])\n"
        ,
        new Object[] {new BigDecimal(4923)}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "KXrAKx4X0aVrt6c0E8dtNg==",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0]}",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0]}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}, StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_QUANTITY, L_PARTKEY, L_EXTENDEDPRICE]}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < p0)'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='CAST((a0 / 7.0B), 'decimal')', finalize=true}], outputColumns=[p0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_PARTKEY, L_QUANTITY]}",
          "StreamQuery{dataSource='lineitem', filter=InDimFilter{dimension='L_PARTKEY', values=[558, 855]}, columns=[L_QUANTITY, L_PARTKEY, L_EXTENDEDPRICE]}"
      );
    } else {
      hook.verifyHooked(
          "BFigKjX0Utv6WCCo/ppZqw==",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "TimeseriesQuery{dataSource='part', filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "TimeseriesQuery{dataSource='CommonJoin{queries=[GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}, CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_EXTENDEDPRICE, L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', filter=MathExprFilter{expression='(L_QUANTITY < p0)'}, aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_EXTENDEDPRICE', inputType='double'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='CAST((a0 / 7.0B), 'decimal')', finalize=true}], outputColumns=[p0]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', columns=[L_PARTKEY, L_QUANTITY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=(P_BRAND=='Brand#31' && P_CONTAINER=='MED BOX'), outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0:sum', fieldName='L_QUANTITY', inputType='long'}, CountAggregatorFactory{name='a0:count'}], postAggregatorSpecs=[ArithmeticPostAggregator{name='a0', fnName='quotient', fields=[FieldAccessPostAggregator{name='null', fieldName='a0:sum'}, FieldAccessPostAggregator{name='null', fieldName='a0:count'}], op=QUOTIENT}, MathPostAggregator{name='p0', expression='(0.2B * a0)', finalize=true}], outputColumns=[d0, p0], $hash=true}",
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
        "SELECT"
        + "  C_NAME,"
        + "  C_CUSTKEY,"
        + "  O_ORDERKEY,"
        + "  O_ORDERDATE,"
        + "  O_TOTALPRICE,"
        + "  SUM(L_QUANTITY)"
        + " FROM"
        + "  customer,"
        + "  orders,"
        + "  lineitem"
        + " WHERE"
        + "  O_ORDERKEY IN ("
        + "    SELECT"
        + "      L_ORDERKEY"
        + "    FROM"
        + "      lineitem"
        + "    GROUP BY"
        + "      L_ORDERKEY"
        + "    HAVING"
        + "      SUM(L_QUANTITY) > 300"
        + "    )"
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
        + " LIMIT 100"
        ,
        "DruidOuterQueryRel(group=[{0, 1, 2, 3, 4}], EXPR$5=[SUM($5)], sort=[$4:DESC, $3:ASC, $2:ASC], fetch=[100])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[1, 0, 3, 2, 4, 5])\n"
        + "    DruidJoinRel(joinType=[INNER], leftKeys=[3], rightKeys=[0], outputColumns=[0, 1, 2, 3, 4, 6])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[0, 1, 3, 4, 5])\n"
        + "        DruidQueryRel(table=[druid.customer], scanProject=[$3, $5])\n"
        + "        DruidQueryRel(table=[druid.orders], scanProject=[$2, $3, $4, $8])\n"
        + "      DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanProject=[$6, $8], group=[{0}], agg#0=[SUM($1)], aggregateFilter=[>($1, 300)], aggregateProject=[$0])\n"
        ,
        new Object[]{"Customer#000000334", "334", "29158", "1995-10-21", 441562.47D, 305L},
        new Object[]{"Customer#000000089", "89", "6882", "1997-04-09", 389430.93D, 303L}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "OX4sgCK+nj4rYCqS+RMq0A==",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}",
          "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[MaterializedQuery{dataSource=[customer]}, StreamQuery{dataSource='orders', filter=(InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]} && InDimFilter{dimension='O_ORDERKEY', values=[29158, 6882]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d1'}, DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='d2'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d3'}, DefaultDimensionSpec{dimension='O_TOTALPRICE', outputName='d4'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d4', direction=descending}, OrderByColumnSpec{dimension='d3', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=100}, outputColumns=[d0, d1, d2, d3, d4, a0]}",
          "StreamQuery{dataSource='orders', filter=(InDimFilter{dimension='O_CUSTKEY', values=[1, 10, 100, 101, 102, 103, 104, 105, 106, 107, ..740 more]} && InDimFilter{dimension='O_ORDERKEY', values=[29158, 6882]}), columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}",
          "StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}"
      );
    } else {
      hook.verifyHooked(
          "iOiXptowcExUDgBNFVMF+g==",
          "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}, StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}], timeColumnName=__time}, StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='C_NAME', outputName='d0'}, DefaultDimensionSpec{dimension='C_CUSTKEY', outputName='d1'}, DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='d2'}, DefaultDimensionSpec{dimension='O_ORDERDATE', outputName='d3'}, DefaultDimensionSpec{dimension='O_TOTALPRICE', outputName='d4'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='d4', direction=descending}, OrderByColumnSpec{dimension='d3', direction=ascending}, OrderByColumnSpec{dimension='d2', direction=ascending}], limit=100}, outputColumns=[d0, d1, d2, d3, d4, a0]}",
          "StreamQuery{dataSource='customer', columns=[C_CUSTKEY, C_NAME], $hash=true}",
          "StreamQuery{dataSource='orders', columns=[O_CUSTKEY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE]}",
          "StreamQuery{dataSource='lineitem', columns=[L_ORDERKEY, L_QUANTITY]}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 300)'}, outputColumns=[d0], $hash=true}"
      );
    }
  }

  @Test
  public void tpch19() throws Exception
  {
    testQuery(
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
        + "    )"
        ,
        "DruidOuterQueryRel(scanFilter=[OR(AND(=($2, 'Brand#32'), OR(=($3, 'SM CASE'), =($3, 'SM BOX'), =($3, 'SM PACK'), =($3, 'SM PKG')), >=($4, 7), <=($4, 17), <=($5, 5)), AND(=($2, 'Brand#35'), OR(=($3, 'MED BAG'), =($3, 'MED BOX'), =($3, 'MED PKG'), =($3, 'MED PACK')), >=($4, 15), <=($4, 25), <=($5, 10)), AND(=($2, 'Brand#24'), OR(=($3, 'LG CASE'), =($3, 'LG BOX'), =($3, 'LG PACK'), =($3, 'LG PKG')), >=($4, 26), <=($4, 36), <=($5, 15)))], scanProject=[*($0, -(1, $1))], REVENUE=[SUM($0)])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[2], outputColumns=[1, 0, 4, 5, 3, 7])\n"
        + "    DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(OR(=($13, 'AIR'), =($13, 'AIR REG')), =($12, 'DELIVER IN PERSON'))], scanProject=[$2, $3, $7, $8])\n"
        + "    DruidQueryRel(table=[druid.part], scanFilter=[>=($7, 1)], scanProject=[$0, $2, $5, $7])\n"
    );
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

  @Test
  public void tpch20() throws Exception
  {
    testQuery(
        "WITH TMP1 AS ("
        + "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"
        + "),"
        + "TMP2 AS ("
        + "SELECT S_NAME, S_ADDRESS, S_SUPPKEY"
        + " FROM supplier, nation"
        + " WHERE S_NATIONKEY = N_NATIONKEY"
        + " AND N_NAME = 'RUSSIA'"    // changed 'CANADA' to 'RUSSIA'
        + "),"
        + "TMP3 AS ("
        + "SELECT L_PARTKEY, 0.5 * SUM(L_QUANTITY) AS SUM_QUANTITY, L_SUPPKEY"
        + " FROM lineitem, TMP2"
        + " WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE <= '1995-01-01'"
        + " AND L_SUPPKEY = S_SUPPKEY"
        + " GROUP BY L_PARTKEY, L_SUPPKEY"
        + "),"
        + "TMP4 AS ("
        + " SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY"
        + " FROM partsupp"
        + " WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM TMP1)"
        + "),"
        + "TMP5 AS ("
        + " SELECT"
        + "    PS_SUPPKEY"
        + " FROM"
        + "    TMP4, TMP3"
        + " WHERE"
        + "    PS_PARTKEY = L_PARTKEY AND"
        + "    PS_SUPPKEY = L_SUPPKEY AND"
        + "    PS_AVAILQTY > SUM_QUANTITY"
        + ")"
        + "SELECT"
        + "    S_NAME, S_ADDRESS"
        + " FROM"
        + "    supplier"
        + " WHERE"
        + "    S_SUPPKEY IN (SELECT PS_SUPPKEY FROM TMP5)"
        + " ORDER BY"
        + "    S_NAME"
        ,
        "DruidOuterQueryRel(sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[1, 0])\n"
        + "    DruidQueryRel(table=[druid.supplier], scanFilter=[IS NOT NULL($6)], scanProject=[$1, $3, $6])\n"
        + "    DruidOuterQueryRel(scanFilter=[>($1, $2)], scanProject=[$0], group=[{0}])\n"
        + "      DruidJoinRel(joinType=[INNER], leftKeys=[0, 1], rightKeys=[0, 2], outputColumns=[1, 2, 4])\n"
        + "        DruidJoinRel(joinType=[INNER], leftKeys=[1], rightKeys=[0], outputColumns=[1, 2, 0])\n"
        + "          DruidQueryRel(table=[druid.partsupp], scanFilter=[IS NOT NULL($3)], scanProject=[$0, $2, $3])\n"
        + "          DruidQueryRel(table=[druid.part], scanFilter=[LIKE($4, 'forest%')], scanProject=[$5], group=[{0}])\n"
        + "        DruidOuterQueryRel(group=[{0, 1}], agg#0=[SUM($2)], aggregateFilter=[IS NOT NULL($0)], aggregateProject=[$0, *(0.5:DECIMAL(2, 1), $2), $1])\n"
        + "          DruidJoinRel(joinType=[INNER], leftKeys=[2], rightKeys=[0], outputColumns=[0, 2, 1])\n"
        + "            DruidQueryRel(table=[druid.lineitem], scanFilter=[AND(>=($11, '1994-01-01'), <=($11, '1995-01-01'))], scanProject=[$7, $8, $14])\n"
        + "            DruidJoinRel(joinType=[INNER], leftKeys=[0], rightKeys=[0], outputColumns=[1])\n"
        + "              DruidQueryRel(table=[druid.supplier], scanProject=[$4, $6])\n"
        + "              DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'RUSSIA')], scanProject=[$2])\n"
        ,
        new Object[] {"Supplier#000000025", "RCQKONXMFnrodzz6w7fObFVV6CUm2q"}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "owhTGJKQ1vWUps4JjWRQTg==",
          "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY]}",
          "StreamQuery{dataSource='supplier', filter=InDimFilter{dimension='S_NATIONKEY', values=[22]}, columns=[S_SUPPKEY]}",
          "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=(!(PS_SUPPKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY]}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1], $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1], $hash=true}",
          "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && InDimFilter{dimension='L_SUPPKEY', values=[16, 25, 40, 42]}), columns=[L_PARTKEY, L_SUPPKEY, L_QUANTITY]}",
          "StreamQuery{dataSource='partsupp', filter=(!(PS_SUPPKEY==NULL) && InDimFilter{dimension='PS_PARTKEY', values=[304, 447, 488, 5, 696, 722, 748, 986]}), columns=[PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY]}",
          "StreamQuery{dataSource='StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && S_SUPPKEY=='25'), columns=[S_NAME, S_ADDRESS]}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
          "StreamQuery{dataSource='supplier', filter=(!(S_SUPPKEY==NULL) && S_SUPPKEY=='25'), columns=[S_NAME, S_ADDRESS]}"
      );
    } else {
      if (broadcastJoin) {
        hook.verifyHooked(
            "2ECEDtvJANcoVXYYovgfvQ==",
            "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY]}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
            "StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='lineitem', filter=(BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)} && BloomFilter{fieldNames=[L_SUPPKEY], groupingSets=Noop}), columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=lineitem, leftJoinColumns=[L_SUPPKEY], rightAlias=supplier+nation, rightJoinColumns=[S_SUPPKEY]}, hashLeft=false, hashSignature={S_SUPPKEY:dimension.string}}}",
            "StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "Dy8hNvQaNk7aHWWa//uV8Q==",
            "TimeseriesQuery{dataSource='part', filter=P_NAME LIKE 'forest%', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "StreamQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}, GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}], timeColumnName=__time}', columns=[S_NAME, S_ADDRESS], orderingSpecs=[OrderByColumnSpec{dimension='S_NAME', direction=ascending}]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}, GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1]}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='PS_SUPPKEY', outputName='d0'}], filter=MathExprFilter{expression='(PS_AVAILQTY > p0)'}, outputColumns=[d0]}",
            "StreamQuery{dataSource='partsupp', filter=!(PS_SUPPKEY==NULL), columns=[PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY]}",
            "GroupByQuery{dataSource='part', dimensions=[DefaultDimensionSpec{dimension='P_PARTKEY', outputName='d0'}], filter=P_NAME LIKE 'forest%', outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='L_PARTKEY', outputName='d0'}, DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='d1'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='L_QUANTITY', inputType='long'}], postAggregatorSpecs=[MathPostAggregator{name='p0', expression='(0.5B * a0)', finalize=true}], havingSpec=ExpressionHavingSpec{expression='isNotNull(d0)'}, outputColumns=[d0, p0, d1]}",
            "StreamQuery{dataSource='supplier', columns=[S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='RUSSIA', columns=[N_NATIONKEY], $hash=true}",
            "StreamQuery{dataSource='lineitem', filter=BoundDimFilter{1994-01-01 <= L_SHIPDATE <= 1995-01-01(lexicographic)}, columns=[L_PARTKEY, L_QUANTITY, L_SUPPKEY]}",
            "StreamQuery{dataSource='supplier', filter=!(S_SUPPKEY==NULL), columns=[S_ADDRESS, S_NAME, S_SUPPKEY], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch21() throws Exception
  {
    testQuery(
        "WITH LOCATION AS ("
        + " SELECT supplier.* FROM supplier, nation WHERE"
        + " S_NATIONKEY = N_NATIONKEY AND N_NAME = 'UNITED STATES'"   // changed 'SAUDI ARABIA' to 'UNITED STATES'
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
        + " LIMIT 100"
        ,
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
        + "      DruidQueryRel(table=[druid.nation], scanFilter=[=($1, 'UNITED STATES')], scanProject=[$2])\n"
        ,
        new Object[]{"Supplier#000000010", 15L},
        new Object[]{"Supplier#000000019", 15L},
        new Object[]{"Supplier#000000046", 15L},
        new Object[]{"Supplier#000000049", 5L}
    );
    if (semiJoin) {
      hook.verifyHooked(
          "No0JuPMf1zDGn94scZ/HzA==",
          "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY]}",
          "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0]}",
          "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
          "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0]}",
          "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
          "StreamQuery{dataSource='lineitem', filter=(InDimFilter{dimension='L_ORDERKEY', values=[10018, 10021, 10022, 10048, 10274, 10305, 10434, 10437, 10438, 10528, ..534 more]} && MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), columns=[L_SUPPKEY]}",
          "GroupByQuery{dataSource='StreamQuery{dataSource='supplier', filter=(S_NATIONKEY=='24' && InDimFilter{dimension='S_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[S_NAME, S_SUPPKEY], outputColumns=[S_NAME], localPostProcessing=RowExplodeProcessor{columns=[S_SUPPKEY]}}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
          "StreamQuery{dataSource='supplier', filter=(S_NATIONKEY=='24' && InDimFilter{dimension='S_SUPPKEY', values=[1, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..40 more]}), columns=[S_NAME, S_SUPPKEY], outputColumns=[S_NAME], localPostProcessing=RowExplodeProcessor{columns=[S_SUPPKEY]}}"
      );
    } else if (broadcastJoin) {
      if (bloomFilter) {
        hook.verifyHooked(
            "ES8q9B8ZnY01Dv2ZBxM/Qw==",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=18965}), columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=18965}]}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "X3Hc/gdYL1LkuP1hrTlSkg==",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY]}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', filter=BloomFilter{fieldNames=[S_NATIONKEY], groupingSets=Noop}, columns=[S_NAME, S_NATIONKEY, S_SUPPKEY], localPostProcessing=BroadcastJoinProcessor{element=JoinElement{joinType=INNER, leftAlias=supplier, leftJoinColumns=[S_NATIONKEY], rightAlias=nation, rightJoinColumns=[N_NATIONKEY]}, hashLeft=false, hashSignature={N_NATIONKEY:dimension.string}}, $hash=true}"
        );
      }
    } else {
      if (bloomFilter) {
        hook.verifyHooked(
            "3eHrB+C2JnhoqT5AlPfOHQ==",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomDimFilter.Factory{bloomSource=$view:orders[O_ORDERKEY](O_ORDERSTATUS=='F'), fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop, maxNumEntries=3655}), columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomDimFilter.Factory{bloomSource=$view:lineitem[L_ORDERKEY](MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}), fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop, maxNumEntries=18965}), columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "TimeseriesQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[O_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=3655}]}",
            "TimeseriesQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, aggregatorSpecs=[BloomFilterAggregatorFactory{name='$bloom', fieldNames=[L_ORDERKEY], groupingSets=Noop, byRow=true, maxNumEntries=18965}]}",
            "StreamQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && BloomFilter{fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='L_ORDERKEY'}], groupingSets=Noop}), columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=(O_ORDERSTATUS=='F' && BloomFilter{fields=[DefaultDimensionSpec{dimension='O_ORDERKEY', outputName='O_ORDERKEY'}], groupingSets=Noop}), columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}"
        );
      } else {
        hook.verifyHooked(
            "F7OPEBZXEgodwaWz4bc0tg==",
            "TimeseriesQuery{dataSource='lineitem', aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "TimeseriesQuery{dataSource='lineitem', filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='$cardinality', fields=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], groupingSets=Noop, byRow=true, round=true, b=11}], postProcessing=cardinality_estimator}",
            "GroupByQuery{dataSource='CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[CommonJoin{queries=[StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}, StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}], timeColumnName=__time}, CommonJoin{queries=[StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}, StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}], timeColumnName=__time}], timeColumnName=__time}', dimensions=[DefaultDimensionSpec{dimension='S_NAME', outputName='d0'}], aggregatorSpecs=[CountAggregatorFactory{name='a0'}], limitSpec=LimitSpec{columns=[OrderByColumnSpec{dimension='a0', direction=descending}, OrderByColumnSpec{dimension='d0', direction=ascending}], limit=100}, outputColumns=[d0, a0]}",
            "StreamQuery{dataSource='lineitem', filter=MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'}, columns=[L_ORDERKEY, L_SUPPKEY]}",
            "StreamQuery{dataSource='orders', filter=O_ORDERSTATUS=='F', columns=[O_ORDERKEY], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 > 1)'}, outputColumns=[d0], $hash=true}",
            "GroupByQuery{dataSource='lineitem', dimensions=[DefaultDimensionSpec{dimension='L_ORDERKEY', outputName='d0'}], filter=(MathExprFilter{expression='(L_RECEIPTDATE > L_COMMITDATE)'} && !(L_ORDERKEY==NULL)), aggregatorSpecs=[CardinalityAggregatorFactory{name='a0:a', fields=[DefaultDimensionSpec{dimension='L_SUPPKEY', outputName='L_SUPPKEY'}], groupingSets=Noop, byRow=true, round=true, b=11}], postAggregatorSpecs=[HyperUniqueFinalizingPostAggregator{name='a0', fieldName='a0:a', round='true'}], havingSpec=ExpressionHavingSpec{expression='(a0 == 1)'}, outputColumns=[d0], $hash=true}",
            "StreamQuery{dataSource='supplier', columns=[S_NAME, S_NATIONKEY, S_SUPPKEY]}",
            "StreamQuery{dataSource='nation', filter=N_NAME=='UNITED STATES', columns=[N_NATIONKEY], $hash=true}"
        );
      }
    }
  }

  @Test
  public void tpch22() throws Exception
  {
    testQuery(
        "WITH q22_customer_tmp_cached AS ("
        + " SELECT"
        + "    C_ACCTBAL,"
        + "    C_CUSTKEY,"
        + "    SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE"
        + " FROM"
        + "    customer"
        + " WHERE"
        + "    SUBSTR(C_PHONE, 1, 2) = '13' OR"
        + "    SUBSTR(C_PHONE, 1, 2) = '31' OR"
        + "    SUBSTR(C_PHONE, 1, 2) = '23' OR"
        + "    SUBSTR(C_PHONE, 1, 2) = '29' OR"
        + "    SUBSTR(C_PHONE, 1, 2) = '30' OR"
        + "    SUBSTR(C_PHONE, 1, 2) = '18' OR"
        + "    SUBSTR(C_PHONE, 1, 2) = '17'"
        + "),"
        + "q22_customer_tmp1_cached AS ("
        + " SELECT"
        + "    AVG(C_ACCTBAL) AS AVG_ACCTBAL"
        + " FROM"
        + "    q22_customer_tmp_cached"
        + " WHERE"
        + "    C_ACCTBAL > 0.00"
        + "),"
        + "q22_orders_tmp_cached AS ("
        + " SELECT"
        + "    O_CUSTKEY"
        + " FROM"
        + "    orders"
        + " GROUP BY"
        + "    O_CUSTKEY"
        + ")"
        + "SELECT"
        + "    CNTRYCODE,"
        + "    COUNT(1) AS NUMCUST,"
        + "    SUM(C_ACCTBAL) AS TOTACCTBAL"
        + " FROM ("
        + "    SELECT"
        + "        CNTRYCODE,"
        + "        C_ACCTBAL,"
        + "        AVG_ACCTBAL"
        + "    FROM"
        + "        q22_customer_tmp1_cached CT1 CROSS JOIN ("
        + "            SELECT"
        + "                CNTRYCODE,"
        + "                C_ACCTBAL"
        + "            FROM"
        + "                q22_orders_tmp_cached OT"
        + "                RIGHT OUTER JOIN q22_customer_tmp_cached CT"
        + "                ON CT.C_CUSTKEY = OT.O_CUSTKEY"
        + "            WHERE"
        + "                O_CUSTKEY IS NULL"
        + "        ) CT2"
        + ") A"
        + " WHERE"
        + "    C_ACCTBAL > AVG_ACCTBAL"
        + " GROUP BY"
        + "    CNTRYCODE"
        + " ORDER BY"
        + "    CNTRYCODE"
        ,
        "DruidOuterQueryRel(scanFilter=[>($2, $0)], scanProject=[$1, $2], group=[{0}], NUMCUST=[COUNT()], TOTACCTBAL=[SUM($1)], sort=[$0:ASC])\n"
        + "  DruidJoinRel(joinType=[INNER])\n"
        + "    DruidQueryRel(table=[druid.customer], scanFilter=[AND(OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17')), >($0, 0.00:DECIMAL(3, 2)))], scanProject=[$0], AVG_ACCTBAL=[AVG($0)])\n"
        + "    DruidOuterQueryRel(scanFilter=[IS NULL($2)], scanProject=[$0, $1])\n"
        + "      DruidJoinRel(joinType=[RIGHT], leftKeys=[0], rightKeys=[1], outputColumns=[3, 1, 0])\n"
        + "        DruidQueryRel(table=[druid.orders], scanProject=[$2], group=[{0}])\n"
        + "        DruidQueryRel(table=[druid.customer], scanFilter=[OR(=(SUBSTR($7, 1, 2), '13'), =(SUBSTR($7, 1, 2), '31'), =(SUBSTR($7, 1, 2), '23'), =(SUBSTR($7, 1, 2), '29'), =(SUBSTR($7, 1, 2), '30'), =(SUBSTR($7, 1, 2), '18'), =(SUBSTR($7, 1, 2), '17'))], scanProject=[$0, $3, SUBSTR($7, 1, 2)])\n"
        ,
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
