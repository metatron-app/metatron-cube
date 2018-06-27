/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.join;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.CharSource;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.math.expr.Parser;
import io.druid.query.DataSource;
import io.druid.query.JoinElement;
import io.druid.query.JoinQuery;
import io.druid.query.JoinType;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class JoinQueryRunnerTest extends QueryRunnerTestHelper
{
  static final String JOIN_DS = "join_test";

  static {
    Parser.register(ModuleBuiltinFunctions.class);

    if (!TestIndex.segmentWalker.contains(JOIN_DS)) {
      AggregatorFactory metric = new GenericSumAggregatorFactory("value", "value", "long");
      DimensionsSpec dimensions = new DimensionsSpec(
          StringDimensionSchema.ofNames("market", "market_month"), null, null
      );
      IncrementalIndexSchema schema = TestIndex.SAMPLE_SCHEMA
          .withMinTimestamp(new DateTime("2011-01-01T00:00:00.000Z").getMillis())
          .withDimensionsSpec(dimensions)
          .withMetrics(metric)
          .withRollup(false);

      DataSegment segment = new DataSegment(
          JOIN_DS,
          TestIndex.INTERVAL,
          "0",
          null,
          Arrays.asList("market", "market_month"),
          Arrays.asList("value"),
          null,
          null,
          0
      );
      StringInputRowParser parser = new StringInputRowParser(
          new DelimitedParseSpec(
              new DefaultTimestampSpec("ts", "iso", null),
              dimensions,
              "\t",
              "\u0001",
              Arrays.asList("ts", "market", "market_month", "value")
          )
          , "utf8"
      );
      CharSource source = TestIndex.asCharSource("druid.sample.join.tsv");
      TestIndex.segmentWalker.add(segment, TestIndex.makeRealtimeIndex(source, schema, parser));
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public JoinQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testJoin()
  {
    JoinQuery joinQuery = new JoinQuery(
        ImmutableMap.<String, DataSource>of(
            dataSource, ViewDataSource.of(dataSource, "__time", "market", "quality", "index"),
            JOIN_DS, ViewDataSource.of(JOIN_DS)
        ),
        Arrays.asList(new JoinElement(JoinType.INNER, dataSource + ".market = " + JOIN_DS + ".market")),
        false,
        firstToThird, 0, 0, 0, 0, 0, null
    );

    String[] columns = new String[]{"__time", "market", "index", "market_month", "value"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns, array("2011-04-01T00:00:00.000Z", "spot", 135.88510131835938, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 118.57034301757812, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 158.74722290039062, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 120.13470458984375, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 109.70581817626953, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 121.58358001708984, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 144.5073699951172, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 78.62254333496094, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 119.92274475097656, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 147.42593383789062, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 112.98703002929688, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 166.01605224609375, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 113.44600677490234, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 110.93193054199219, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 114.2901382446289, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 135.30149841308594, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 97.38743591308594, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "spot", 126.41136169433594, "april_spot", 41111L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1314.8397216796875, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1522.043701171875, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1193.5562744140625, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "total_market", 1321.375, "april_total_market", 41112L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1447.3411865234375, "april_upfront", 41113L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1234.24755859375, "april_upfront", 41113L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1144.3424072265625, "april_upfront", 41113L),
        array("2011-04-01T00:00:00.000Z", "upfront", 1049.738525390625, "april_upfront", 41113L)
    );

    Iterable<Row> rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow());
    for (Object x : rows) {
      System.out.println(x);
    }
    TestHelper.assertExpectedObjects(expectedRows, rows, "");

    // prefixed
    rows = Iterables.transform(runTabularQuery(joinQuery.withPrefixAlias(true)), Rows.mapToRow());
    columns = new String[]{
        dataSource + ".market", dataSource + ".__time", dataSource + ".index",
        JOIN_DS + ".market", JOIN_DS + ".__time", JOIN_DS + ".market_month", JOIN_DS + ".value"
    };
    GroupByQueryRunnerTestHelper.printToExpected(columns, rows);
    expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("spot", 1301616000000L, 135.88510131835938, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 118.57034301757812, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 158.74722290039062, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 120.13470458984375, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 109.70581817626953, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 121.58358001708984, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 144.5073699951172, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 78.62254333496094, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301616000000L, 119.92274475097656, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 147.42593383789062, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 112.98703002929688, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 166.01605224609375, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 113.44600677490234, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 110.93193054199219, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 114.2901382446289, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 135.30149841308594, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 97.38743591308594, "spot", 1301616000000L, "april_spot", 41111L),
        array("spot", 1301702400000L, 126.41136169433594, "spot", 1301616000000L, "april_spot", 41111L),
        array("total_market", 1301616000000L, 1314.8397216796875, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("total_market", 1301616000000L, 1522.043701171875, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("total_market", 1301702400000L, 1193.5562744140625, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("total_market", 1301702400000L, 1321.375, "total_market", 1301616000000L, "april_total_market", 41112L),
        array("upfront", 1301616000000L, 1447.3411865234375, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("upfront", 1301616000000L, 1234.24755859375, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("upfront", 1301702400000L, 1144.3424072265625, "upfront", 1301616000000L, "april_upfront", 41113L),
        array("upfront", 1301702400000L, 1049.738525390625, "upfront", 1301616000000L, "april_upfront", 41113L)
    );
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }
}