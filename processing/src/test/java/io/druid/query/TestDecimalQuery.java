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

package io.druid.query;

import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestDecimalQuery extends TestHelper
{
  private static final TestQuerySegmentWalker segmentWalker = newWalker();

  static {
    segmentWalker.addIndex(
        "decimal",
        Arrays.asList("time", "dim", "m"),
        Arrays.asList("yyyy-MM-dd", "dimension", "decimal(32, 10)"),
        Granularities.DAY,
        "2019-08-01,a,-12637162.123123123123\n" +
        "2019-08-01,b,-23472346728343284.23423423432\n" +
        "2019-08-01,b,21312423234234.234234234234234234\n" +
        "2019-08-02,a,234234234.23423423478973289423\n" +
        "2019-08-02,a,123.123871283789123781923\n" +
        "2019-08-02,b,23847823.234823749237489234"
    );
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("decimal")
        .setDimensions(DefaultDimensionSpec.toSpec("dim"))
        .setAggregatorSpecs(
            GenericMinAggregatorFactory.of("MIN(m)", "m"),
            GenericMaxAggregatorFactory.of("MAX(m)", "m"),
            GenericSumAggregatorFactory.of("SUM(m)", "m")
        )
        .setGranularity(Granularities.YEAR)
        .build();
    String[] columnNames = {"__time", "dim", "MIN(m)", "MAX(m)", "SUM(m)"};
    Object[][] objects = {
        array("2019-01-01", "a", -12637162.1231231231, 234234234.2342342347, 221597195.2349823953),
        array("2019-01-01", "b", -23472346728343284.2342342343, 21312423234234.2342342342, -23451034281261226.7651762509)
    };
    Iterable<Row> results;
    List<Row> expectedResults;

    results = runQuery(query, segmentWalker);
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
        .setDataSource("decimal")
        .setAggregatorSpecs(
            GenericMinAggregatorFactory.of("MIN(m)", "m"),
            GenericMaxAggregatorFactory.of("MAX(m)", "m"),
            GenericSumAggregatorFactory.of("SUM(m)", "m")
        )
        .setGranularity(Granularities.YEAR)
        .build();
    String[] columnNames = {"__time", "MIN(m)", "MAX(m)", "SUM(m)"};
    Object[][] objects = {
        array("2019-01-01", -23472346728343284.2342342343, 21312423234234.2342342342, -23451034059664031.5301938556)
    };
    Iterable<Row> results;
    List<Row> expectedResults;

    results = runQuery(query, segmentWalker);
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
