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

package io.druid.hive;

import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.aggregation.AverageAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class HiveUDAFTest extends GroupByQueryRunnerTestHelper
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return cartesian(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public HiveUDAFTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .setDataSource(dataSource)
                                  .setQuerySegmentSpec(fullOnInterval)
                                  .setAggregatorSpecs(
                                      Arrays.asList(
                                          rowsCount,
                                          new AverageAggregatorFactory("avg", "index", null),
                                          new GenericSumAggregatorFactory("sum", "index", null),
                                          new HiveUDAFAggregatorFactory("hive.avg", Arrays.asList("index"), "avg"),
                                          new HiveUDAFAggregatorFactory("hive.sum", Arrays.asList("index"), "sum")
                                      )
                                  )
                                  .setGranularity(Granularities.MONTH)
                                  .build();

    String[] columnNames = {"__time", "rows", "avg", "sum", "hive.avg", "hive.sum"};
    Object[][] objects = {
        array("2011-01-01", 247L, 407.98625019397815, 100772.6037979126, 407.98625019397815, 100772.6037979126),
        array("2011-02-01", 364L, 430.63291683825815, 156750.38172912598, 430.63291683825815, 156750.38172912598),
        array("2011-03-01", 403L, 414.43195595753104, 167016.078250885, 414.43195595753104, 167016.078250885),
        array("2011-04-01", 195L, 404.06894030448717, 78793.443359375, 404.06894030448717, 78793.443359375)
    };
    Iterable<Row> results = runQuery(query, true);
    List<Row> expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(dataSource)
                                     .setQuerySegmentSpec(fullOnInterval)
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             rowsCount,
                                             new AverageAggregatorFactory("avg", "index", null),
                                             new GenericSumAggregatorFactory("sum", "index", null),
                                             new HiveUDAFAggregatorFactory("hive.avg", Arrays.asList("index"), "avg"),
                                             new HiveUDAFAggregatorFactory("hive.sum", Arrays.asList("index"), "sum")
                                         )
                                     )
                                     .setGranularity(Granularities.ALL)
                                     .setDimensions(DefaultDimensionSpec.of("market"))
                                     .build();

    String[] columnNames = {"__time", "market", "rows", "avg", "sum", "hive.avg", "hive.sum"};
    Object[][] objects = {
        array("1970-01-01", "spot", 837L, 114.22529548727056, 95606.57232284546, 114.22529548727056, 95606.57232284546),
        array("1970-01-01", "total_market", 186L, 1159.5689720235846, 215679.82879638672, 1159.5689720235846, 215679.82879638672),
        array("1970-01-01", "upfront", 186L, 1032.5059463336904, 192046.1060180664, 1032.5059463336904, 192046.1060180664)
    };
    Iterable<Row> results = runQuery(query, true);
    List<Row> expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
