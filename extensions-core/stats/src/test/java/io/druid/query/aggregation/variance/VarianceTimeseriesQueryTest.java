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

package io.druid.query.aggregation.variance;

import com.google.common.collect.Lists;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerTest;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class VarianceTimeseriesQueryTest
{
  @Parameterized.Parameters(name="{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return TimeseriesQueryRunnerTest.constructorFeeder();
  }

  private final String dataSource;
  private final boolean descending;

  public VarianceTimeseriesQueryTest(String dataSource, boolean descending)
  {
    this.dataSource = dataSource;
    this.descending = descending;
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters("bobby", null)
                                  .intervals(VarianceTestHelper.firstToThird)
                                  .aggregators(VarianceTestHelper.commonPlusVarAggregators)
                                  .postAggregators(
                                      Arrays.<PostAggregator>asList(
                                          VarianceTestHelper.addRowsIndexConstant,
                                          VarianceTestHelper.stddevOfIndexPostAggr
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            VarianceTestHelper.of(
                "rows", 13L,
                "index", 6626.151596069336,
                "addRowsIndexConstant", 6640.151596069336,
                "uniques", VarianceTestHelper.UNIQUES_9,
                "index_var", 368885.6915300076,
                "index_stddev", 607.3596064359298
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            VarianceTestHelper.of(
                "rows", 13L,
                "index", 5833.2095947265625,
                "addRowsIndexConstant", 5847.2095947265625,
                "uniques", VarianceTestHelper.UNIQUES_9,
                "index_var", 259061.60305484047,
                "index_stddev", 508.98094566971804
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  private <T> void assertExpectedResults(Iterable<Row> expectedResults, Iterable<Row> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedObjects(expectedResults, results);
  }
}
