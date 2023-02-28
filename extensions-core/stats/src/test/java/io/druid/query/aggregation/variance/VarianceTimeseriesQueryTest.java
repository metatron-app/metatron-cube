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

import io.druid.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class VarianceTimeseriesQueryTest extends GroupByQueryRunnerTestHelper
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

    if (descending) {
      validate(
          query,
          array("__time", "rows", "index", "addRowsIndexConstant", "uniques", "index_var", "index_stddev"),
          array("2011-04-02", 13, 5833.2095947265625D, 5847.2095947265625D, 9.019833517963864D, 259061.6030548405D, 508.9809456697181D),
          array("2011-04-01", 13, 6626.151596069336D, 6640.151596069336D, 9.019833517963864D, 368885.6915300076D, 607.3596064359298D)
      );
    } else {
      validate(
          query,
          array("__time", "rows", "index", "addRowsIndexConstant", "uniques", "index_var", "index_stddev"),
          array("2011-04-01", 13, 6626.151596069336D, 6640.151596069336D, 9.019833517963864D, 368885.6915300076D, 607.3596064359298D),
          array("2011-04-02", 13, 5833.2095947265625D, 5847.2095947265625D, 9.019833517963864D, 259061.60305484047D, 508.98094566971804D)
      );
    }
  }
}
