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

package io.druid.query.topn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.Granularities;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryRunners;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopNUnionQueryTest
{
  private final TopNQueryQueryToolChest toolchest = new TopNQueryQueryToolChest(
      new TopNQueryConfig(), TestHelper.testTopNQueryEngine()
  );
  private final TopNQueryRunnerFactory factory1 = new TopNQueryRunnerFactory(
      TestQueryRunners.getPool(), toolchest, TestHelper.NOOP_QUERYWATCHER
  );
  private final TopNQueryRunnerFactory factory2 = new TopNQueryRunnerFactory(
      StupidPool.heap(2000), toolchest, TestHelper.NOOP_QUERYWATCHER
  );

  @Test
  public void testTopNUnionQuery()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.unionDataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            GuavaUtils.concat(
                QueryRunnerTestHelper.commonAggregators,
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
        .postAggregators(
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAgg
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 744L)
                                .put("index", 862719.3151855469D)
                                .put("addRowsIndexConstant", 863464.3151855469D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 864209.3151855469D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 744L)
                                .put("index", 768184.4240722656D)
                                .put("addRowsIndexConstant", 768929.4240722656D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 769674.4240722656D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
                                .put("rows", 3348L)
                                .put("index", 382426.28929138184D)
                                .put("addRowsIndexConstant", 385775.28929138184D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 389124.28929138184D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_9 + 1.0
                                )
                                .put("maxIndex", 277.2735290527344D)
                                .put("minIndex", 59.02102279663086D)
                                .build()
                )
            )
        )
    );
    for (QueryRunner<Result<TopNResultValue>> runner : Iterables.concat(
        QueryRunnerTestHelper.makeUnionQueryRunners(query, factory1, QueryRunnerTestHelper.unionDataSource),
        QueryRunnerTestHelper.makeUnionQueryRunners(query, factory2, QueryRunnerTestHelper.unionDataSource)
    )) {
      HashMap<String,Object> context = new HashMap<String, Object>();
      TestHelper.assertExpectedResults(expectedResults, QueryRunners.run(query, runner));
    }
  }
}
