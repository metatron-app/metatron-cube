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

package io.druid.query.aggregation.variance;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.utils.Sequences;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.ordering.Direction;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryRunnerTest;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class VarianceTopNQueryTest
{
  @Parameterized.Parameters(name = "{0}:{1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return TopNQueryRunnerTest.constructorFeeder();
  }

  private final String dataSource;
  private final Direction direction;

  public VarianceTopNQueryTest(String dataSource, Direction direction)
  {
    this.dataSource = dataSource;
    this.direction = direction;
  }

  @Test
  public void testFullOnTopNOverUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec(QueryRunnerTestHelper.uniqueMetric, direction))
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    VarianceTestHelper.commonPlusVarAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    // todo why 'index_var' is chaged?
    boolean mmaped = TestIndex.MMAPPED_SPLIT.equals(dataSource);
    Map<String, Object> row1 = ImmutableMap.<String, Object>builder()
        .put("market", "spot")
        .put("rows", 837L)
        .put("index", 95606.57232284546D)
        .put("addRowsIndexConstant", 96444.57232284546D)
        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
        .put("maxIndex", 277.2735290527344D)
        .put("minIndex", 59.02102279663086D)
        .put("index_var", !mmaped ? 439.3851636380398D : 439.3851636380395D)
        .build();
    Map<String, Object> row2 = ImmutableMap.<String, Object>builder()
        .put("market", "total_market")
        .put("rows", 186L)
        .put("index", 215679.82879638672D)
        .put("addRowsIndexConstant", 215866.82879638672D)
        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
        .put("maxIndex", 1743.9217529296875D)
        .put("minIndex", 792.3260498046875D)
        .put("index_var", !mmaped ? 27679.900640856464D : 27679.900640856475D)
        .build();
    Map<String, Object> row3 = ImmutableMap.<String, Object>builder()
        .put("market", "upfront")
        .put("rows", 186L)
        .put("index", 192046.1060180664D)
        .put("addRowsIndexConstant", 192233.1060180664D)
        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
        .put("maxIndex", 1870.06103515625D)
        .put("minIndex", 545.9906005859375D)
        .put("index_var", 79699.97748853161D)
        .build();

    TopNResultValue value;
    if (direction == Direction.DESCENDING) {
      value = new TopNResultValue(Arrays.<Map<String, Object>>asList(row1, row2, row3));
    } else {
      value = new TopNResultValue(Arrays.<Map<String, Object>>asList(row2, row3, row1));
    }
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            value
        )
    );
    List<Result<TopNResultValue>> retval = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of())
    );
    TestHelper.assertExpectedResults(expectedResults, retval);
  }
}
