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

package io.druid.query.aggregation.kurtosis;

import com.google.common.collect.Lists;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class KurtosisGroupByQueryTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return GroupByQueryRunnerTest.constructorFeeder();
  }

  public KurtosisGroupByQueryTest(GroupByQueryRunnerFactory factory, QueryRunner<Row> runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupByKurtosisOnly()
  {
    BaseAggregationQuery.Builder<GroupByQuery> query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("market", "market")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                KurtosisTestHelper.indexKurtosisAggr, QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran);

    KurtosisTestHelper.RowBuilder builder =
        new KurtosisTestHelper.RowBuilder(new String[]{"market", "index_kurtosis", "rows"});

    List<Row> expectedResults = builder
        .add("2011-04-01", "spot", 0.11476606745707718, 9L)
        .add("2011-04-01", "total_market", Double.NaN, 2L)
        .add("2011-04-01", "upfront", Double.NaN, 2L)
        .add("2011-04-02", "spot", -0.4371159794172188, 9L)
        .add("2011-04-02", "total_market", Double.NaN, 2L)
        .add("2011-04-02", "upfront", Double.NaN, 2L)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query.build());
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    expectedResults = builder
        .add("2011-04-01", "spot", -0.023823862781180427, 18L)
        .add("2011-04-01", "total_market", -0.9471818210812399, 4L)
        .add("2011-04-01", "upfront", -1.0942665533184908, 4L)
        .build();

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        runner,
        query.setGranularity(QueryGranularities.ALL).build()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
