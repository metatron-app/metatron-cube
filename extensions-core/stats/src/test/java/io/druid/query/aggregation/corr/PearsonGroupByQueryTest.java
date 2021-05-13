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

package io.druid.query.aggregation.corr;

import com.google.common.collect.Lists;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.VirtualColumn;
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
public class PearsonGroupByQueryTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return GroupByQueryRunnerTest.constructorFeeder();
  }

  public PearsonGroupByQueryTest(GroupByQueryRunnerFactory factory, QueryRunner<Row> runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupByPearsonOnly()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setVirtualColumns(Arrays.<VirtualColumn>asList(new ExprVirtualColumn("(index-200)^2", "(index-200)^2")))
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("market", "market")))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(PearsonTestHelper.indexPearsonAggr))
        .setGranularity(Granularities.DAY)
        .build();

    PearsonTestHelper.RowBuilder builder =
        new PearsonTestHelper.RowBuilder(new String[]{"market", "index_corr"});

    List<Row> expectedResults = builder
        .add("2011-04-01", "spot", -0.9833350073834872d)
        .add("2011-04-01", "total_market", 1.0000000000000002d)
        .add("2011-04-01", "upfront", 0.9999999999999998d)
        .add("2011-04-02", "spot", -0.9888978049882882d)
        .add("2011-04-02", "total_market", 1.0d)
        .add("2011-04-02", "upfront", 1.0d)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
