/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query.aggregation.median;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class DruidTDigestGroupByQueryTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = StupidPool.heap(1024 * 1024);

    QueryConfig config = new QueryConfig();
    config.getGroupBy().setMaxResults(10000);

    final GroupByQueryEngine engine = new GroupByQueryEngine(pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(
            config, engine, pool
        ),
        pool
    );

    config = new QueryConfig();
    config.getGroupBy().setSingleThreaded(true);
    config.getGroupBy().setMaxResults(10000);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(
            config, engine, pool
        ),
        pool
    );


    return Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(factory),
                new Function<QueryRunner<Row>, Object[]>()
                {
                  @Override
                  public Object[] apply(QueryRunner<Row> input)
                  {
                    return new Object[] {factory, input};
                  }
                }
            ),
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(singleThreadFactory),
                new Function<QueryRunner<Row>, Object[]>()
                {
                  @Override
                  public Object[] apply(QueryRunner<Row> input)
                  {
                    return new Object[] {singleThreadFactory, input};
                  }
                }
            )
        )
    );
  }

  @SuppressWarnings("unchecked")
  public DruidTDigestGroupByQueryTest(GroupByQueryRunnerFactory factory, QueryRunner runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupByWithDruidTDigestAgg()
  {
    DruidTDigestAggregatorFactory aggFactory = new DruidTDigestAggregatorFactory(
        "digest",
        "index",
        50
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        Direction.DESCENDING
                    )
                ), 1
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            aggFactory
        )
        .setPostAggregatorSpecs(
            new DruidTDigestMedianPostAggregator("median", "digest")
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "median", 951.2986450195312f
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    Assert.assertEquals("T Digest median",
        expectedResults.get(0).getFloatMetric("median"), results.iterator().next().getFloatMetric("median"), 5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithSameNameComplexPostAgg()
  {
    DruidTDigestAggregatorFactory aggFactory = new DruidTDigestAggregatorFactory(
        "quantile",
        "index",
        10
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        Direction.DESCENDING
                    )
                ), 1
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            aggFactory
        )
        .setPostAggregatorSpecs(
            new DruidTDigestMedianPostAggregator("quantile", "quantile")
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "quantile", 880.9881f
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "T Digest median");
  }
}
