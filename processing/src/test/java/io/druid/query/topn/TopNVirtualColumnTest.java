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

package io.druid.query.topn;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.InDimFilter;
import io.druid.query.groupby.VirtualColumnTest;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.KeyIndexedVirtualColumn;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.dayGran;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class TopNVirtualColumnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    new DefaultObjectMapper();
    TopNQueryQueryToolChest toolChest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        TestHelper.testTopNQueryEngine(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    TopNQueryRunnerFactory factory1 = new TopNQueryRunnerFactory(
        TestQueryRunners.getPool(),
        toolChest,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    TopNQueryRunnerFactory factory2 = new TopNQueryRunnerFactory(
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(100);
              }
            }
        ),
        toolChest,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    IncrementalIndex index1 = VirtualColumnTest.createArrayIncrementalIndex();
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    final List<QueryRunner<Result<TopNResultValue>>> runners = Arrays.asList(
        QueryRunnerTestHelper.makeQueryRunner(factory1, "index1", new IncrementalIndexSegment(index1, "index1")),
        QueryRunnerTestHelper.makeQueryRunner(factory1, "index2", new QueryableIndexSegment("index2", index2)),
        QueryRunnerTestHelper.makeQueryRunner(factory2, "index1", new IncrementalIndexSegment(index1, "index1")),
        QueryRunnerTestHelper.makeQueryRunner(factory2, "index2", new QueryableIndexSegment("index2", index2))
    );

    return transformToConstructionFeeder(runners);
  }

  private final QueryRunner<Result<TopNResultValue>> runner;

  public TopNVirtualColumnTest(QueryRunner<Result<TopNResultValue>> runner)
  {
    this.runner = runner;
  }

  private TopNQueryBuilder testBuilder()
  {
    return new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(dayGran)
        .intervals(fullOnInterval);
  }

  @Test
  public void testIndexedDimension()
  {
    TopNQueryBuilder builder = testBuilder()
        .virtualColumn(new KeyIndexedVirtualColumn("keys", Arrays.asList("values"), null, null, "indexed"))
        .dimension("indexed")
        .metric(new NumericTopNMetricSpec("sumOf"))
        .threshold(4)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "values"),
                new LongMinAggregatorFactory("minOf", "values"),
                new LongMaxAggregatorFactory("maxOf", "values")
            )
        );

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key2", "sumOf", 1229L, "minOf", 4L, "maxOf", 500L),
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L),
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L)
                )
            )
        )
    );

    builder.metric(new NumericTopNMetricSpec("sumOf"));

    List result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);

    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L),
                    ImmutableMap.<String, Object>of("indexed", "key2", "sumOf", 1229L, "minOf", 4L, "maxOf", 500L),
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L)
                )
            )
        )
    );
    builder.metric(new NumericTopNMetricSpec("maxOf"));

    result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);

    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L),
                    ImmutableMap.<String, Object>of("indexed", "key2", "sumOf", 1229L, "minOf", 4L, "maxOf", 500L),
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L)
                )
            )
        )
    );
    builder.metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("minOf")));

    result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);

    // with filter
    builder.virtualColumn(
        new KeyIndexedVirtualColumn(
            "keys", Arrays.asList("values"), null,
            new InDimFilter("indexed", Arrays.asList("key1", "key3"), null), "indexed"
        )
    );
    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L),
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L)
                )
            )
        )
    );
    result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);
  }


  @Test
  public void testIndexedMetric()
  {
    TopNQueryBuilder builder = testBuilder()
        .virtualColumn(new KeyIndexedVirtualColumn("keys", null, Arrays.asList("array"), null, "indexed"))
        .dimension("indexed")
        .metric(new NumericTopNMetricSpec("sumOf"))
        .threshold(4)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "array"),
                new LongMinAggregatorFactory("minOf", "array"),
                new LongMaxAggregatorFactory("maxOf", "array")
            )
        );

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key2", "sumOf", 1229L, "minOf", 4L, "maxOf", 500L),
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L),
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L)
                )
            )
        )
    );

    builder.metric(new NumericTopNMetricSpec("sumOf"));

    List result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);

    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L),
                    ImmutableMap.<String, Object>of("indexed", "key2", "sumOf", 1229L, "minOf", 4L, "maxOf", 500L),
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L)
                )
            )
        )
    );
    builder.metric(new NumericTopNMetricSpec("maxOf"));

    result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);

    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L),
                    ImmutableMap.<String, Object>of("indexed", "key2", "sumOf", 1229L, "minOf", 4L, "maxOf", 500L),
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L)
                )
            )
        )
    );
    builder.metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("minOf")));

    result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);

    // with filter
    builder.virtualColumn(
        new KeyIndexedVirtualColumn(
            "keys", null, Arrays.asList("array"),
            new InDimFilter("indexed", Arrays.asList("key1", "key3"), null), "indexed"
        )
    );
    expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of("indexed", "key1", "sumOf", 613L, "minOf", 1L, "maxOf", 400L),
                    ImmutableMap.<String, Object>of("indexed", "key3", "sumOf", 947L, "minOf", 8L, "maxOf", 600L)
                )
            )
        )
    );
    result = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.<String, Object>of()),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, result);
  }
}
