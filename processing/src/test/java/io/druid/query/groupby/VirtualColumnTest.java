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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.ArrayAggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DimensionArrayAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.InDimFilter;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.KeyIndexedVirtualColumn;
import io.druid.segment.MapVirtualColumn;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.dayGran;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.makeQueryRunnerWithMerge;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class VirtualColumnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        configSupplier, mapper, engine, TestQueryRunners.pool,
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        toolChest,
        TestQueryRunners.pool
    );

    IncrementalIndex index1 = createArrayIncrementalIndex();
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    final ExecutorService executor = MoreExecutors.sameThreadExecutor();
    final List<QueryRunner<Row>> runners = Arrays.asList(
        makeQueryRunnerWithMerge(factory, executor, "index1", new IncrementalIndexSegment(index1, "index1")),
        makeQueryRunnerWithMerge(factory, executor, "index2", new QueryableIndexSegment("index2", index2))
    );
    return transformToConstructionFeeder(runners);
  }

  public static IncrementalIndex createArrayIncrementalIndex() throws IOException
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
        .withQueryGranularity(QueryGranularities.NONE)
        .withMetrics(
            new AggregatorFactory[]{
                new ArrayAggregatorFactory("array", new LongSumAggregatorFactory("array", "array"), -1),
            }
        )
        .build();
    final IncrementalIndex index = new OnheapIncrementalIndex(schema, true, 10000);

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("ts", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(Arrays.asList("dim", "keys", "values", "value")), null, null),
            "\t",
            ",",
            Arrays.asList("ts", "dim", "keys", "values", "value", "array")
        )
        , "utf8"
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\t100,200,300\t100\t100,200,300\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key2\t100,500,900\t200\t100,500,900\n" +
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\t400,500,600\t300\t400,500,600\n" +
        "2011-01-12T00:00:00.000Z\t\tkey1,key2,key3\t10,20,30\t400\t10,20,30\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key2,key3\t1,5,9\t500\t1,5,9\n" +
        "2011-01-12T00:00:00.000Z\t\tkey1,key2,key3\t2,4,8\t600\t2,4,8\n"
    );

    return TestIndex.loadIncrementalIndex(index, input, parser);
  }

  private final QueryRunner runner;

  public VirtualColumnTest(QueryRunner runner)
  {
    this.runner = runner;
  }

  private GroupByQuery.Builder testBuilder()
  {
    return GroupByQuery.builder()
                       .setDataSource(dataSource)
                       .setGranularity(dayGran)
                       .setInterval(fullOnInterval);
  }

  @Test
  public void testBasic() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "dim_nvl", "sum_of_key1", "count"},
        new Object[]{"2011-01-12T00:00:00.000Z", "a", 500L, 2L},
        new Object[]{"2011-01-12T00:00:00.000Z", "c", 101L, 2L},
        new Object[]{"2011-01-12T00:00:00.000Z", "null", 12L, 2L}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", "values", null, "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "dim_nvl")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("dim_nvl"))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", null, "cast(params.key1, 'long')", null),
                new CountAggregatorFactory("count")
            )
        )
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("dim_nvl")
        .build();
    checkSelectQuery(query, expectedResults);


    virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", null, "array", "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "dim_nvl")
    );
    query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("dim_nvl"))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", "params.key1"),
                new CountAggregatorFactory("count")
            )
        )
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("dim_nvl")
        .build();
    checkSelectQuery(query, expectedResults);
  }

  @Test
  public void testDimensionToMetric() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "sum_of_key1", "sum_of_key2", "count", "sum_array", "min_array", "max_array"},
        new Object[]{
            "2011-01-12T00:00:00.000Z", 2100L, 2100L, 6L,
            Arrays.asList(613L, 1229L, 1847L), Arrays.asList(1L, 4L, 8L), Arrays.asList(400L, 500L, 900L)
        }
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ExprVirtualColumn("cast(value, 'long')", "val_long")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec())
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", "val_long"),
                new LongSumAggregatorFactory("sum_of_key2", null, "cast(value, 'long')", null),
                new CountAggregatorFactory("count"),
                new DimensionArrayAggregatorFactory("values", new LongSumAggregatorFactory("sum_array", "values"), -1),
                new DimensionArrayAggregatorFactory("values", new LongMinAggregatorFactory("min_array", "values"), -1),
                new DimensionArrayAggregatorFactory("values", new LongMaxAggregatorFactory("max_array", "values"), -1)
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();
    checkSelectQuery(query, expectedResults);
  }

  @Test
  public void testArrayMetricAggregator() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ExprVirtualColumn("nvl(dim, 'x')", "dim_nvl")
    );

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "dim_nvl", "sum_of_array", "min_of_array", "max_of_array"},
        new Object[]{ "2011-01-12T00:00:00.000Z", "a", 2100L, 100L, 600L},
        new Object[]{ "2011-01-12T00:00:00.000Z", "c", 1515L, 1L, 900L},
        new Object[]{ "2011-01-12T00:00:00.000Z", "x", 74L, 2L, 30L}
    );

    GroupByQuery query = builder
        .setVirtualColumns(virtualColumns)
        .setDimensions(DefaultDimensionSpec.toSpec("dim_nvl"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new GenericSumAggregatorFactory("sum_of_array", "array", "array.long"),
                new GenericMinAggregatorFactory("min_of_array", "array", "array.long"),
                new GenericMaxAggregatorFactory("max_of_array", "array", "array.long")
            )
        )
        .build();
    checkSelectQuery(query, expectedResults);
  }

  @Test
  public void testX() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "keys", "cardinality1", "cardinality2"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key1", 3.0021994137521975D, 6.008806266444944D},
        new Object[]{"2011-01-12T00:00:00.000Z", "key2", 3.0021994137521975D, 6.008806266444944D},
        new Object[]{"2011-01-12T00:00:00.000Z", "key3", 2.000977198748901D, 5.006113467958146D}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ExprVirtualColumn("if (value < '300', value, 0)", "val_expr")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("keys"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new CardinalityAggregatorFactory("cardinality1", Arrays.asList("val_expr"), true),
                new CardinalityAggregatorFactory("cardinality2", Arrays.asList("value"), true)
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();
    checkSelectQuery(query, expectedResults);
  }

  @Test
  public void testIndexProvider() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "indexed", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key1", 613L, 1L, 400L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key2", 1229L, 4L, 500L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key3", 947L, 8L, 600L}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn("keys", Arrays.asList("values"), null, null, "indexed")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("indexed"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "values"),
                new LongMinAggregatorFactory("minOf", "values"),
                new LongMaxAggregatorFactory("maxOf", "values")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();

    checkSelectQuery(query, expectedResults);

    // same query on array metric
    virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn("keys", null, Arrays.asList("array"), null, "indexed")
    );
    query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("indexed"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "array"),
                new LongMinAggregatorFactory("minOf", "array"),
                new LongMaxAggregatorFactory("maxOf", "array")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();

    checkSelectQuery(query, expectedResults);

    // with filter
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "indexed", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key2", 1229L, 4L, 500L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key3", 947L, 8L, 600L}
    );

    virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn(
            "keys",
            Arrays.asList("values"),
            null,
            new InDimFilter("indexed", Arrays.asList("key2", "key3"), null),
            "indexed"
        )
    );
    query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("indexed"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "values"),
                new LongMinAggregatorFactory("minOf", "values"),
                new LongMaxAggregatorFactory("maxOf", "values")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();

    checkSelectQuery(query, expectedResults);
    virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn(
            "keys",
            null,
            Arrays.asList("array"),
            new InDimFilter("indexed", Arrays.asList("key2", "key3"), null),
            "indexed"
        )
    );
    query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("indexed"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "array"),
                new LongMinAggregatorFactory("minOf", "array"),
                new LongMaxAggregatorFactory("maxOf", "array")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();

    checkSelectQuery(query, expectedResults);
  }

  private void checkSelectQuery(GroupByQuery query, List<Row> expected) throws Exception
  {
    List<Row> results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expected, results, "");
  }
}
