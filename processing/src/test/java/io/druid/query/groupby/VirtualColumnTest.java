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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseAggregationQuery;
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
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExpressionExtractionFn;
import io.druid.query.filter.InDimFilter;
import io.druid.segment.ArrayVirtualColumn;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.KeyIndexedVirtualColumn;
import io.druid.segment.LateralViewVirtualColumn;
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
        configSupplier, engine, TestQueryRunners.pool,
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
                new LongSumAggregatorFactory("m1"),
                new LongSumAggregatorFactory("m2"),
                new LongSumAggregatorFactory("m3")
            }
        )
        .build();
    final IncrementalIndex index = new OnheapIncrementalIndex(schema, true, 10000);

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec("ts", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(Arrays.asList("dim", "keys", "values", "value")), null, null
            ),
            "\t",
            ",",
            Arrays.asList("ts", "dim", "keys", "values", "value", "array", "m1", "m2", "m3")
        )
        , "utf8"
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\t100,200,300\t100\t100,200,300\t100\t200\t300\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key2\t100,500,900\t200\t100,500,900\t100\t500\t900\n" +
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\t400,500,600\t300\t400,500,600\t400\t500\t600\n" +
        "2011-01-12T01:00:00.000Z\t\tkey1,key2,key3\t10,20,30\t400\t10,20,30\t10\t20\t30\n" +
        "2011-01-12T01:00:00.000Z\tc\tkey1,key2,key3\t1,5,9\t500\t1,5,9\t1\t5\t9\n" +
        "2011-01-12T01:00:00.000Z\t\tkey1,key2,key3\t2,4,8\t600\t2,4,8\t2\t4\t8\n"
    );

    return TestIndex.loadIncrementalIndex(index, input, parser);
  }

  private final QueryRunner<Row> runner;

  public VirtualColumnTest(QueryRunner<Row> runner)
  {
    this.runner = runner;
  }

  private BaseAggregationQuery.Builder<GroupByQuery> testBuilder()
  {
    return GroupByQuery.builder()
                       .setDataSource(dataSource)
                       .setGranularity(dayGran)
                       .setInterval(fullOnInterval);
  }

  @Test
  public void testBasic() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "a.dim_nvl", "sum_of_key1", "count"},
        new Object[]{"2011-01-12T00:00:00.000Z", "a", 500L, 2L},
        new Object[]{"2011-01-12T00:00:00.000Z", "c", 101L, 2L},
        new Object[]{"2011-01-12T00:00:00.000Z", "null", 12L, 2L}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", "values", null, "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "a.dim_nvl")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("a.dim_nvl"))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", null, "cast(params.key1, 'long')", null),
                new CountAggregatorFactory("count")
            )
        )
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("a.dim_nvl")
        .build();
    checkQueryResult(query, expectedResults);


    virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", null, "array", "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "a.dim_nvl")
    );
    query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("a.dim_nvl"))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", "params.key1"),
                new CountAggregatorFactory("count")
            )
        )
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("a.dim_nvl")
        .build();
    checkQueryResult(query, expectedResults);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException1() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", null, "array", "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "a.dim_nvl")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("params"))
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("a.dim_nvl")
        .build();

    Sequences.toList(runner.run(query, ImmutableMap.<String, Object>of()), Lists.<Row>newArrayList());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException2() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", null, "array", "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "a.dim_nvl")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("params.key1"))
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("a.dim_nvl")
        .build();

    Sequences.toList(runner.run(query, ImmutableMap.<String, Object>of()), Lists.<Row>newArrayList());
  }

  @Test
  public void testDimensionToMetric() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

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
    checkQueryResult(query, expectedResults);
  }

  @Test
  public void testArrayMetricAggregator() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ExprVirtualColumn("nvl(dim, 'x')", "dim_nvl")
    );

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "dim_nvl", "sum_of_array", "min_of_array", "max_of_array"},
        new Object[]{"2011-01-12T00:00:00.000Z", "a", 2100L, 100L, 600L},
        new Object[]{"2011-01-12T00:00:00.000Z", "c", 1515L, 1L, 900L},
        new Object[]{"2011-01-12T00:00:00.000Z", "x", 74L, 2L, 30L}
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
    checkQueryResult(query, expectedResults);
  }

  @Test
  public void testX() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

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
    checkQueryResult(query, expectedResults);
  }

  @Test
  public void testArrayVC() throws Exception
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    // implicit
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "dim", "sumOfArray0", "sumOfArray1", "sumOfArray2"},
        new Object[]{"2011-01-12T00:00:00.000Z", null, 12L, 24L, 38L},
        new Object[]{"2011-01-12T00:00:00.000Z", "a", 500L, 700L, 900L},
        new Object[]{"2011-01-12T00:00:00.000Z", "c", 101L, 505L, 909L}
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("dim"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOfArray0", "array.0"),
                new LongSumAggregatorFactory("sumOfArray1", "array.1"),
                new LongSumAggregatorFactory("sumOfArray2", "array.2")
            )
        )
        .build();

    checkQueryResult(query, expectedResults);

    // explicit
    query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("dim"))
        .setVirtualColumns(Arrays.<VirtualColumn>asList(new ArrayVirtualColumn("array", "access")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOfArray0", "access.0"),
                new LongSumAggregatorFactory("sumOfArray1", "access.1"),
                new LongSumAggregatorFactory("sumOfArray2", "access.2")
            )
        )
        .build();

    checkQueryResult(query, expectedResults);
  }

  @Test
  public void testIndexProvider() throws Exception
  {
    // key1 key2 key3
    // 100  200  300
    // 400  500  600
    // 100  500 (900)
    //  10   20   30
    //   2    4    8
    //   1    5    9
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "indexed", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key1",  613L, 1L, 400L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key2", 1229L, 4L, 500L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key3",  947L, 8L, 600L}
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

    checkQueryResult(query, expectedResults);

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

    checkQueryResult(query, expectedResults);

    // expression
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "indexed", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key1", 1226L,  6L, 1000L}, //  613+613(m1), 2+4(m2), 400+600(m3)
        new Object[]{"2011-01-12T00:00:00.000Z", "key2", 1842L,  8L, 1400L}, // 1229+613(m1), 4+4(m2), 500+900(m3)
        new Object[]{"2011-01-12T00:00:00.000Z", "key3", 1460L, 12L, 1200L}  //  947+513(m1), 8+4(m2), 600+600(m3)
    );

    // very confusing..
    //  k1  k2  k3     sum(+m1)        min(+m2)        max(+m3)
    // 100 200 300   200 300  400   300  400  500    400  500  600
    // 400 500 600   800 900 1000   900 1000 1100   1000 1100 1200
    // 100 500       200 600        600 1000        1000 1400
    //  10  20  30    20  30   40    30   40   50     40   50   60
    //   2   4   8     4   6   10     6    8   12     10   12   16
    //   1   5   9     2   6   10     6   10   14     10   14   18
    query = builder
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", null, "array + m1", null),
                new LongMinAggregatorFactory("minOf", null, "array + m2"),
                new LongMaxAggregatorFactory("maxOf", null, "array + m3")
            )
        )
        .build();

    checkQueryResult(query, expectedResults);

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

    checkQueryResult(query, expectedResults);

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

    checkQueryResult(query, expectedResults);

    // with null filter
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "indexed", "sumOf", "minOf", "maxOf"}
    );

    virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn(
            "keys",
            Arrays.asList("values"),
            null,
            new InDimFilter("indexed", Arrays.asList("not", "existing"), null),
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

    checkQueryResult(query, expectedResults);

    virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn(
            "keys",
            null,
            Arrays.asList("array"),
            new InDimFilter("indexed", Arrays.asList("not", "existing"), null),
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

    checkQueryResult(query, expectedResults);
  }

  @Test
  public void testWithExtractFn() throws Exception
  {
    // key1 key2 key3
    // 100  200  300
    // 400  500  600
    // 100  500 (900)
    //  10   20   30
    //   2    4    8
    //   1    5    9
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "indexed", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key10", 613L, 1L, 400L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key20", 1229L, 4L, 500L},
        new Object[]{"2011-01-12T00:00:00.000Z", "key30", 947L, 8L, 600L}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new KeyIndexedVirtualColumn("keys", Arrays.asList("values"), null, null, "indexed")
    );
    GroupByQuery query = builder
        .setDimensions(DimensionSpecs.of("indexed", new ExpressionExtractionFn("indexed + '0'")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "values"),
                new LongMinAggregatorFactory("minOf", "values"),
                new LongMaxAggregatorFactory("maxOf", "values")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();

    checkQueryResult(query, expectedResults);
  }

  @Test
  public void testLateralView() throws Exception
  {
    //  m1  m2  m3
    // 100 200 300
    // 400 500 600
    // 100 500 900
    //  10  20  30
    //   2   4   8
    //   1   5   9
    BaseAggregationQuery.Builder<GroupByQuery> builder = testBuilder();

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new LateralViewVirtualColumn("LV", "M", null, Arrays.asList("m1", "m2", "m3"))
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("LV"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", "M"),
                new LongMinAggregatorFactory("minOf", "M"),
                new LongMaxAggregatorFactory("maxOf", "M")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "LV", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "m1",  613L, 1L, 400L},  // 100:100:400:10:1:2
        new Object[]{"2011-01-12T00:00:00.000Z", "m2", 1229L, 4L, 500L},  // 200:500:500:20:5:4
        new Object[]{"2011-01-12T00:00:00.000Z", "m3", 1847L, 8L, 900L}   // 300:900:600:30:9:8
    );

    checkQueryResult(query, expectedResults);

    // hour
    query = builder.setGranularity(QueryGranularities.HOUR).build();

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "LV", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "m1",  600L, 100L, 400L},  // 100:100:400
        new Object[]{"2011-01-12T00:00:00.000Z", "m2", 1200L, 200L, 500L},  // 200:500:500
        new Object[]{"2011-01-12T00:00:00.000Z", "m3", 1800L, 300L, 900L},  // 300:900:600
        new Object[]{"2011-01-12T01:00:00.000Z", "m1",   13L,   1L,  10L},  // 10:1:2
        new Object[]{"2011-01-12T01:00:00.000Z", "m2",   29L,   4L,  20L},  // 20:5:4
        new Object[]{"2011-01-12T01:00:00.000Z", "m3",   47L,   8L,  30L}   // 30:9:8
    );

    checkQueryResult(query, expectedResults);

    // expression
    query = builder
        .setGranularity(QueryGranularities.DAY)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("sumOf", null, "if(M >= 100, M, M * 2)", null),
                new LongMinAggregatorFactory("minOf", null, "if(M >= 100, M, M * 2)"),
                new LongMaxAggregatorFactory("maxOf", null, "if(M >= 100, M, M * 2)")
            )
        ).build();

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "LV", "sumOf", "minOf", "maxOf"},
        new Object[]{"2011-01-12T00:00:00.000Z", "m1", 626L,   2L, 400L},  // 100:100:400:10:1:2
        new Object[]{"2011-01-12T00:00:00.000Z", "m2", 1258L,  8L, 500L},  // 200:500:500:20:5:4
        new Object[]{"2011-01-12T00:00:00.000Z", "m3", 1894L, 16L, 900L}   // 300:900:600:30:9:8
    );

    checkQueryResult(query, expectedResults);
  }

  private void checkQueryResult(GroupByQuery query, List<Row> expected) throws Exception
  {
    List<Row> results = Sequences.toList(
        runner.run(query, ImmutableMap.<String, Object>of()),
        Lists.<Row>newArrayList()
    );
    for(Object x : results) {
      System.out.println(x);
    }
    TestHelper.assertExpectedObjects(expected, results, "");
  }
}
