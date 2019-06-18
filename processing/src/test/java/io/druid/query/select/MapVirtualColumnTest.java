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

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.ArrayAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExpressionExtractionFn;
import io.druid.query.extraction.UpperExtractionFn;
import io.druid.segment.ArrayVirtualColumn;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.MapVirtualColumn;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.druid.query.QueryRunnerTestHelper.allGran;
import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.makeQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class MapVirtualColumnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    SelectQueryRunnerFactory factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            new DefaultObjectMapper(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new SelectQueryEngine(),
        new SelectQueryConfig(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
        .withQueryGranularity(QueryGranularities.NONE)
        .withMetrics(
            new AggregatorFactory[]{
                new ArrayAggregatorFactory("array", new LongSumAggregatorFactory("array", "array"), -1)
            }
        )
        .build();
    final IncrementalIndex index = new OnheapIncrementalIndex(schema, true, 10000);

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec("ts", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("dim", "keys", "values")), null, null),
            "\t",
            ",",
            Arrays.asList("ts", "dim", "keys", "values", "array")
        )
        , "utf8"
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\tvalue1,value2,value3\t100,200,300\n" +
        "2011-01-12T00:00:00.001Z\t\tkey4,key5,key6\tvalue4\t100,500,900\n" +
        "2011-01-12T00:00:00.002Z\tc\tkey1,key5\tvalue1,value5,value9\t400,500,600\n"
    );

    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(index, input, parser);
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    return transformToConstructionFeeder(
        Arrays.asList(
            makeQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1")),
            makeQueryRunner(factory, "index2", new QueryableIndexSegment("index2", index2))
        )
    );
  }

  private final QueryRunner runner;

  public MapVirtualColumnTest(QueryRunner runner)
  {
    this.runner = runner;
  }

  private Druids.SelectQueryBuilder testBuilder()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(allGran)
                 .intervals(fullOnInterval)
                 .pagingSpec(new PagingSpec(null, 3));
  }

  @Test
  public void testBasic() throws Exception
  {
    Druids.SelectQueryBuilder builder = testBuilder();

    List<Map> expectedResults = Arrays.<Map>asList(
        mapOf(
            "dim", "a",
            "dim_nvl", "a",
            "params.key1", "value1",
            "params.key3", "value3",
            "params.key5", null,
            "params", mapOf("key1", "value1", "key2", "value2", "key3", "value3")
        ),
        mapOf(
            "dim", null,
            "dim_nvl", "null",
            "params.key1", null,
            "params.key3", null,
            "params.key5", null,
            "params", mapOf("key4", "value4")
        ),
        mapOf(
            "dim", "c",
            "dim_nvl", "c",
            "params.key1", "value1",
            "params.key3", null,
            "params.key5", "value5",
            "params", mapOf("key1", "value1", "key5", "value5")
        )
    );
    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", "values", null, "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "dim_nvl")
    );
    SelectQuery selectQuery = builder.dimensions(Arrays.asList("dim", "params.key1"))
                                     .metrics(Arrays.asList("params.key3", "params.key5", "params", "dim_nvl"))
                                     .virtualColumns(virtualColumns)
                                     .build();
    checkSelectQuery(selectQuery, expectedResults);
  }

  @Test
  public void testWithExtractFn() throws Exception
  {
    Druids.SelectQueryBuilder builder = testBuilder();

    List<Map> expectedResults = Arrays.<Map>asList(
        mapOf("params.key1", "VALUE1", "dim_nvl", "value1-a"),
        mapOf("params.key1", null, "dim_nvl", "null-a"),
        mapOf("params.key1", "VALUE1", "dim_nvl", "value1-a")
    );
    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", "values", null, "params"),
        new ExprVirtualColumn("nvl(params.key1, 'null')", "dim_nvl")
    );
    SelectQuery selectQuery = builder
        .dimensionSpecs(
            DimensionSpecs.of("params.key1", new UpperExtractionFn("en")),
            DimensionSpecs.of("dim_nvl", new ExpressionExtractionFn("dim_nvl + '-a'"))
        )
        .virtualColumns(virtualColumns)
        .context(ImmutableMap.<String, Object>of(QueryContextKeys.ALL_METRICS_FOR_EMPTY, false))
        .build();
    checkSelectQuery(selectQuery, expectedResults);
  }

  @Test(expected = UnsupportedOperationException.class)
  @Ignore("now index vc is used automatically for map vc")
  public void testException1() throws Exception
  {
    Druids.SelectQueryBuilder builder = testBuilder();
    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", "values", null, "params")
    );
    // cannot use map type as dimension
    SelectQuery selectQuery = builder.dimensions(Arrays.asList("params"))
                                     .virtualColumns(virtualColumns)
                                     .build();
    System.out.println(
        Sequences.toList(
            runner.run(selectQuery, ImmutableMap.of()),
            Lists.<Result<SelectResultValue>>newArrayList()
        )
    );
  }

  @Ignore("supported")
  @Test(expected = UnsupportedOperationException.class)
  public void testException2() throws Exception
  {
    Druids.SelectQueryBuilder builder = testBuilder();
    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", null, "array", "params")
    );
    // cannot use non-string type as dimension
    SelectQuery selectQuery = builder.dimensions(Arrays.asList("params.key1"))
                                     .virtualColumns(virtualColumns)
                                     .build();
    System.out.println(
        Sequences.toList(
            runner.run(selectQuery, ImmutableMap.of()),
            Lists.<Result<SelectResultValue>>newArrayList()
        )
    );
  }

  @Test
  public void testArrayVC() throws Exception
  {
    List<Map> expectedResults = Arrays.<Map>asList(
        mapOf(
            "dim", "a",
            "access", Arrays.asList(100L, 200L, 300L),
            "access.0", 100L,
            "access.1", 200L,
            "access.2", 300L
        ),
        mapOf(
            "dim", null,
            "access", Arrays.asList(100L, 500L, 900L),
            "access.0", 100L,
            "access.1", 500L,
            "access.2", 900L
        ),
        mapOf(
            "dim", "c",
            "access", Arrays.asList(400L, 500L, 600L),
            "access.0", 400L,
            "access.1", 500L,
            "access.2", 600L
        )
    );
    // access via explicit vc
    Druids.SelectQueryBuilder builder = testBuilder();
    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ArrayVirtualColumn("array", "access")
    );
    SelectQuery selectQuery = builder.dimensions(Arrays.asList("dim"))
                                     .metrics(Arrays.asList("access", "access.0", "access.1", "access.2"))
                                     .virtualColumns(virtualColumns)
                                     .build();
    checkSelectQuery(selectQuery, expectedResults);

    // access via implicit vc
    expectedResults = Arrays.<Map>asList(
        mapOf(
            "dim", "a",
            "array", Arrays.asList(100L, 200L, 300L),
            "array.0", 100L,
            "array.1", 200L,
            "array.2", 300L
        ),
        mapOf(
            "dim", null,
            "array", Arrays.asList(100L, 500L, 900L),
            "array.0", 100L,
            "array.1", 500L,
            "array.2", 900L
        ),
        mapOf(
            "dim", "c",
            "array", Arrays.asList(400L, 500L, 600L),
            "array.0", 400L,
            "array.1", 500L,
            "array.2", 600L
        )
    );

    selectQuery = builder.dimensions(Arrays.asList("dim"))
                         .metrics(Arrays.asList("array", "array.0", "array.1", "array.2"))
                         .build();
    checkSelectQuery(selectQuery, expectedResults);
  }

  private Map mapOf(Object... elements)
  {
    Map map = Maps.newHashMap();
    for (int i = 0; i < elements.length; i += 2) {
      map.put(elements[i], elements[i + 1]);
    }
    return map;
  }

  private void checkSelectQuery(SelectQuery searchQuery, List<Map> expected) throws Exception
  {
    List<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(searchQuery, ImmutableMap.of()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());

    List<EventHolder> events = results.get(0).getValue().getEvents();

    Assert.assertEquals(expected.size(), events.size());
    for (int i = 0; i < events.size(); i++) {
      Map event = events.get(i).getEvent();
      event.remove(EventHolder.timestampKey);
      Assert.assertEquals(expected.get(i), event);
    }
  }
}
