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
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.LateralViewSpec;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.druid.query.QueryRunnerTestHelper.makeQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class GroupByQueryExplodeTest
{
  private static final String[] V_0401 = {
      "2011-04-01T00:00:00.000Z	x1	10	50	90	130	170	210	250",
      "2011-04-01T01:00:00.000Z	x2	20	60	100	140	180	220	260",
      "2011-04-01T02:00:00.000Z	x3	30	70	110	150	190	230	270",
      "2011-04-01T03:00:00.000Z	x4	40	80	120	160	200	240	280"
  };

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
    final GroupByQueryEngine engine = new GroupByQueryEngine(pool);

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

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-04-01T00:00:00.000Z").getMillis())
        .withQueryGranularity(QueryGranularities.NONE)
        .withMetrics(
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("spot$automotive", "a"),
                new LongSumAggregatorFactory("spot$mezzanine", "b"),
                new LongSumAggregatorFactory("spot$premium", "c"),
                new LongSumAggregatorFactory("total_market$mezzanine", "d"),
                new LongSumAggregatorFactory("total_market$premium", "e"),
                new LongSumAggregatorFactory("upfront$mezzanine", "f"),
                new LongSumAggregatorFactory("upfront$premium", "g")
            }
        )
        .build();
    final IncrementalIndex index = new OnheapIncrementalIndex(schema, true, 10000);

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec("ts", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("x")), null, null),
            "\t",
            ",",
            Arrays.asList("ts", "x", "a", "b", "c", "d", "e", "f", "g")
        )
        , "utf8"
    );

    CharSource v_401 = CharSource.wrap(StringUtils.join(V_0401, "\n"));

    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(index, v_401, parser);
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    return transformToConstructionFeeder(
        Arrays.asList(
            makeQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1")),
            makeQueryRunner(factory, "index2", new QueryableIndexSegment("index2", index2))
        )
    );
  }

  private final QueryRunner<Row> runner;

  public GroupByQueryExplodeTest(QueryRunner<Row> runner)
  {
    this.runner = runner;
  }

  @Test
  public void testGroupBy()
  {
    BaseAggregationQuery.Builder<GroupByQuery> builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(DefaultDimensionSpec.toSpec("x"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("spot$automotive"),
                new LongSumAggregatorFactory("spot$mezzanine"),
                new LongSumAggregatorFactory("spot$premium"),
                new LongSumAggregatorFactory("total_market$mezzanine"),
                new LongSumAggregatorFactory("total_market$premium"),
                new LongSumAggregatorFactory("upfront$mezzanine"),
                new LongSumAggregatorFactory("upfront$premium")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLateralViewSpec(
            new LateralViewSpec(
                Arrays.<LateralViewSpec.LateralViewElement>asList(
                    new LateralViewSpec.LateralViewElement("market", null),
                    new LateralViewSpec.LateralViewElement("quality", null)
                ),
                null,
                null,
                "$",
                "count"
            )
        )
        .setContext(
            ImmutableMap.<String, Object>of("TEST_AS_SORTED", true)
        );

    List<Row> expectedResults;
    Iterable<Row> results;
    String[] columnNames;

    columnNames = new String[]{"__time", "market", "quality", "count"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "spot", "automotive", 10L),
        array("2011-04-01T00:00:00.000Z", "spot", "mezzanine", 50L),
        array("2011-04-01T00:00:00.000Z", "spot", "premium", 90L),
        array("2011-04-01T00:00:00.000Z", "total_market", "mezzanine", 130L),
        array("2011-04-01T00:00:00.000Z", "total_market", "premium", 170L),
        array("2011-04-01T00:00:00.000Z", "upfront", "mezzanine", 210L),
        array("2011-04-01T00:00:00.000Z", "upfront", "premium", 250L),
        array("2011-04-01T00:00:00.000Z", "spot", "automotive", 20L),
        array("2011-04-01T00:00:00.000Z", "spot", "mezzanine", 60L),
        array("2011-04-01T00:00:00.000Z", "spot", "premium", 100L),
        array("2011-04-01T00:00:00.000Z", "total_market", "mezzanine", 140L),
        array("2011-04-01T00:00:00.000Z", "total_market", "premium", 180L),
        array("2011-04-01T00:00:00.000Z", "upfront", "mezzanine", 220L),
        array("2011-04-01T00:00:00.000Z", "upfront", "premium", 260L),
        array("2011-04-01T00:00:00.000Z", "spot", "automotive", 30L),
        array("2011-04-01T00:00:00.000Z", "spot", "mezzanine", 70L),
        array("2011-04-01T00:00:00.000Z", "spot", "premium", 110L),
        array("2011-04-01T00:00:00.000Z", "total_market", "mezzanine", 150L),
        array("2011-04-01T00:00:00.000Z", "total_market", "premium", 190L),
        array("2011-04-01T00:00:00.000Z", "upfront", "mezzanine", 230L),
        array("2011-04-01T00:00:00.000Z", "upfront", "premium", 270L),
        array("2011-04-01T00:00:00.000Z", "spot", "automotive", 40L),
        array("2011-04-01T00:00:00.000Z", "spot", "mezzanine", 80L),
        array("2011-04-01T00:00:00.000Z", "spot", "premium", 120L),
        array("2011-04-01T00:00:00.000Z", "total_market", "mezzanine", 160L),
        array("2011-04-01T00:00:00.000Z", "total_market", "premium", 200L),
        array("2011-04-01T00:00:00.000Z", "upfront", "mezzanine", 240L),
        array("2011-04-01T00:00:00.000Z", "upfront", "premium", 280L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Row>newArrayList());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element
    builder.setLateralViewSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement("market", null)
            ),
            null,
            Arrays.asList("x"),
            "$",
            null
        )
    );

    columnNames = new String[]{"__time", "market", "mezzanine", "premium", "automotive"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "spot", 50L, 90L, 10L),
        array("2011-04-01T00:00:00.000Z", "total_market", 130L, 170L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 210L, 250L, null),
        array("2011-04-01T00:00:00.000Z", "spot", 60L, 100L, 20L),
        array("2011-04-01T00:00:00.000Z", "total_market", 140L, 180L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 220L, 260L, null),
        array("2011-04-01T00:00:00.000Z", "spot", 70L, 110L, 30L),
        array("2011-04-01T00:00:00.000Z", "total_market", 150L, 190L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 230L, 270L, null),
        array("2011-04-01T00:00:00.000Z", "spot", 80L, 120L, 40L),
        array("2011-04-01T00:00:00.000Z", "total_market", 160L, 200L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 240L, 280L, null)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Row>newArrayList());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, selective
    builder.setLateralViewSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement("market", Arrays.asList("total_market", "upfront", "xxx"))
            ),
            null,
            Arrays.asList("x"),
            "$",
            null
        )
    );

    columnNames = new String[]{"__time", "market", "mezzanine", "premium", "automotive"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "total_market", 130L, 170L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 210L, 250L, null),
        array("2011-04-01T00:00:00.000Z", "total_market", 140L, 180L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 220L, 260L, null),
        array("2011-04-01T00:00:00.000Z", "total_market", 150L, 190L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 230L, 270L, null),
        array("2011-04-01T00:00:00.000Z", "total_market", 160L, 200L, null),
        array("2011-04-01T00:00:00.000Z", "upfront", 240L, 280L, null)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Row>newArrayList());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, 2nd
    builder.setLateralViewSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                null,
                new LateralViewSpec.LateralViewElement("quality", null)
            ),
            null,
            Arrays.asList("x"),
            "$",
            null
        )
    );

    columnNames = new String[]{"__time", "quality", "spot", "total_market", "upfront"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "automotive", 10L, null, null),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 50L, 130L, 210L),
        array("2011-04-01T00:00:00.000Z", "premium", 90L, 170L, 250L),
        array("2011-04-01T00:00:00.000Z", "automotive", 20L, null, null),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 60L, 140L, 220L),
        array("2011-04-01T00:00:00.000Z", "premium", 100L, 180L, 260L),
        array("2011-04-01T00:00:00.000Z", "automotive", 30L, null, null),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 70L, 150L, 230L),
        array("2011-04-01T00:00:00.000Z", "premium", 110L, 190L, 270L),
        array("2011-04-01T00:00:00.000Z", "automotive", 40L, null, null),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 80L, 160L, 240L),
        array("2011-04-01T00:00:00.000Z", "premium", 120L, 200L, 280L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Row>newArrayList());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, 2nd, selective
    builder.setLateralViewSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement(null, Arrays.asList("spot", "total_market")),
                new LateralViewSpec.LateralViewElement("quality", Arrays.asList("mezzanine", "premium"))
            ),
            null,
            Arrays.asList("x"),
            "$",
            "count"
        )
    );

    columnNames = new String[]{"__time", "quality", "spot", "total_market"};
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "mezzanine", 50L, 130L),
        array("2011-04-01T00:00:00.000Z", "premium", 90L, 170L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 60L, 140L),
        array("2011-04-01T00:00:00.000Z", "premium", 100L, 180L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 70L, 150L),
        array("2011-04-01T00:00:00.000Z", "premium", 110L, 190L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 80L, 160L),
        array("2011-04-01T00:00:00.000Z", "premium", 120L, 200L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Row>newArrayList());
    GroupByQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  private Object[] array(Object... objects)
  {
    return objects;
  }
}
