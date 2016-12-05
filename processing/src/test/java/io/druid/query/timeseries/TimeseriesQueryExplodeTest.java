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

package io.druid.query.timeseries;

import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Druids;
import io.druid.query.LateralViewSpec;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
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
import java.util.Arrays;
import java.util.List;

import static io.druid.query.QueryRunnerTestHelper.makeQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryExplodeTest
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
    TimeseriesQueryQueryToolChest toolChest = new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        toolChest,
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
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
            new TimestampSpec("ts", "iso", null),
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
            toolChest.finalQueryDecoration(
                makeQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1"))
            ),
            toolChest.finalQueryDecoration(
                makeQueryRunner(factory, "index2", new QueryableIndexSegment("index2", index2))
            )
        )
    );
  }

  private final QueryRunner<Result<TimeseriesResultValue>> runner;

  public TimeseriesQueryExplodeTest(QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    this.runner = runner;
  }

  @Test
  public void testExplodeHour()
  {
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
              .aggregators(
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
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .granularity(QueryGranularities.HOUR)
              .explodeSpec(
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
              );

    List<Result<TimeseriesResultValue>> expectedResults;
    List<Result<TimeseriesResultValue>> results;
    String[] columnNames;

    // retain timestamp
    columnNames = new String[]{"market", "quality", "count"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", "automotive", 10L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", "mezzanine", 50L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", "premium", 90L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", "mezzanine", 130L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", "premium", 170L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", "mezzanine", 210L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", "premium", 250L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "spot", "automotive", 20L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "spot", "mezzanine", 60L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "spot", "premium", 100L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "total_market", "mezzanine", 140L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "total_market", "premium", 180L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "upfront", "mezzanine", 220L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "upfront", "premium", 260L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "spot", "automotive", 30L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "spot", "mezzanine", 70L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "spot", "premium", 110L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "total_market", "mezzanine", 150L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "total_market", "premium", 190L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "upfront", "mezzanine", 230L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "upfront", "premium", 270L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "spot", "automotive", 40L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "spot", "mezzanine", 80L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "spot", "premium", 120L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "total_market", "mezzanine", 160L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "total_market", "premium", 200L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "upfront", "mezzanine", 240L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "upfront", "premium", 280L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement("market", null)
            ),
            null,
            null,
            "$",
            null
        )
    );

    columnNames = new String[]{"market", "mezzanine", "premium", "automotive"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", 50L, 90L, 10L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", 130L, 170L, null),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", 210L, 250L, null),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "spot", 60L, 100L, 20L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "total_market", 140L, 180L, null),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "upfront", 220L, 260L, null),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "spot", 70L, 110L, 30L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "total_market", 150L, 190L, null),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "upfront", 230L, 270L, null),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "spot", 80L, 120L, 40L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "total_market", 160L, 200L, null),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "upfront", 240L, 280L, null)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, selective
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement("market", Arrays.asList("total_market", "upfront", "xxx"))
            ),
            null,
            null,
            "$",
            null
        )
    );

    columnNames = new String[]{"market", "mezzanine", "premium", "automotive"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", 130L, 170L, null),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", 210L, 250L, null),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "total_market", 140L, 180L, null),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "upfront", 220L, 260L, null),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "total_market", 150L, 190L, null),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "upfront", 230L, 270L, null),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "total_market", 160L, 200L, null),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "upfront", 240L, 280L, null)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, 2nd
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                null,
                new LateralViewSpec.LateralViewElement("quality", null)
            ),
            null,
            null,
            "$",
            null
        )
    );

    columnNames = new String[]{"quality", "spot", "total_market", "upfront"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "automotive", 10L, null, null),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "mezzanine", 50L, 130L, 210L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "premium", 90L, 170L, 250L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "automotive", 20L, null, null),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "mezzanine", 60L, 140L, 220L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "premium", 100L, 180L, 260L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "automotive", 30L, null, null),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "mezzanine", 70L, 150L, 230L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "premium", 110L, 190L, 270L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "automotive", 40L, null, null),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "mezzanine", 80L, 160L, 240L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "premium", 120L, 200L, 280L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, 2nd, selective
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement(null, Arrays.asList("spot", "total_market")),
                new LateralViewSpec.LateralViewElement("quality", Arrays.asList("mezzanine", "premium"))
            ),
            null,
            Arrays.asList("x", "timestamp"),
            "$",
            "count"
        )
    );

    columnNames = new String[]{"quality", "spot", "total_market"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "mezzanine", 50L, 130L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "premium", 90L, 170L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "mezzanine", 60L, 140L),
        array(new DateTime("2011-04-01T01:00:00.000Z"), "premium", 100L, 180L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "mezzanine", 70L, 150L),
        array(new DateTime("2011-04-01T02:00:00.000Z"), "premium", 110L, 190L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "mezzanine", 80L, 160L),
        array(new DateTime("2011-04-01T03:00:00.000Z"), "premium", 120L, 200L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  @Test
  public void testExplodeDay()
  {
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
              .aggregators(
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
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .granularity(QueryGranularities.DAY)
              .explodeSpec(
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
              );

    List<Result<TimeseriesResultValue>> expectedResults;
    List<Result<TimeseriesResultValue>> results;
    String[] columnNames;

    // retain timestamp
    columnNames = new String[]{"market", "quality", "count"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", "automotive", 100L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", "mezzanine", 260L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", "premium", 420L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", "mezzanine", 580L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", "premium", 740L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", "mezzanine", 900L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", "premium", 1060L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement("market", null)
            ),
            null,
            null,
            "$",
            null
        )
    );

    columnNames = new String[]{"market", "mezzanine", "premium", "automotive"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "spot", 260L, 420L, 100L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", 580L, 740L, null),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", 900L, 1060L, null)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, selective
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement("market", Arrays.asList("total_market", "upfront", "xxx"))
            ),
            null,
            null,
            "$",
            null
        )
    );

    columnNames = new String[]{"market", "mezzanine", "premium", "automotive"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "total_market", 580L, 740L, null),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "upfront", 900L, 1060L, null)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, 2nd
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                null,
                new LateralViewSpec.LateralViewElement("quality", null)
            ),
            null,
            null,
            "$",
            null
        )
    );

    columnNames = new String[]{"quality", "spot", "total_market", "upfront"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "automotive", 100L, null, null),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "mezzanine", 260L, 580L, 900L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "premium", 420L, 740L, 1060L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);

    // single element, 2nd, selective
    builder.explodeSpec(
        new LateralViewSpec(
            Arrays.<LateralViewSpec.LateralViewElement>asList(
                new LateralViewSpec.LateralViewElement(null, Arrays.asList("spot", "total_market")),
                new LateralViewSpec.LateralViewElement("quality", Arrays.asList("mezzanine", "premium"))
            ),
            null,
            null,
            "$",
            "count"
        )
    );

    columnNames = new String[]{"quality", "spot", "total_market"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        array(new DateTime("2011-04-01T00:00:00.000Z"), "mezzanine", 260L, 580L),
        array(new DateTime("2011-04-01T00:00:00.000Z"), "premium", 420L, 740L)
    );
    results = Sequences.toList(runner.run(builder.build(), null), Lists.<Result<TimeseriesResultValue>>newArrayList());
    TimeseriesQueryRunnerTestHelper.validate(columnNames, expectedResults, results);
  }

  private Object[] array(Object... objects)
  {
    return objects;
  }
}
