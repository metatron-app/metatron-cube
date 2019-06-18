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

package io.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;
import io.druid.query.UnionQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TimeSeriesUnionQueryRunnerTest
{
  private final QueryRunner runner;
  private final boolean descending;

  public TimeSeriesUnionQueryRunnerTest(
      QueryRunner runner, boolean descending
  )
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Parameterized.Parameters(name="{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeUnionQueryRunners(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            ),
            QueryRunnerTestHelper.unionDataSource
        ),
        // descending?
        Arrays.asList(false, true)
    );
  }

  private <T> void assertExpectedResults(Iterable<Row> expectedResults, Iterable<Row> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testUnionTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.unionDataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("rows", 52L, "idx", 26476L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of("rows", 52L, "idx", 23308L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        )
    );
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Row> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Row>newArrayList()
    );

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testUnionResultMerging()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(UnionDataSource.of(Lists.newArrayList("ds1", "ds2")))
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .build();
    QueryToolChest toolChest = new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator());
    final List<Row> ds1 = Lists.<Row>newArrayList(
        new CompactRow(
            new Object[]{new DateTime("2011-04-02").getMillis(), 1L, 2L}
        ),
        new CompactRow(
            new Object[]{new DateTime("2011-04-03").getMillis(), 3L, 4L}
        )
    );
    final List<Row> ds2 = Lists.<Row>newArrayList(
        new CompactRow(
            new Object[]{new DateTime("2011-04-01").getMillis(), 5L, 6L}
        ),
        new CompactRow(
            new Object[]{new DateTime("2011-04-02").getMillis(), 7L, 8L}
        ),
        new CompactRow(
            new Object[]{new DateTime("2011-04-04").getMillis(), 9L, 10L}
        )
    );

    QueryRunner mergingrunner = toolChest.mergeResults(
        new UnionQueryRunner<>(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(
                  Query<Row> query,
                  Map<String, Object> responseContext
              )
              {
                if (query.getDataSource().equals(new TableDataSource("ds1"))) {
                  return Sequences.simple(descending ? Lists.reverse(ds1) : ds1);
                } else {
                  return Sequences.simple(descending ? Lists.reverse(ds2) : ds2);
                }
              }
            }
        )
    );

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("rows", 5L, "idx", 6L)
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of("rows", 8L, "idx", 10L)
        ),
        new MapBasedRow(
            new DateTime("2011-04-03"),
            ImmutableMap.<String, Object>of("rows", 3L, "idx", 4L)
        ),
        new MapBasedRow(
            new DateTime("2011-04-04"),
            ImmutableMap.<String, Object>of("rows", 9L, "idx", 10L)
        )
    );

    Iterable<Row> results = Sequences.toList(
        mergingrunner.run(query, Maps.<String, Object>newHashMap()),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }
}
