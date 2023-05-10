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
import io.druid.common.DateTimes;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryRunners;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;
import io.druid.query.UnionQueryRunner;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TimeSeriesUnionQueryRunnerTest
{
  private final TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
      new TimeseriesQueryQueryToolChest(),
      new TimeseriesQueryEngine(),
      new QueryConfig(),
      TestHelper.NOOP_QUERYWATCHER
  );

  @Test
  public void testUnionTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.unionDataSource)
                                  .granularity(Granularities.DAY)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      new LongSumAggregatorFactory("idx", "index"),
                                      QueryRunnerTestHelper.qualityUniques
                                  )
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

    for (boolean descending : new boolean[]{false, true}) {
      for (QueryRunner<Row> runner : QueryRunnerTestHelper.makeUnionQueryRunners(query, factory, QueryRunnerTestHelper.unionDataSource)) {
        List<Row> results = Sequences.toList(QueryRunners.run(query.withDescending(descending), runner));
        assertExpectedResults(expectedResults, results, descending);
      }
    }
  }

  private <T> void assertExpectedResults(List<Row> expectedResults, List<Row> results, boolean descending)
  {
    TestHelper.assertExpectedObjects(descending ? Lists.reverse(expectedResults) : expectedResults, results);
  }

  @Test
  public void testUnionResultMerging()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(UnionDataSource.of(Lists.newArrayList("ds1", "ds2")))
                                  .granularity(Granularities.DAY)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      new LongSumAggregatorFactory("idx", "index")
                                  )
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(DateTimes.utc("2011-04-01"), ImmutableMap.<String, Object>of("rows", 5L, "idx", 6L)),
        new MapBasedRow(DateTimes.utc("2011-04-02"), ImmutableMap.<String, Object>of("rows", 8L, "idx", 10L)),
        new MapBasedRow(DateTimes.utc("2011-04-03"), ImmutableMap.<String, Object>of("rows", 3L, "idx", 4L)),
        new MapBasedRow(DateTimes.utc("2011-04-04"), ImmutableMap.<String, Object>of("rows", 9L, "idx", 10L))
    );

    for (boolean descending : new boolean[]{false, true}) {
      List<Row> ds1 = Arrays.asList(
          CompactRow.of(DateTimes.millis("2011-04-02"), 1L, 2L),
          CompactRow.of(DateTimes.millis("2011-04-03"), 3L, 4L)
      );
      List<Row> ds2 = Arrays.asList(
          CompactRow.of(DateTimes.millis("2011-04-01"), 5L, 6L),
          CompactRow.of(DateTimes.millis("2011-04-02"), 7L, 8L),
          CompactRow.of(DateTimes.millis("2011-04-04"), 9L, 10L)
      );
      QueryRunner<Row> runner = factory.getToolchest().mergeResults(
          new UnionQueryRunner<>(
              new QueryRunner<Row>()
              {
                @Override
                public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
                {
                  List<String> columns = query.estimatedInitialColumns();
                  if (query.getDataSource().equals(new TableDataSource("ds1"))) {
                    return Sequences.simple(columns, query.isDescending() ? Lists.reverse(ds1) : ds1);
                  } else {
                    return Sequences.simple(columns, query.isDescending() ? Lists.reverse(ds2) : ds2);
                  }
                }
              }
          )
      );
      List<Row> results = Sequences.toList(QueryRunners.run(query.withDescending(descending), runner));
      assertExpectedResults(expectedResults, results, descending);
    }
  }
}
