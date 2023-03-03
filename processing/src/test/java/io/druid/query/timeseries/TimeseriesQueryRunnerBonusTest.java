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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerBonusTest
{
  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(false, true));
  }

  private final boolean descending;

  public TimeseriesQueryRunnerBonusTest(boolean descending)
  {
    this.descending = descending;
  }

  @Test
  public void testOneRowAtATime() throws Exception
  {
    final IncrementalIndex oneRowIndex = new OnheapIncrementalIndex(
        new DateTime("2012-01-01T00:00:00Z").getMillis(), QueryGranularities.NONE, new AggregatorFactory[]{}, 1000
    );

    List<Row> results;

    oneRowIndex.add(
        new MapBasedInputRow(
            new DateTime("2012-01-01T00:00:00Z").getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.<String, Object>of("dim1", "x")
        )
    );

    results = runTimeseriesCount(oneRowIndex);

    Assert.assertEquals("index size", 1, oneRowIndex.size());
    Assert.assertEquals("result size", 1, results.size());
    Assert.assertEquals("result timestamp", new DateTime("2012-01-01T00:00:00Z"), results.get(0).getTimestamp());
    Assert.assertEquals("result count metric", 1, results.get(0).getLongMetric("rows"));

    oneRowIndex.add(
        new MapBasedInputRow(
            new DateTime("2012-01-01T00:00:00Z").getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.<String, Object>of("dim1", "y")
        )
    );

    results = runTimeseriesCount(oneRowIndex);

    Assert.assertEquals("index size", 2, oneRowIndex.size());
    Assert.assertEquals("result size", 1, results.size());
    Assert.assertEquals("result timestamp", new DateTime("2012-01-01T00:00:00Z"), results.get(0).getTimestamp());
    Assert.assertEquals("result count metric", 2, results.get(0).getLongMetric("rows"));
  }

  private List<Row> runTimeseriesCount(IncrementalIndex index)
  {
    final TimeseriesQueryQueryToolChest toolChest = new TimeseriesQueryQueryToolChest();
    final QueryRunnerFactory<Row> factory = new TimeseriesQueryRunnerFactory(
        toolChest,
        new TimeseriesQueryEngine(),
        new QueryConfig(),
        TestHelper.NOOP_QUERYWATCHER
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(QueryGranularities.ALL)
                                  .intervals(ImmutableList.of(new Interval("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
                                  .descending(descending)
                                  .build();
    HashMap<String,Object> context = new HashMap<String, Object>();

    final QueryRunner<Row> runner = new FinalizeResultsQueryRunner<Row>(
        toolChest.mergeResults(
            factory.createRunner(
                new IncrementalIndexSegment(index, DataSegment.asKey("test" )),
                null
            )),
        toolChest
    );

    return Sequences.toList(
        runner.run(query, context),
        Lists.<Row>newArrayList()
    );
  }

}
