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

package io.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;


public class TimewarpOperatorTest
{
  public static final ImmutableMap<String, Object> CONTEXT = ImmutableMap.of();

  TimewarpOperator<Row> testOperator = new TimewarpOperator<>(
      new Interval(new DateTime("2014-01-01"), new DateTime("2014-01-15")),
      new Period("P1W"),
      new DateTime("2014-01-06") // align on Monday
  );

  @Test
  public void testComputeOffset() throws Exception
  {
    {
      final DateTime t = new DateTime("2014-01-23");
      final DateTime tOffset = new DateTime("2014-01-09");

      Assert.assertEquals(
          new DateTime(tOffset),
          t.plus(testOperator.computeOffset(t.getMillis()))
      );
    }

    {
      final DateTime t = new DateTime("2014-08-02");
      final DateTime tOffset = new DateTime("2014-01-11");

      Assert.assertEquals(
          new DateTime(tOffset),
          t.plus(testOperator.computeOffset(t.getMillis()))
      );
    }
  }

  @Test
  public void testPostProcess() throws Exception
  {
    QueryRunner<Row> queryRunner = testOperator.postProcess(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query,
              Map<String, Object> responseContext
          )
          {
            return Sequences.<Row>of(
                new MapBasedRow(
                    new DateTime("2014-01-09"),
                    ImmutableMap.<String, Object>of("metric", 2)
                ),
                new MapBasedRow(
                    new DateTime("2014-01-11"),
                    ImmutableMap.<String, Object>of("metric", 3)
                ),
                new MapBasedRow(
                    query.getIntervals().get(0).getEnd(),
                    ImmutableMap.<String, Object>of("metric", 5)
                )
            );
          }
        },
        new DateTime("2014-08-02").getMillis()
    );

    final Query<Row> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-07-31/2014-08-05")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new MapBasedRow(
                new DateTime("2014-07-31"),
                ImmutableMap.<String, Object>of("metric", 2)
            ),
            new MapBasedRow(
                new DateTime("2014-08-02"),
                ImmutableMap.<String, Object>of("metric", 3)
            ),
            new MapBasedRow(
                new DateTime("2014-08-02"),
                ImmutableMap.<String, Object>of("metric", 5)
            )
        ),
        Sequences.toList(queryRunner.run(query, CONTEXT), Lists.<Row>newArrayList())
    );


    TimewarpOperator<Result<TimeBoundaryResultValue>> timeBoundaryOperator = new TimewarpOperator<>(
        new Interval(new DateTime("2014-01-01"), new DateTime("2014-01-15")),
        new Period("P1W"),
        new DateTime("2014-01-06") // align on Monday
    );

    QueryRunner<Result<TimeBoundaryResultValue>> timeBoundaryRunner = timeBoundaryOperator.postProcess(
        new QueryRunner<Result<TimeBoundaryResultValue>>()
        {
          @Override
          public Sequence<Result<TimeBoundaryResultValue>> run(
              Query<Result<TimeBoundaryResultValue>> query,
              Map<String, Object> responseContext
          )
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        new DateTime("2014-01-12"),
                        new TimeBoundaryResultValue(
                            ImmutableMap.<String, Object>of(
                                "maxTime",
                                new DateTime("2014-01-12")
                            )
                        )
                    )
                )
            );
          }
        },
        new DateTime("2014-08-02").getMillis()
    );

    final Query<Result<TimeBoundaryResultValue>> timeBoundaryQuery =
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource("dummy")
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                new DateTime("2014-08-02"),
                new TimeBoundaryResultValue(ImmutableMap.<String, Object>of("maxTime", new DateTime("2014-08-02")))
            )
        ),
        Sequences.toList(
            timeBoundaryRunner.run(timeBoundaryQuery, CONTEXT),
            Lists.<Result<TimeBoundaryResultValue>>newArrayList()
        )
    );

  }

  @Test
  public void testEmptyFutureInterval() throws Exception
  {
    QueryRunner<Row> queryRunner = testOperator.postProcess(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query,
              Map<String, Object> responseContext
          )
          {
            return Sequences.<Row>of(
                new MapBasedRow(
                    query.getIntervals().get(0).getStart(),
                    ImmutableMap.<String, Object>of("metric", 2)
                ),
                new MapBasedRow(
                    query.getIntervals().get(0).getEnd(),
                    ImmutableMap.<String, Object>of("metric", 3)
                )
            );
          }
        },
        new DateTime("2014-08-02").getMillis()
    );

    final Query<Row> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-08-06/2014-08-08")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new MapBasedRow(
                new DateTime("2014-08-02"),
                ImmutableMap.<String, Object>of("metric", 2)
            ),
            new MapBasedRow(
                new DateTime("2014-08-02"),
                ImmutableMap.<String, Object>of("metric", 3)
            )
        ),
        Sequences.toList(queryRunner.run(query, Maps.<String, Object>newHashMap()), Lists.<Row>newArrayList())
    );
  }
}
