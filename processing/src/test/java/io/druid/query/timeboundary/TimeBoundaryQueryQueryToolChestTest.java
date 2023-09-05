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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 */
public class TimeBoundaryQueryQueryToolChestTest
{
  private static final TimeBoundaryQuery TIME_BOUNDARY_QUERY = new TimeBoundaryQuery(
      new TableDataSource("test"),
      null,
      null,
      null
  );

  private static final TimeBoundaryQuery MAXTIME_BOUNDARY_QUERY = new TimeBoundaryQuery(
      new TableDataSource("test"),
      null,
      TimeBoundaryQuery.MAX_TIME,
      null
  );

  private static final TimeBoundaryQuery MINTIME_BOUNDARY_QUERY = new TimeBoundaryQuery(
      new TableDataSource("test"),
      null,
      TimeBoundaryQuery.MIN_TIME,
      null
  );

  private static class TestSegment implements LogicalSegment
  {
    private final Interval interval;

    private TestSegment(String interval) {this.interval = new Interval(interval);}

    @Override
    public Interval getInterval()
    {
      return interval;
    }

    @Override
    public boolean equals(Object o)
    {
      return interval.equals(((TestSegment) o).interval);
    }
  }

  private List<LogicalSegment> segments(String... intervals)
  {
    return Arrays.stream(intervals).map(TestSegment::new).collect(Collectors.toList());
  }

  private final TimeBoundaryQueryQueryToolChest toolChest = new TimeBoundaryQueryQueryToolChest();

  @Test
  public void testFilterSegments() throws Exception
  {
    List<LogicalSegment> segments = segments(
        "2013-01-01/P1D",
        "2013-01-01T01/PT1H",
        "2013-01-01T02/PT1H",
        "2013-01-02/P1D",
        "2013-01-03T01/PT1H",
        "2013-01-03T02/PT1H",
        "2013-01-03/P1D"
    );

    List<LogicalSegment> filtered = toolChest.filterSegments(TIME_BOUNDARY_QUERY, segments);

    Assert.assertEquals(6, filtered.size());

    List<LogicalSegment> expected = segments(
        "2013-01-01/P1D",
        "2013-01-01T01/PT1H",
        "2013-01-01T02/PT1H",
        "2013-01-03T01/PT1H",
        "2013-01-03T02/PT1H",
        "2013-01-03/P1D"
    );

    Assert.assertEquals(expected, filtered);
  }

  @Test
  public void testMaxTimeFilterSegments() throws Exception
  {
    List<LogicalSegment> segments = segments(
        "2013-01-01/P1D",
        "2013-01-01T01/PT1H",
        "2013-01-01T02/PT1H",
        "2013-01-02/P1D",
        "2013-01-03T01/PT1H",
        "2013-01-03T02/PT1H",
        "2013-01-03/P1D"
    );
    List<LogicalSegment> filtered = toolChest.filterSegments(MAXTIME_BOUNDARY_QUERY, segments);

    Assert.assertEquals(3, filtered.size());

    List<LogicalSegment> expected = segments("2013-01-03T01/PT1H", "2013-01-03T02/PT1H", "2013-01-03/P1D");

    Assert.assertEquals(expected, filtered);
  }

  @Test
  public void testMinTimeFilterSegments() throws Exception
  {
    List<LogicalSegment> segments = segments(
        "2013-01-01/P1D",
        "2013-01-01T01/PT1H",
        "2013-01-01T02/PT1H",
        "2013-01-02/P1D",
        "2013-01-03T01/PT1H",
        "2013-01-03T02/PT1H",
        "2013-01-03/P1D"
    );
    List<LogicalSegment> filtered = toolChest.filterSegments(MINTIME_BOUNDARY_QUERY, segments);

    Assert.assertEquals(3, filtered.size());

    List<LogicalSegment> expected = segments("2013-01-01/P1D", "2013-01-01T01/PT1H", "2013-01-01T02/PT1H");

    Assert.assertEquals(expected, filtered);
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    TimeBoundaryQuery query = new TimeBoundaryQuery(
        new TableDataSource("dummy" ),
        new MultipleIntervalSegmentSpec(
            ImmutableList.of(
                new Interval(
                    "2015-01-01/2015-01-02"
                )
            )
        ),
        null,
        null
    );
    CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery> strategy =
        toolChest.getCacheStrategyIfExists(query);

    final Result<TimeBoundaryResultValue> result = new Result<>(
        new DateTime(123L), new TimeBoundaryResultValue(
        ImmutableMap.of(
            TimeBoundaryQuery.MIN_TIME, new DateTime(0L).toString(),
            TimeBoundaryQuery.MAX_TIME, new DateTime("2015-01-01").toString()
        )
    )
    );

    Object preparedValue = strategy.prepareForCache(query).apply(result);

    ObjectMapper objectMapper = new DefaultObjectMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeBoundaryResultValue> fromCacheResult = strategy.pullFromCache(query).apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }
}
