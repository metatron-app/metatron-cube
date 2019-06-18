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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TimeseriesQueryQueryToolChestTest
{
  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(false, true));
  }

  private final boolean descending;

  public TimeseriesQueryQueryToolChestTest(boolean descending)
  {
    this.descending = descending;
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    CacheStrategy<Row, Object[], TimeseriesQuery> strategy =
        new TimeseriesQueryQueryToolChest(null).getCacheStrategyIfExists(
            new TimeseriesQuery(
                new TableDataSource("dummy"),
                new MultipleIntervalSegmentSpec(
                    ImmutableList.of(
                        new Interval(
                            "2015-01-01/2015-01-02"
                        )
                    )
                ),
                descending,
                null,
                QueryGranularities.ALL,
                null,
                ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("metric1")),
                null,
                null,
                null,
                null,
                null,
                null
            )
        );

    final CompactRow result = new CompactRow(
        // test timestamps that result in integer size millis
        new Object[]{new DateTime(123L).getMillis(), 2}
    );

    Object preparedValue = strategy.prepareForCache().apply(result);

    ObjectMapper objectMapper = new DefaultObjectMapper();
    Object[] fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    CompactRow fromCacheResult = (CompactRow) strategy.pullFromCache().apply(fromCacheValue);
    Assert.assertEquals(result.getTimestampFromEpoch(), fromCacheResult.getTimestampFromEpoch());
  }
}
