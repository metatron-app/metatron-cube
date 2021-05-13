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

import io.druid.collections.StupidPool;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;

import java.nio.ByteBuffer;

/**
 */
public class TestQueryRunners
{
  public static final StupidPool<ByteBuffer> pool = StupidPool.heap(1024 * 1024 * 10);
  public static final TopNQueryConfig topNConfig = new TopNQueryConfig();

  public static StupidPool<ByteBuffer> getPool()
  {
    return pool;
  }

  public static <T> QueryRunner<T> makeTopNQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        pool,
        new TopNQueryQueryToolChest(
            topNConfig,
            TestHelper.testTopNQueryEngine()
        ),
        TestHelper.NOOP_QUERYWATCHER
    );
    return factory.getToolchest().finalizeResults(
        factory.createRunner(adapter, null)
    );
  }

  public static <T> QueryRunner<T> makeTimeSeriesQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        new QueryConfig(),
        TestHelper.NOOP_QUERYWATCHER
    );

    return factory.getToolchest().finalizeResults(
        factory.createRunner(adapter, null)
    );
  }

  public static <T> QueryRunner<T> makeSearchQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new SearchQueryRunnerFactory(new SearchQueryQueryToolChest(
          new SearchQueryConfig()
    ),
        TestHelper.NOOP_QUERYWATCHER);
    return factory.getToolchest().finalizeResults(
        factory.createRunner(adapter, null)
    );
  }

  public static <T> QueryRunner<T> makeTimeBoundaryQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = TestHelper.factoryFor(TimeBoundaryQuery.class);
    return factory.getToolchest().finalizeResults(
        factory.createRunner(adapter, null)
    );
  }
}
