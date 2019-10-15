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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.data.input.Row;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunners;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class TimeseriesQueryRunnerFactory extends QueryRunnerFactory.Abstract<Row, TimeseriesQuery>
{
  private final TimeseriesQueryEngine engine;

  @Inject
  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
  }

  @Override
  public QueryRunner<Row> createRunner(Segment segment, Future<Object> optimizer)
  {
    return new TimeseriesQueryRunner(engine, segment, cache);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Row>> querys,
      final Future<Object> optimizer
  )
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        // need to limit resource usage for some aggregators like CountMinSketch
        final int parallelism = query.getContextInt(Query.TIMESERIES_MERGE_PARALLELISM, -1);
        final QueryRunner<Row> runner;
        if (parallelism > 0) {
          runner = QueryRunners.executeParallel(queryExecutor, Lists.newArrayList(querys), query.getMergeOrdering());
        } else {
          runner = new ChainedExecutionQueryRunner<Row>(queryExecutor, queryWatcher, querys);
        }
        return runner.run(query, responseContext);
      }
    };
  }

  private static class TimeseriesQueryRunner implements QueryRunner<Row>
  {
    private final TimeseriesQueryEngine engine;
    private final Segment segment;
    private final Cache cache;

    private TimeseriesQueryRunner(TimeseriesQueryEngine engine, Segment segment, Cache cache)
    {
      this.engine = engine;
      this.segment = segment;
      this.cache = cache;
    }

    @Override
    public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
    {
      if (!(input instanceof TimeseriesQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeseriesQuery.class);
      }
      return engine.process((TimeseriesQuery) input, segment, true, cache);
    }
  }
}
