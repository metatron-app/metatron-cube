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

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.granularity.QueryGranularities;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.segment.Segment;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class TimeseriesQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private final TimeseriesQueryQueryToolChest toolChest;
  private final TimeseriesQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryWatcher queryWatcher) {
    this(toolChest, engine, queryWatcher, null);
  }

  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryWatcher queryWatcher,
      Cache cache
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
    this.cache = cache;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> createRunner(final Segment segment, Future<Object> optimizer)
  {
    return new TimeseriesQueryRunner(engine, segment, cache);
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeseriesResultValue>>> queryRunners,
      Future<Object> optimizer
  )
  {
    return new ChainedExecutionQueryRunner<Result<TimeseriesResultValue>>(
        queryExecutor, queryWatcher, queryRunners
    ) {
      @Override
      protected Iterator<Result<TimeseriesResultValue>> toIterator(
          Query<Result<TimeseriesResultValue>> query,
          List<Iterable<Result<TimeseriesResultValue>>> results
      )
      {
        TimeseriesQuery timeseriesQuery = (TimeseriesQuery) query;
        if (QueryGranularities.ALL.equals(timeseriesQuery.getGranularity())) {
          return Iterables.concat(results).iterator();
        }
        return super.toIterator(query, results);
      }
    };
  }

  @Override
  public QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> getToolchest()
  {
    return toolChest;
  }

  @Override
  public Future<Object> preFactoring(
      TimeseriesQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return null;
  }

  private static class TimeseriesQueryRunner implements QueryRunner<Result<TimeseriesResultValue>>
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
    public Sequence<Result<TimeseriesResultValue>> run(
        Query<Result<TimeseriesResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TimeseriesQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeseriesQuery.class);
      }

      return engine.process((TimeseriesQuery) input, segment, cache);
    }
  }
}
