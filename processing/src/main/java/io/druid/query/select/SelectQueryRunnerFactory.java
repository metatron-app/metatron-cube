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

package io.druid.query.select;

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SelectQueryRunnerFactory
    implements QueryRunnerFactory<Result<SelectResultValue>, SelectQuery>
{
  private final SelectQueryQueryToolChest toolChest;
  private final SelectQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public SelectQueryRunnerFactory(
      SelectQueryQueryToolChest toolChest,
      SelectQueryEngine engine,
      QueryWatcher queryWatcher) {
    this(toolChest, engine, queryWatcher, null);
  }

  public SelectQueryRunnerFactory(
      SelectQueryQueryToolChest toolChest,
      SelectQueryEngine engine,
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
  public QueryRunner<Result<SelectResultValue>> createRunner(final Segment segment, Object optimizer)
  {
    return new SelectQueryRunner(engine, segment, cache);
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Result<SelectResultValue>>> queryRunners,
      final Object optimizer
  )
  {
    return new ChainedExecutionQueryRunner<Result<SelectResultValue>>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<SelectResultValue>, SelectQuery> getToolchest()
  {
    return toolChest;
  }

  @Override
  public Object preFactoring(SelectQuery query, List<Segment> segments)
  {
    return null;
  }

  private static class SelectQueryRunner implements QueryRunner<Result<SelectResultValue>>
  {
    private final SelectQueryEngine engine;
    private final Segment segment;
    private final Cache cache;

    private SelectQueryRunner(SelectQueryEngine engine, Segment segment, Cache cache)
    {
      this.engine = engine;
      this.segment = segment;
      this.cache = cache;
    }

    @Override
    public Sequence<Result<SelectResultValue>> run(
        Query<Result<SelectResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof SelectQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SelectQuery.class);
      }

      return engine.process((SelectQuery) input, segment, cache);
    }
  }
}
