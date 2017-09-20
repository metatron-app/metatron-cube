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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.python.google.common.util.concurrent.Futures;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class SelectQueryRunnerFactory
    implements QueryRunnerFactory<Result<SelectResultValue>, SelectQuery>
{
  private static final Logger LOG = new Logger(SelectQueryRunnerFactory.class);

  private final SelectQueryQueryToolChest toolChest;
  private final SelectQueryEngine engine;
  private final SelectQueryConfig config;
  private final QueryWatcher queryWatcher;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public SelectQueryRunnerFactory(
      SelectQueryQueryToolChest toolChest,
      SelectQueryEngine engine,
      SelectQueryConfig config,
      QueryWatcher queryWatcher) {
    this(toolChest, engine, config, queryWatcher, null);
  }

  public SelectQueryRunnerFactory(
      SelectQueryQueryToolChest toolChest,
      SelectQueryEngine engine,
      SelectQueryConfig config,
      QueryWatcher queryWatcher,
      Cache cache
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.config = config;
    this.queryWatcher = queryWatcher;
    this.cache = cache;
  }

  @Override
  public Future<Object> preFactoring(SelectQuery query, List<Segment> segments, ExecutorService exec)
  {
    if (segments.size() < config.getOptimizeSegmentThreshold()) {
      return null;
    }
    PagingSpec pagingSpec = query.getPagingSpec();
    int threshold = pagingSpec.getThreshold();
    if (threshold > 0) {
      final SelectMetaQuery baseQuery = query.toMetaQuery(false);
      final SelectMetaQueryEngine metaQueryEngine = new SelectMetaQueryEngine();

      final Set<String> targets = Sets.newHashSet();
      for (Segment segment : query.isDescending() ? Lists.reverse(segments) : segments) {
        targets.add(segment.getIdentifier());
        SelectMetaQuery metaQuery = baseQuery.withQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(Arrays.asList(segment.getDataInterval()))
        );
        for (Result<SelectMetaResultValue> result : Sequences.toList(
            metaQueryEngine.process(metaQuery, segment), Lists.<Result<SelectMetaResultValue>>newArrayList()
        )) {
          threshold -= result.getValue().getTotalCount();
          if (threshold < 0) {
            LOG.info(
                "Trimmed %d segments from original target %d segments",
                (segments.size() - targets.size()),
                segments.size()
            );
            return Futures.<Object>immediateFuture(targets);
          }
        }
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<Result<SelectResultValue>> createRunner(final Segment segment, Future<Object> optimizer)
  {
    if (optimizer == null ||
        ((Set<String>) Futures.getUnchecked(optimizer)).contains(segment.getIdentifier())) {
      return new SelectQueryRunner(engine, config, segment, cache);
    }
    return new NoopQueryRunner<Result<SelectResultValue>>();
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Result<SelectResultValue>>> queryRunners,
      final Future<Object> optimizer
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

  private static class SelectQueryRunner implements QueryRunner<Result<SelectResultValue>>
  {
    private final SelectQueryEngine engine;
    private final SelectQueryConfig config;
    private final Segment segment;
    private final Cache cache;

    private SelectQueryRunner(SelectQueryEngine engine, SelectQueryConfig config, Segment segment, Cache cache)
    {
      this.engine = engine;
      this.segment = segment;
      this.config = config;
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

      return engine.process((SelectQuery) input, config, segment.asStorageAdapter(true), cache);
    }
  }
}
