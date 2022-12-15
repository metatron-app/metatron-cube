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

package io.druid.query.select;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 */
public class SelectQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Result<SelectResultValue>, SelectQuery>
{
  private static final Logger LOG = new Logger(SelectQueryRunnerFactory.class);

  private final SelectQueryEngine engine;
  private final SelectQueryConfig config;

  @Inject
  public SelectQueryRunnerFactory(
      SelectQueryQueryToolChest toolChest,
      SelectQueryEngine engine,
      SelectQueryConfig config,
      QueryWatcher queryWatcher) {
    super(toolChest, queryWatcher);
    this.engine = engine;
    this.config = config;
  }

  @Override
  public Supplier<Object> preFactoring(
      SelectQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    if (segments.size() < config.getOptimizeSegmentThreshold()) {
      return null;
    }
    PagingSpec pagingSpec = query.getPagingSpec();
    int threshold = pagingSpec.getThreshold();
    if (threshold > 0) {
      final SelectMetaQuery baseQuery = query.toMetaQuery(false);
      final SelectMetaQueryEngine engine = new SelectMetaQueryEngine();

      final Set<String> targets = Sets.newHashSet();
      for (Segment segment : segments) {
        targets.add(segment.getIdentifier());
        SelectMetaQuery metaQuery = baseQuery.withQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(Arrays.asList(segment.getInterval()))
        );
        for (Result<SelectMetaResultValue> result : Sequences.toList(engine.process(metaQuery, segment, cache(query)))) {
          threshold -= result.getValue().getTotalCount();
          if (threshold < 0) {
            LOG.info(
                "Trimmed %d segments from original target %d segments",
                (segments.size() - targets.size()),
                segments.size()
            );
            return Suppliers.ofInstance(targets);
          }
        }
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<Result<SelectResultValue>> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    if (optimizer == null || ((Set<String>) optimizer.get()).contains(segment.getIdentifier())) {
      return new SelectQueryRunner(engine, config, segment, cache);
    }
    return NoopQueryRunner.instance();
  }

  private static class SelectQueryRunner implements QueryRunner<Result<SelectResultValue>>
  {
    private final SelectQueryEngine engine;
    private final SelectQueryConfig config;
    private final Segment segment;
    private final SessionCache cache;

    private SelectQueryRunner(SelectQueryEngine engine, SelectQueryConfig config, Segment segment, SessionCache cache)
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

      return engine.process((SelectQuery) input, config, segment, cache);
    }
  }
}
