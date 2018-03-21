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

package io.druid.query.search;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.search.search.SearchQuery;
import io.druid.segment.Segment;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class SearchQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
{
  private final SearchQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public SearchQueryRunnerFactory(
      SearchQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this(toolChest, queryWatcher, null);
  }

  public SearchQueryRunnerFactory(
      SearchQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher,
      Cache cache
  )
  {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
    this.cache = cache;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> createRunner(final Segment segment, Future<Object> optimizer)
  {
    return new SearchQueryRunner(segment, cache);
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<SearchResultValue>>> queryRunners,
      Future<Object> optimizer
  )
  {
    return new ChainedExecutionQueryRunner<Result<SearchResultValue>>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<SearchResultValue>, SearchQuery> getToolchest()
  {
    return toolChest;
  }

  @Override
  public Future<Object> preFactoring(
      SearchQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return null;
  }
}
