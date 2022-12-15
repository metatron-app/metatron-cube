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

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.cache.SessionCache;
import io.druid.segment.Segment;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * An interface that defines the nitty gritty implementation details of a Query on a Segment
 */
public interface QueryRunnerFactory<T, QueryType extends Query<T>>
{
  /**
   * @return
   */
  default Supplier<Object> preFactoring(
      QueryType query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  ) {
    return null;
  }

  /**
   * Given a specific segment, this method will create a QueryRunner.
   *
   * The QueryRunner, when asked, will generate a Sequence of results based on the given segment.  This
   * is the meat of the query processing and is where the results are actually generated.  Everything else
   * is just merging and reduction logic.
   *
   * @param segment The segment to process
   * @param optimizer optimization object
   * @return A QueryRunner that, when asked, will generate a Sequence of results based on the given segment
   */
  QueryRunner<T> _createRunner(Segment segment, Supplier<Object> optimizer);

  default QueryRunner<T> createRunner(final Segment segment, final Supplier<Object> optimizer)
  {
    final QueryRunner<T> runner = _createRunner(segment, optimizer);    // eager instantiate
    return (query, responseContext) -> runner.run(BaseQuery.specialize(query, segment), responseContext);
  }

  /**
   * Runners generated with createRunner() and combined into an Iterable in (time,shardId) order are passed
   * along to this method with an ExecutorService.  The method should then return a QueryRunner that, when
   * asked, will use the ExecutorService to run the base QueryRunners in some fashion.
   *
   * The vast majority of the time, this should be implemented with
   *
   *     return new ChainedExecutionQueryRunner<>(
   *         queryExecutor, toolChest.getOrdering(), queryWatcher, queryRunners
   *     );
   *
   * Which will allow for parallel execution up to the maximum number of processing threads allowed.
   *
   *
   * @param query
   * @param queryExecutor ExecutorService to be used for parallel processing
   * @param queryRunners Individual QueryRunner objects that produce some results
   * @param optimizer optimization object
   * @return a QueryRunner that, when asked, will use the ExecutorService to run the base QueryRunners
   */
  QueryRunner<T> mergeRunners(
      Query<T> query,
      ExecutorService queryExecutor,
      Iterable<QueryRunner<T>> queryRunners,
      Supplier<Object> optimizer
  );

  /**
   * Provides access to the toolchest for this specific query type.
   *
   * @return an instance of the toolchest for this specific query type.
   */
  QueryToolChest<T, QueryType> getToolchest();

  interface Splitable<T, QueryType extends Query<T>> extends QueryRunnerFactory<T, QueryType>
  {
    List<List<Segment>> splitSegments(
        QueryType query,
        List<Segment> targets,
        Supplier<Object> optimizer,
        Supplier<RowResolver> resolver,
        QuerySegmentWalker segmentWalker
    );

    List<QueryType> splitQuery(
        QueryType query,
        List<Segment> targets,
        Supplier<Object> optimizer,
        Supplier<RowResolver> resolver,
        QuerySegmentWalker segmentWalker
    );
  }

  abstract class Abstract<T, QueryType extends Query<T>> implements QueryRunnerFactory<T, QueryType>
  {
    protected final QueryToolChest<T, QueryType> toolChest;
    protected final QueryWatcher queryWatcher;

    @BitmapCache
    @Inject(optional = true)
    private Cache cache;

    protected SessionCache cache(Query<?> query)
    {
      return queryWatcher.getSessionCache(query.getId()).wrap(cache);
    }

    protected Abstract(QueryToolChest<T, QueryType> toolChest, QueryWatcher queryWatcher)
    {
      this.toolChest = toolChest;
      this.queryWatcher = queryWatcher;
    }

    @Override
    public QueryRunner<T> mergeRunners(
        final Query<T> query,
        final ExecutorService queryExecutor,
        final Iterable<QueryRunner<T>> runners,
        final Supplier<Object> optimizer
    )
    {
      return QueryRunners.executeParallel(query, queryExecutor, Lists.newArrayList(runners), queryWatcher);
    }

    @Override
    public final QueryToolChest<T, QueryType> getToolchest()
    {
      return toolChest;
    }

    @Override
    public final QueryRunner<T> _createRunner(Segment segment, Supplier<Object> optimizer)
    {
      return (query, response) -> _createRunner(segment, optimizer, cache(query)).run(query, response);
    }

    protected abstract QueryRunner<T> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache);
  }
}
