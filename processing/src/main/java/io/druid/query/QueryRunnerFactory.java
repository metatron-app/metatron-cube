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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.data.Pair;
import io.druid.segment.Segment;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * An interface that defines the nitty gritty implementation details of a Query on a Segment
 */
public interface QueryRunnerFactory<T, QueryType extends Query<T>>
{
  /**
   */
  public Future<Object> preFactoring(
      QueryType query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  );

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
  public QueryRunner<T> createRunner(Segment segment, Future<Object> optimizer);

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
   * @param queryExecutor ExecutorService to be used for parallel processing
   * @param queryRunners Individual QueryRunner objects that produce some results
   * @param optimizer optimization object
   * @return a QueryRunner that, when asked, will use the ExecutorService to run the base QueryRunners
   */
  public QueryRunner<T> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<T>> queryRunners,
      Future<Object> optimizer
  );

  /**
   * Provides access to the toolchest for this specific query type.
   *
   * @return an instance of the toolchest for this specific query type.
   */
  public QueryToolChest<T, QueryType> getToolchest();

  interface Splitable<T, QueryType extends Query<T>> extends QueryRunnerFactory<T, QueryType>
  {
    public List<List<Segment>> splitSegments(
        QueryType query,
        List<Segment> targets,
        Future<Object> optimizer,
        Supplier<RowResolver> resolver,
        QuerySegmentWalker segmentWalker,
        ObjectMapper mapper
    );

    public Iterable<QueryType> splitQuery(
        QueryType query,
        List<Segment> targets,
        Future<Object> optimizer,
        Supplier<RowResolver> resolver,
        QuerySegmentWalker segmentWalker,
        ObjectMapper mapper
    );
  }

  abstract class Abstract<T, QueryType extends Query<T>> implements QueryRunnerFactory<T, QueryType>
  {
    protected final QueryToolChest<T, QueryType> toolChest;
    protected final QueryWatcher queryWatcher;

    @BitmapCache
    @Inject(optional = true)
    protected Cache cache;

    protected Abstract(QueryToolChest<T, QueryType> toolChest, QueryWatcher queryWatcher)
    {
      this.toolChest = toolChest;
      this.queryWatcher = queryWatcher;
    }

    @Override
    public Future<Object> preFactoring(
        QueryType query,
        List<Segment> segments,
        Supplier<RowResolver> resolver,
        ExecutorService exec
    )
    {
      return null;
    }

    @Override
    public final QueryToolChest<T, QueryType> getToolchest()
    {
      return toolChest;
    }
  }
}
