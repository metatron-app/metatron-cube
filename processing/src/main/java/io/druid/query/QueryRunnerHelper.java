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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Cursor;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class QueryRunnerHelper
{
  private static final Logger log = new Logger(QueryRunnerHelper.class);

  public static Aggregator[] makeAggregators(Cursor cursor, List<AggregatorFactory> aggregatorSpecs)
  {
    Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
    int aggregatorIndex = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      aggregators[aggregatorIndex] = spec.factorize(cursor);
      ++aggregatorIndex;
    }
    return aggregators;
  }

  public static String[] makeAggregatorNames(List<AggregatorFactory> aggregatorSpecs)
  {
    String[] aggregators = new String[aggregatorSpecs.size()];
    int aggregatorIndex = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      aggregators[aggregatorIndex++] = spec.getName();
    }
    return aggregators;
  }

  public static <T> Sequence<Result<T>> makeCursorBasedQuery(
      StorageAdapter adapter,
      List<Interval> queryIntervals,
      RowResolver resolver,
      DimFilter filter,
      Cache cache,
      boolean descending,
      Granularity granularity,
      final Function<Cursor, Result<T>> mapFn
  )
  {
    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    Interval interval = Iterables.getOnlyElement(queryIntervals);
    return makeCursorBasedQuery(adapter, interval, resolver, filter, cache, descending, granularity, mapFn);
  }

  public static <T> Sequence<T> makeCursorBasedQuery(
      final StorageAdapter adapter,
      Interval interval,
      RowResolver resolver,
      DimFilter filter,
      Cache cache,
      boolean descending,
      Granularity granularity,
      final Function<Cursor, T> mapFn
  )
  {
    return Sequences.filter(
        Sequences.map(
            adapter.makeCursors(filter, interval, resolver, granularity, cache, descending),
            new Function<Cursor, T>()
            {
              @Override
              public T apply(Cursor input)
              {
                log.debug("Running over cursor[%s]", adapter.getInterval(), input.getTime());
                return mapFn.apply(input);
              }
            }
        ),
        Predicates.<T>notNull()
    );
  }

  public static <T>  QueryRunner<T> makeClosingQueryRunner(final QueryRunner<T> runner, final Closeable closeable){
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return new ResourceClosingSequence<>(runner.run(query, responseContext), closeable);
      }
    };
  }

  public static <T> QueryRunner<T> toManagementRunner(
      Query<T> query,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService exec,
      ObjectMapper mapper
  )
  {
    QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    exec = exec == null ? MoreExecutors.sameThreadExecutor() : exec;
    return FinalizeResultsQueryRunner.finalize(
        toolChest.mergeResults(
            factory.mergeRunners(exec, Arrays.asList(factory.createRunner(null, null)), null)
        ),
        toolChest,
        mapper
    );
  }
}
