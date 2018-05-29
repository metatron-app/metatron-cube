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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class StreamRawQueryRunnerFactory
    implements QueryRunnerFactory.Splitable<RawRows, StreamRawQuery>
{
  private static final Logger logger = new Logger(StreamRawQueryRunnerFactory.class);

  private final StreamRawQueryToolChest toolChest;
  private final StreamQueryEngine engine;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public StreamRawQueryRunnerFactory(StreamRawQueryToolChest toolChest, StreamQueryEngine engine)
  {
    this(toolChest, engine, null);
  }

  public StreamRawQueryRunnerFactory(StreamRawQueryToolChest toolChest, StreamQueryEngine engine, Cache cache)
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.cache = cache;
  }

  @Override
  public Future<Object> preFactoring(
      StreamRawQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Futures.<Object>immediateFuture(new MutableInt(0));
  }

  @Override
  public Iterable<StreamRawQuery> splitQuery(
      StreamRawQuery query,
      List<Segment> segments,
      Future<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker,
      ObjectMapper mapper
  )
  {
    int numSplit = query.getContextInt(Query.RAW_LOCAL_SPLIT_NUM, 5);
    if (GuavaUtils.isNullOrEmpty(query.getSortOn()) || numSplit < 2) {
      return null;
    }
    String sortColumn = query.getSortOn().get(0);
    final Object[] values = Queries.makeColumnHistogramOn(
        resolver,
        segmentWalker,
        mapper,
        query.asTimeseriesQuery(),
        DefaultDimensionSpec.of(sortColumn),
        numSplit
    );
    if (values == null || values.length < 3) {
      return null;
    }
    logger.info("--> values : %s", Arrays.toString(values));

    List<StreamRawQuery> splits = Lists.newArrayList();
    for (int i = 1; i < values.length; i++) {
      BoundDimFilter filter;
      if (i == 1) {
        filter = BoundDimFilter.lt(sortColumn, values[i]);
      } else if (i < values.length - 1) {
        filter = BoundDimFilter.between(sortColumn, values[i - 1], values[i]);
      } else {
        filter = BoundDimFilter.gte(sortColumn, values[i - 1]);
      }
      logger.info("--> filter : %s ", filter);
      splits.add(
          query.withDimFilter(DimFilters.and(query.getDimFilter(), filter))
      );
    }
    return splits;
  }

  @Override
  public QueryRunner<RawRows> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<RawRows>()
    {
      @Override
      public Sequence<RawRows> run(Query<RawRows> query, Map<String, Object> responseContext)
      {
        return engine.process((StreamRawQuery) query, segment, optimizer, cache);
      }
    };
  }

  @Override
  public QueryRunner<RawRows> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<RawRows>> queryRunners,
      Future<Object> optimizer
  )
  {
    return new QueryRunner<RawRows>()
    {
      @Override
      public Sequence<RawRows> run(final Query<RawRows> query, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            Iterables.transform(
                queryRunners,
                new Function<QueryRunner<RawRows>, Sequence<RawRows>>()
                {
                  @Override
                  public Sequence<RawRows> apply(final QueryRunner<RawRows> input)
                  {
                    return new LazySequence<RawRows>(
                        new Supplier<Sequence<RawRows>>()
                        {
                          @Override
                          public Sequence<RawRows> get()
                          {
                            return input.run(query, responseContext);
                          }
                        }
                    );
                  }
                }
            )
        );
      }
    };
  }

  @Override
  public QueryToolChest<RawRows, StreamRawQuery> getToolchest()
  {
    return toolChest;
  }
}
