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

package io.druid.query.groupby;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.data.GenericIndexed;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
  private static final Logger log = new Logger(GroupByQueryRunnerFactory.class);

  private static final int PRE_OPTIMIZE_THRESHOLD = 4;

  private final GroupByQueryEngine engine;
  private final QueryWatcher queryWatcher;
  private final Supplier<GroupByQueryConfig> config;
  private final GroupByQueryQueryToolChest toolChest;
  private final StupidPool<ByteBuffer> computationBufferPool;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine,
      QueryWatcher queryWatcher,
      Supplier<GroupByQueryConfig> config,
      GroupByQueryQueryToolChest toolChest,
      @Global StupidPool<ByteBuffer> computationBufferPool
  )
  {
    this(engine, queryWatcher, config, toolChest, computationBufferPool, null);
  }

  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine,
      QueryWatcher queryWatcher,
      Supplier<GroupByQueryConfig> config,
      GroupByQueryQueryToolChest toolChest,
      @Global StupidPool<ByteBuffer> computationBufferPool,
      Cache cache
  )
  {
    this.engine = engine;
    this.queryWatcher = queryWatcher;
    this.config = config;
    this.toolChest = toolChest;
    this.computationBufferPool = computationBufferPool;
    this.cache = cache;
  }

  @Override
  public Object preFactoring(GroupByQuery query, List<Segment> segments)
  {
    if (segments.size() < PRE_OPTIMIZE_THRESHOLD) {
      return null;
    }
    log.info("Starting optimization with %d segments", segments.size());
    long start = System.currentTimeMillis();
    final List<String> dimensionNames = Lists.newArrayList();
    for (DimensionSpec dimension : query.getDimensions()) {
      if (!(dimension instanceof DefaultDimensionSpec)) {
        return null;
      }
      dimensionNames.add(((DefaultDimensionSpec)dimension).getDimension());
    }
    List<QueryableIndex> indices = Lists.newArrayList();
    for (Segment segment : segments) {
      QueryableIndex index = segment.asQueryableIndex();
      if (index == null) {
        return null;
      }
      indices.add(index);
    }
    final Map<String, Map<String, Integer>> dictionaries = Maps.newLinkedHashMap();
    for (String dimensionName : dimensionNames) {
      Map<String, Integer> dictionary = null;
      for (QueryableIndex index : indices) {
        Column column = index.getColumn(dimensionName);
        if (column == null || !column.getCapabilities().isDictionaryEncoded()) {
          dictionaries.clear();
          return null;
        }
        GenericIndexed<String> encoded = column.getDictionary();
        if (dictionary == null) {
          dictionary = Maps.newHashMapWithExpectedSize(encoded.size() << 1);
        }
        final Map<String, Integer> current = dictionary;
        final Function<String, Integer> id = new Function<String, Integer>()
        {
          @Override
          public Integer apply(String s)
          {
            return current.size();
          }
        };
        for (String word : encoded.loadFully()) {
          current.computeIfAbsent(word, id);
        }
      }
      if (dictionary != null) {
        log.info("Dictionary for %s = %d", dimensionName, dictionary.size());
        dictionaries.put(dimensionName, dictionary);
      }
    }
    log.info("Optimization took %,d msec", (System.currentTimeMillis() - start));
    return dictionaries;
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment, final Object optimizer)
  {
    return new GroupByQueryRunner(segment, engine, cache);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      final ExecutorService exec,
      final Iterable<QueryRunner<Row>> queryRunners,
      final Object optimizer
  )
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);
    return new GroupByMergedQueryRunner<Row>(
        queryExecutor, config, queryWatcher, computationBufferPool, queryRunners, optimizer
    );
  }

  @Override
  public QueryToolChest<Row, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<Row>
  {
    private final StorageAdapter adapter;
    private final GroupByQueryEngine engine;
    private final Cache cache;

    public GroupByQueryRunner(Segment segment, GroupByQueryEngine engine, Cache cache)
    {
      this.adapter = segment.asStorageAdapter();
      this.engine = engine;
      this.cache = cache;
    }

    @Override
    public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
    {
      if (!(input instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
      }

      return engine.process((GroupByQuery) input, adapter, cache);
    }
  }
}
