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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.data.DictionaryLoader;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.filter.Filters;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
  public Future<Object> preFactoring(GroupByQuery query, List<Segment> segments, ExecutorService exec)
  {
    if (segments.size() < PRE_OPTIMIZE_THRESHOLD) {
      return null;
    }
    if (!DefaultDimensionSpec.isAllDefault(query.getDimensions())) {
      return null;
    }

    final long start = System.currentTimeMillis();
    log.info("Initializing group-by optimizer with target %d segments", segments.size());

    Filter filter = null;
    String filterDim = null;
    DimFilter dimFilter = query.getDimFilter();
    if (dimFilter != null) {
      filter = Filters.toFilter(dimFilter);
      if (filter.supportsBitmap()) {
        Set<String> dependents = Filters.getDependents(dimFilter);
        if (dependents.size() == 1) {
          filterDim = Iterables.getOnlyElement(dependents);
          log.info("Using filtered loader for dimension %s: %s", filterDim, dimFilter);
        }
      }
    }

    final Map<String, List<DictionaryLoader<String>>> columns = Maps.newLinkedHashMap();
    for (Segment segment : segments) {
      QueryableIndex index = segment.asQueryableIndex();
      if (index == null) {
        return null;
      }
      for (DimensionSpec dimension : query.getDimensions()) {
        String dimensionName = dimension.getDimension();
        Column column = index.getColumn(dimensionName);
        if (column == null || !column.getCapabilities().isDictionaryEncoded()) {
          return null;
        }
        List<DictionaryLoader<String>> dictionaries = columns.get(dimension.getOutputName());
        if (dictionaries == null) {
          columns.put(dimension.getOutputName(), dictionaries = Lists.newArrayList());
        }
        final GenericIndexed<String> dictionary = column.getDictionary();
        if (dimensionName.equals(filterDim)) {
          final ImmutableBitmap bitmap = filter.getValueBitmap(
              new ColumnSelectorBitmapIndexSelector(index.getBitmapFactoryForDimensions(), dimensionName, column)
          );
          if (bitmap == null || bitmap.size() == dictionary.size()) {
            dictionaries.add(dictionary);
            continue;
          }
          log.debug(
              "Applied filter on segment %s, reducing dictionary %d to %d",
              segment.getIdentifier(),
              dictionary.size(),
              bitmap.size()
          );
          dictionaries.add(
              new DictionaryLoader<String>() {

                @Override
                public int size()
                {
                  return bitmap.size();
                }

                @Override
                public Collection<String> loadFully()
                {
                  final IntIterator iterator = bitmap.iterator();
                  final List<String> values = Lists.newArrayListWithCapacity(bitmap.size());
                  while (iterator.hasNext()) {
                    values.add(dictionary.get(iterator.next()));
                  }
                  return values;
                }
              }
          );
        } else {
          dictionaries.add(dictionary);
        }
      }
    }

    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(exec);

    final Map<String, Future<String[]>> optimizer = Maps.newLinkedHashMap();
    for (Map.Entry<String, List<DictionaryLoader<String>>> entry : columns.entrySet()) {
      final String outputName = entry.getKey();
      final Set<String> merged = Sets.newConcurrentHashSet();
      final List<ListenableFuture<Integer>> elements = Lists.newArrayList();
      for (final DictionaryLoader<String> dictionary : entry.getValue()) {
        elements.add(
            executor.submit(
                new AbstractPrioritizedCallable<Integer>(0)
                {
                  @Override
                  public Integer call()
                  {
                    for (String value : dictionary.loadFully()) {
                      merged.add(Strings.nullToEmpty(value));
                    }
                    return dictionary.size();
                  }
                }
            )
        );
      }
      final SettableFuture<String[]> sorted = SettableFuture.create();
      final ListenableFuture<List<Integer>> future = Futures.allAsList(elements);
      future.addListener(
          new Runnable()
          {
            @Override
            public void run()
            {
              int counter = 0;
              try {
                for (Integer merging : Futures.getUnchecked(future)) {
                  counter += merging;
                }
                final String[] array = merged.toArray(new String[merged.size()]);
                Arrays.sort(array);
                sorted.set(array);
                log.info(
                    "Merged %,d words into %,d dictionary in %,d msec",
                    counter, merged.size(), (System.currentTimeMillis() - start)
                );
              }
              catch (Throwable t) {
                sorted.setException(t); // propagate exception
              }
            }
          },
          MoreExecutors.sameThreadExecutor()
      );
      optimizer.put(outputName, sorted);
    }

    return Futures.<Object>immediateFuture(
        Maps.transformValues(
            optimizer, new Function<Future<String[]>, String[]>()
            {
              @Override
              public String[] apply(Future<String[]> input)
              {
                return Futures.getUnchecked(input);
              }
            }
        )
    );
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new GroupByQueryRunner(segment, engine, cache);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      final ExecutorService exec,
      final Iterable<QueryRunner<Row>> queryRunners,
      final Future<Object> optimizer
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
