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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
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
    List<DimensionSpec> dimensions = query.getDimensions();
    VirtualColumns vcs = VirtualColumns.valueOf(query.getVirtualColumns());

    List<ValueType> prev = null;
    for (Segment segment : segments) {
      List<ValueType> types = Lists.newArrayList();
      RowResolver resolver = RowResolver.of(segment, vcs);
      for (DimensionSpec dimensionSpec : dimensions) {
        ValueDesc type = dimensionSpec.resolveType(resolver);
        if (ValueDesc.isDimension(type)) {
          types.add(ValueType.STRING);
        } else if (ValueDesc.isPrimitive(type)) {
          types.add(type.type());
        } else {
          throw new ISE("cannot group-by on non-primitive type %s [%s]", type, dimensionSpec);
        }
      }
      if (prev != null && !prev.equals(types)) {
        throw new ISE("cannot group-by on conflicting types %s vs %s", prev, types);
      }
      prev = types;
    }
    return Futures.<Object>immediateFuture(prev);
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
      this.adapter = segment.asStorageAdapter(true);
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

    @Override
    public String toString() {
      return adapter.toString();
    }
  }
}
