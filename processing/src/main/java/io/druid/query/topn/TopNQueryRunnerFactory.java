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

package io.druid.query.topn;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.guice.annotations.Global;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumns;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class TopNQueryRunnerFactory extends QueryRunnerFactory.Abstract<Result<TopNResultValue>, TopNQuery>
{
  private final TopNQueryEngine queryEngine;

  @Inject
  public TopNQueryRunnerFactory(
      @Global StupidPool<ByteBuffer> computationBufferPool,
      TopNQueryQueryToolChest toolchest,
      QueryWatcher queryWatcher
  )
  {
    super(toolchest, queryWatcher);
    this.queryEngine = new TopNQueryEngine(computationBufferPool);
  }

  @Override
  public Supplier<Object> preFactoring(
      TopNQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    if (!GuavaUtils.isNullOrEmpty(query.getVirtualColumns())) {
      VirtualColumns.assertDimensionIndexed(resolver.get(), query.getDimensionSpec());
    }
    return null;
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> _createRunner(Segment segment, Supplier<Object> optimizer, Cache cache)
  {
    return (query, response) -> queryEngine.query((TopNQuery) query, segment, cache);
  }
}
