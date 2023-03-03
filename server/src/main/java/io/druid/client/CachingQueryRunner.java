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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class CachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(CachingQueryRunner.class);
  private final byte[] segmentIdentifier;
  private final SegmentDescriptor segmentDescriptor;
  private final QueryRunner<T> base;
  private final QueryToolChest<T> toolChest;
  private final Cache cache;
  private final ObjectMapper mapper;
  private final CacheConfig cacheConfig;
  private final ListeningExecutorService backgroundExecutorService;

  public CachingQueryRunner(
      String segmentIdentifier,
      SegmentDescriptor segmentDescriptor,
      ObjectMapper mapper,
      Cache cache,
      QueryToolChest<T> toolchest,
      QueryRunner<T> base,
      ExecutorService backgroundExecutorService,
      CacheConfig cacheConfig
  )
  {
    this.base = base;
    this.segmentIdentifier = StringUtils.toUtf8(segmentIdentifier);
    this.segmentDescriptor = segmentDescriptor;
    this.toolChest = toolchest;
    this.cache = cache;
    this.mapper = mapper;
    this.backgroundExecutorService = MoreExecutors.listeningDecorator(backgroundExecutorService);
    this.cacheConfig = cacheConfig;
  }

  @Override
  public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
  {
    final boolean queryCacheable = cacheConfig.isQueryCacheable(query);
    final boolean useCache = BaseQuery.isUseCache(query, true)
                             && cacheConfig.isUseCache()
                             && queryCacheable;
    final boolean populateCache = BaseQuery.isPopulateCache(query, true)
                                  && cacheConfig.isPopulateCache()
                                  && queryCacheable;

    if (!useCache && !populateCache) {
      return base.run(query, responseContext);
    }

    final CacheStrategy<T, Object, Query<T>> strategy = toolChest.getCacheStrategyIfExists(query);
    if (strategy == null) {
      return base.run(query, responseContext);
    }

    final Cache.NamedKey key = createKey(query, strategy);
    if (key == null) {
      return base.run(query, responseContext);
    }

    final List<String> columns = query.estimatedInitialColumns();

    if (useCache) {
      final byte[] cachedResult = cache.get(key);
      if (cachedResult != null) {
        if (cachedResult.length == 0) {
          return Sequences.empty(columns);
        }
        final TypeReference<?> cacheObjectClazz = strategy.getCacheObjectClazz();

        final Sequence<Object> sequence = Sequences.simple(() -> {
          try {
            return mapper.readValues(
                mapper.getFactory().createParser(cachedResult),
                cacheObjectClazz
            );
          }
          catch (Exception e) {
            throw QueryException.wrapIfNeeded(e);
          }
        });
        return Sequences.map(columns, sequence, strategy.pullFromCache(query));
      }
    }

    if (populateCache) {
      final CachePopulator populator = new CachePopulator(cache, mapper, key);
      final Function<T, Object> cacheFn = strategy.prepareForCache(query);

      return Sequences.withEffect(
          Sequences.map(
              columns,
              base.run(query, responseContext),
              new Function<T, T>()
              {
                @Override
                public T apply(final T input)
                {
                  populator.write(input, cacheFn);
                  return input;
                }
              }
          ),
          populator,
          backgroundExecutorService
      );
    } else {
      return base.run(query, responseContext);
    }
  }

  private Cache.NamedKey createKey(Query<T> query, CacheStrategy<T, Object, Query<T>> strategy)
  {
    final byte[] queryKey = strategy.computeCacheKey(query, cacheConfig.getKeyLimit());
    if (queryKey != null) {
      return CacheUtil.computeSegmentCacheKey(segmentIdentifier, segmentDescriptor, queryKey);
    }
    return null;
  }
}
