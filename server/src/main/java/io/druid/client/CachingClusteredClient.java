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
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.StringUtils;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.EmittingLogger;
import io.druid.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.client.selector.TierSelectorStrategy;
import io.druid.common.guava.FutureSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.FilterableManagementQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.server.ServiceTypes;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;

/**
 */
public class CachingClusteredClient<T> implements QueryRunner<T>
{
  private static final EmittingLogger log = new EmittingLogger(CachingClusteredClient.class);

  private final ServiceDiscovery<String> discovery;
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final TierSelectorStrategy tierSelectorStrategy;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final QueryConfig queryConfig;
  private final CacheConfig cacheConfig;
  private final ListeningExecutorService executorService;
  private final ListeningExecutorService backgroundExecutorService;

  @Inject
  public CachingClusteredClient(
      ServiceDiscovery<String> discovery,
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      TierSelectorStrategy tierSelectorStrategy,
      Cache cache,
      @Smile ObjectMapper objectMapper,
      @Processing ExecutorService executorService,
      @BackgroundCaching ExecutorService backgroundExecutorService,
      QueryConfig queryConfig,
      CacheConfig cacheConfig
  )
  {
    this.discovery = discovery;
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.cache = cache;
    this.objectMapper = objectMapper;
    this.queryConfig = queryConfig;
    this.cacheConfig = cacheConfig;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.backgroundExecutorService = MoreExecutors.listeningDecorator(backgroundExecutorService);

    serverView.registerSegmentCallback(
        Execs.singleThreaded("CCClient-ServerView-CB-%d"),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            CachingClusteredClient.this.cache.close(segment.getIdentifier());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    if (query instanceof Query.ManagementQuery) {
      try {
        List<Sequence<T>> sequences = Lists.newArrayList();
        for (DruidServer server : getManagementTargets(query)) {
          QueryRunner<T> runner = serverView.getQueryRunner(query, server);
          if (runner != null) {
            sequences.add(runner.run(query, responseContext));
          }
        }
        return QueryUtils.mergeSort(query, sequences);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    final CacheStrategy<T, Object, Query<T>> strategy = toolChest.getCacheStrategyIfExists(query);
    final Map<DruidServer, List<SegmentDescriptor>> serverSegments = Maps.newTreeMap();

    final List<Pair<Interval, byte[]>> cachedResults = Lists.newArrayList();
    final Map<String, CachePopulator> cachePopulatorMap = Maps.newHashMap();

    final boolean useCache = BaseQuery.isUseCache(query, true)
                             && strategy != null
                             && cacheConfig.isUseCache()
                             && cacheConfig.isQueryCacheable(query);
    final boolean populateCache = BaseQuery.isPopulateCache(query, true)
                                  && strategy != null
                                  && cacheConfig.isPopulateCache()
                                  && cacheConfig.isQueryCacheable(query);
    final boolean explicitBySegment = BaseQuery.isBySegment(query);


    final ImmutableMap.Builder<String, Object> contextBuilder = new ImmutableMap.Builder<>();

    final int priority = BaseQuery.getContextPriority(query, 0);
    contextBuilder.put(Query.PRIORITY, priority);

    if (populateCache) {
      // prevent down-stream nodes from caching results as well if we are populating the cache
      contextBuilder.put(Query.POPULATE_CACHE, false);
      contextBuilder.put(Query.BY_SEGMENT, true);
    }

    TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());

    if (timeline == null) {
      return Sequences.empty();
    }

    // build set of segments to query
    Set<Pair<ServerSelector, SegmentDescriptor>> segments = Sets.newLinkedHashSet();

    List<TimelineObjectHolder<String, ServerSelector>> serversLookup = Lists.newLinkedList();

    // Note that enabling this leads to putting uncovered intervals information in the response headers
    // and might blow up in some cases https://github.com/druid-io/druid/issues/2108
    int uncoveredIntervalsLimit = BaseQuery.getContextUncoveredIntervalsLimit(query, 0);

    if (uncoveredIntervalsLimit > 0) {
      List<Interval> uncoveredIntervals = Lists.newArrayListWithCapacity(uncoveredIntervalsLimit);
      boolean uncoveredIntervalsOverflowed = false;

      for (Interval interval : query.getIntervals()) {
        Iterable<TimelineObjectHolder<String, ServerSelector>> lookup = timeline.lookup(interval);
        long startMillis = interval.getStartMillis();
        long endMillis = interval.getEndMillis();
        for (TimelineObjectHolder<String, ServerSelector> holder : lookup) {
          Interval holderInterval = holder.getInterval();
          long intervalStart = holderInterval.getStartMillis();
          if (!uncoveredIntervalsOverflowed && startMillis != intervalStart) {
            if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
              uncoveredIntervals.add(new Interval(startMillis, intervalStart));
            } else {
              uncoveredIntervalsOverflowed = true;
            }
          }
          startMillis = holderInterval.getEndMillis();
          serversLookup.add(holder);
        }

        if (!uncoveredIntervalsOverflowed && startMillis < endMillis) {
          if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
            uncoveredIntervals.add(new Interval(startMillis, endMillis));
          } else {
            uncoveredIntervalsOverflowed = true;
          }
        }
      }

      if (!uncoveredIntervals.isEmpty()) {
        // This returns intervals for which NO segment is present.
        // Which is not necessarily an indication that the data doesn't exist or is
        // incomplete. The data could exist and just not be loaded yet.  In either
        // case, though, this query will not include any data from the identified intervals.
        responseContext.put("uncoveredIntervals", uncoveredIntervals);
        responseContext.put("uncoveredIntervalsOverflowed", uncoveredIntervalsOverflowed);
      }
    } else {
      for (Interval interval : query.getIntervals()) {
        Iterables.addAll(serversLookup, timeline.lookup(interval));
      }
    }

    // Let tool chest filter out unneeded segments
    final List<TimelineObjectHolder<String, ServerSelector>> filteredServersLookup =
        toolChest.filterSegments(query, serversLookup);

    // minor quick-path
    if (query instanceof SegmentMetadataQuery && ((SegmentMetadataQuery) query).analyzingOnlyInterval()) {
      @SuppressWarnings("unchecked")
      Sequence<T> sequence = (Sequence<T>) Sequences.simple(
          Arrays.asList(
              new SegmentAnalysis(
                  JodaUtils.condenseIntervals(
                      Iterables.transform(
                          filteredServersLookup,
                          new Function<TimelineObjectHolder<String, ServerSelector>, Interval>()
                          {
                            @Override
                            public Interval apply(TimelineObjectHolder<String, ServerSelector> input)
                            {
                              return input.getInterval();
                            }
                          }
                      )
                  )
              )
          )
      );
      return sequence;
    }

    for (TimelineObjectHolder<String, ServerSelector> holder : filteredServersLookup) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        ServerSelector selector = chunk.getObject();
        final SegmentDescriptor descriptor = new SegmentDescriptor(
            holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
        );

        segments.add(Pair.of(selector, descriptor));
      }
    }

    final byte[] queryCacheKey;

    if ((populateCache || useCache) // implies strategy != null
        && !explicitBySegment) // explicit bySegment queries are never cached
    {
      queryCacheKey = strategy.computeCacheKey(query);
    } else {
      queryCacheKey = null;
    }

    if (queryCacheKey != null) {
      // cachKeys map must preserve segment ordering, in order for shards to always be combined in the same order
      Map<Pair<ServerSelector, SegmentDescriptor>, Cache.NamedKey> cacheKeys = Maps.newLinkedHashMap();
      for (Pair<ServerSelector, SegmentDescriptor> segment : segments) {
        final Cache.NamedKey segmentCacheKey = CacheUtil.computeSegmentCacheKey(
            StringUtils.toUtf8(segment.lhs.getSegment().getIdentifier()),
            segment.rhs,
            queryCacheKey
        );
        cacheKeys.put(segment, segmentCacheKey);
      }

      // Pull cached segments from cache and remove from set of segments to query
      final Map<Cache.NamedKey, byte[]> cachedValues;
      if (useCache) {
        long start = System.currentTimeMillis();
        cachedValues = cache.getBulk(Iterables.limit(cacheKeys.values(), cacheConfig.getCacheBulkMergeLimit()));
        int total = 0;
        for (byte[] value : cachedValues.values()) {
          total += value.length;
        }
        log.debug(
            "Requested %,d segments from cache for [%s] and returned %,d segments (%,d bytes), took %,d msec",
            cacheKeys.size(), query.getType(), cachedValues.size(), total, System.currentTimeMillis() - start
        );
      } else {
        cachedValues = ImmutableMap.of();
      }

      for (Map.Entry<Pair<ServerSelector, SegmentDescriptor>, Cache.NamedKey> entry : cacheKeys.entrySet()) {
        Pair<ServerSelector, SegmentDescriptor> segment = entry.getKey();
        Cache.NamedKey segmentCacheKey = entry.getValue();
        final Interval segmentQueryInterval = segment.rhs.getInterval();

        final byte[] cachedValue = cachedValues.get(segmentCacheKey);
        if (cachedValue != null) {
          // remove cached segment from set of segments to query
          segments.remove(segment);
          cachedResults.add(Pair.of(segmentQueryInterval, cachedValue));
        } else if (populateCache) {
          // otherwise, if populating cache, add segment to list of segments to cache
          final String segmentIdentifier = segment.lhs.getSegment().getIdentifier();
          cachePopulatorMap.put(
              String.format("%s_%s", segmentIdentifier, segmentQueryInterval),
              new CachePopulator(cache, objectMapper, segmentCacheKey)
          );
        }
      }
    }

    Predicate<QueryableDruidServer> predicate = null;
    if (queryConfig.isUseHistoricalNodesOnlyForLuceneFilter()
        && DimFilters.hasAnyLucene(BaseQuery.getDimFilter(query))) {
      predicate = new Predicate<QueryableDruidServer>()
      {
        @Override
        public boolean apply(QueryableDruidServer input)
        {
          return ServiceTypes.HISTORICAL.equals(input.getServer().getType());
        }
      };
    }

    // Compile list of all segments not pulled from cache
    for (Pair<ServerSelector, SegmentDescriptor> segment : segments) {
      final QueryableDruidServer queryableDruidServer = segment.lhs.pick(tierSelectorStrategy, predicate);

      if (queryableDruidServer == null) {
        log.makeAlert(
            "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
            segment.rhs,
            query.getDataSource()
        ).emit();
      } else {
        final DruidServer server = queryableDruidServer.getServer();
        List<SegmentDescriptor> descriptors = serverSegments.get(server);

        if (descriptors == null) {
          descriptors = Lists.newArrayList();
          serverSegments.put(server, descriptors);
        }

        descriptors.add(segment.rhs);
      }
    }

    return new Supplier<Sequence<T>>()
    {
      @Override
      public Sequence<T> get()
      {
        final ToIntFunction counter = toolChest.numRows(query);
        final CacheAccessor cacheAccessor = strategy == null ? null : new CacheAccessor(counter, strategy.pullFromCache());

        ArrayList<Sequence<T>> sequencesByInterval = Lists.newArrayList();
        if (cacheAccessor != null && !cachedResults.isEmpty()) {
          addSequencesFromCache(sequencesByInterval, cacheAccessor);
        }
        addSequencesFromServer(sequencesByInterval);

        return mergeCachedAndUncachedSequences(
            query,
            toolChest,
            sequencesByInterval,
            cachedResults.size(),
            cacheAccessor
        );
      }

      private void addSequencesFromCache(
          final ArrayList<Sequence<T>> listOfSequences,
          final CacheAccessor cacheAccessor
      )
      {
        final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
        for (Pair<Interval, byte[]> cachedResultPair : cachedResults) {
          final byte[] cachedResult = cachedResultPair.rhs;
          Sequence<Object> cachedSequence = Sequences.simple(
              new Iterable<Object>()
              {
                @Override
                public Iterator<Object> iterator()
                {
                  if (cachedResult.length == 0) {
                    return Iterators.emptyIterator();
                  }
                  long prev = System.currentTimeMillis();
                  try {
                    return objectMapper.readValues(
                        objectMapper.getFactory().createParser(cachedResult),
                        cacheObjectClazz
                    );
                  }
                  catch (IOException e) {
                    throw Throwables.propagate(e);
                  }
                  finally {
                    cacheAccessor.addTime(System.currentTimeMillis() - prev);
                  }
                }
              }
          );
          listOfSequences.add(Sequences.map(cachedSequence, cacheAccessor));
        }
      }

      private void addSequencesFromServer(ArrayList<Sequence<T>> listOfSequences)
      {
        listOfSequences.ensureCapacity(listOfSequences.size() + serverSegments.size());

        final Query<T> prepared = prepareQuery(query);
        final Function<T, T> deserializer = toolChest.makePreComputeManipulatorFn(
            prepared, MetricManipulatorFns.deserializing()
        );
        final List<Sequence> needPostProcessing = Lists.newArrayList();

        // Loop through each server, setting up the query and initiating it.
        // The data gets handled as a Future and parsed in the long Sequence chain in the resultSeqToAdd setter.
        for (Map.Entry<DruidServer, List<SegmentDescriptor>> entry : serverSegments.entrySet()) {
          final DruidServer server = entry.getKey();
          final List<SegmentDescriptor> descriptors = entry.getValue();

          Query<T> rewritten = prepared;
          if (server.isAssignable() && populateCache) {
            rewritten = rewritten.withOverriddenContext(contextBuilder.build());
          }
          final Query<T> running = rewritten.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(descriptors));
          final QueryRunner runner = serverView.getQueryRunner(running, server);
          if (runner == null) {
            log.error("server [%s] has disappeared.. skipping", server);
            continue;
          }

          final Sequence sequence;
          if (runner instanceof DirectDruidClient) {
            sequence = new FutureSequence(
                executorService.submit(
                    new PrioritizedCallable.Background<Sequence>()
                    {
                      @Override
                      public Sequence call() throws Exception
                      {
                        return runner.run(running, responseContext);
                      }
                    }
                )
            );
          } else {
            sequence = runner.run(running, responseContext);
          }
          if (!BaseQuery.isBySegment(running)) {
            listOfSequences.add(sequence);
          } else if (!populateCache) {
            Sequence deserialized = Sequences.map(sequence, BySegmentResultValueClass.applyAll(deserializer));
            listOfSequences.add(deserialized);
          } else {
            needPostProcessing.add(sequence);
          }
        }

        if (!needPostProcessing.isEmpty()) {
          final Function<T, Object> prepareForCache = strategy.prepareForCache();

          for (Sequence sequence : needPostProcessing) {
            final Function<Result<BySegmentResultValueClass<T>>, Sequence<T>> populator =
                new Function<Result<BySegmentResultValueClass<T>>, Sequence<T>>()
            {
              // Acctually do something with the results
              @Override
              public Sequence<T> apply(Result<BySegmentResultValueClass<T>> input)
              {
                final BySegmentResultValueClass<T> value = input.getValue();
                final CachePopulator cachePopulator = cachePopulatorMap.get(
                    String.format("%s_%s", value.getSegmentId(), value.getInterval())
                );
                if (cachePopulator == null) {
                  return Sequences.<T>simple(Iterables.transform(value.getResults(), deserializer));
                }

                final Queue<ListenableFuture<Object>> cacheFutures = new ConcurrentLinkedQueue<>();

                return Sequences.<T>withEffect(
                    Sequences.map(
                        Sequences.<T>simple(value.getResults()),
                        new Function<T, T>()
                        {
                          @Override
                          public T apply(final T input)
                          {
                            // only compute cache data if populating cache
                            cacheFutures.add(
                                backgroundExecutorService.submit(
                                    GuavaUtils.asCallable(prepareForCache, input)
                                )
                            );
                            return deserializer.apply(input);
                          }
                        }
                    ),
                    new Runnable()
                    {
                      @Override
                      public void run()
                      {
                        Futures.addCallback(
                            Futures.allAsList(cacheFutures),
                            new FutureCallback<List<Object>>()
                            {
                              @Override
                              public void onSuccess(List<Object> cacheData)
                              {
                                cachePopulator.populate(cacheData);
                                // Help out GC by making sure all references are gone
                                cacheFutures.clear();
                              }

                              @Override
                              public void onFailure(Throwable throwable)
                              {
                                log.error(throwable, "Background caching failed");
                                cacheFutures.clear();
                              }
                            },
                            backgroundExecutorService
                        );
                      }
                    },
                    MoreExecutors.sameThreadExecutor()
                );// End withEffect
              }
            };
            if (explicitBySegment) {
              sequence = Sequences.map(sequence, new IdentityFunction<Result<BySegmentResultValueClass<T>>>() {
                @Override
                public Result<BySegmentResultValueClass<T>> apply(Result<BySegmentResultValueClass<T>> input) {
                  final BySegmentResultValueClass value = input.getValue();
                  return new Result<BySegmentResultValueClass<T>>(
                      input.getTimestamp(),
                      new BySegmentResultValueClass<T>(
                          Sequences.toList(populator.apply(input)), value.getSegmentId(), value.getInterval()
                      )
                  );
                }
              });
            } else {
              sequence = QueryUtils.mergeSort(prepared, Sequences.map(sequence, populator));
            }
            listOfSequences.add(sequence);
          }
        }
      }
    }.get();
  }

  private Query<T> prepareQuery(Query<T> query)
  {
    if (queryConfig.useCustomSerdeForDateTime(query)) {
      query = query.withOverriddenContext(ImmutableMap.<String, Object>of(Query.DATETIME_CUSTOM_SERDE, true));
    }
    if (queryConfig.useBulkRow(query)) {
      query = query.withOverriddenContext(ImmutableMap.<String, Object>of(Query.USE_BULK_ROW, true));
    }
    return query;
  }

  private List<DruidServer> getManagementTargets(Query<T> query) throws Exception
  {
    Set<DruidServer> dedup = Sets.newLinkedHashSet();
    Set<String> supports = ((Query.ManagementQuery) query).supports();
    for (QueryableServer registered : serverView.getServers()) {
      DruidServer server = registered.getServer();
      if (supports.isEmpty() || supports.contains(server.getType())) {
        dedup.add(server);
      }
    }
    if (!GuavaUtils.isNullOrEmpty(supports)) {
      // damn shity discovery..
      for (String service : discovery.queryForNames()) {
        for (ServiceInstance<String> instance : discovery.queryForInstances(service)) {
          String type = instance.getPayload();
          if (type != null && supports.contains(type)) {
            String name = instance.getAddress() + ":" + instance.getPort();
            dedup.add(new DruidServer(name, name, -1, type, "", -1));
          }
        }
      }
    }
    List<DruidServer> servers = Lists.newArrayList(dedup);
    if (query instanceof FilterableManagementQuery) {
      servers = ((FilterableManagementQuery) query).filter(servers);
    }
    return servers;
  }

  protected Sequence<T> mergeCachedAndUncachedSequences(
      final Query<T> query,
      final QueryToolChest<T, Query<T>> toolChest,
      final List<Sequence<T>> sequencesByInterval,
      final int numCachedSegments,
      final CacheAccessor cacheAccessor
  )
  {
    Sequence<T> sequence = QueryUtils.mergeSort(query, sequencesByInterval);
    if (numCachedSegments > 0 && cacheAccessor != null) {
      sequence = Sequences.withBaggage(
          sequence, new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              log.debug(
                  "Deserialized %,d rows from %,d cached segments, took %,d msec",
                  cacheAccessor.rows(), numCachedSegments, cacheAccessor.time()
              );
            }
          }
      );
    }
    return sequence;
  }

  private static class CacheAccessor<T> extends Pair<AtomicLong, AtomicInteger> implements Function<Object, T>
  {
    private final ToIntFunction counter;
    private final Function<Object, T> pullFromCacheFunction;

    public CacheAccessor(ToIntFunction counter, Function<Object, T> pullFromCacheFunction)
    {
      super(new AtomicLong(), new AtomicInteger());
      this.counter = counter;
      this.pullFromCacheFunction = pullFromCacheFunction;
    }

    public void addTime(long time)
    {
      lhs.addAndGet(time);
    }

    public long time()
    {
      return lhs.get();
    }

    public int rows()
    {
      return rhs.get();
    }

    @Override
    public T apply(Object input)
    {
      long start = System.currentTimeMillis();
      try {
        final T cached = pullFromCacheFunction.apply(input);
        rhs.addAndGet(counter.applyAsInt(cached));
        return cached;
      }
      finally {
        lhs.addAndGet(System.currentTimeMillis() - start);
      }
    }
  }

  private static class CachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final Cache.NamedKey key;

    public CachePopulator(Cache cache, ObjectMapper mapper, Cache.NamedKey key)
    {
      this.cache = cache;
      this.mapper = mapper;
      this.key = key;
    }

    public void populate(Iterable<Object> results)
    {
      CacheUtil.populate(cache, mapper, key, results);
    }
  }
}
