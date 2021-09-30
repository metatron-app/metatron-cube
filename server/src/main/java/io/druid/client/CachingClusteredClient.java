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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValue;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSources;
import io.druid.query.FilterableManagementQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.segment.ObjectArray;
import io.druid.server.ServiceTypes;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.utils.StopWatch;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;

/**
 */
public class CachingClusteredClient<T> implements QueryRunner<T>
{
  private static final EmittingLogger LOG = new EmittingLogger(CachingClusteredClient.class);

  private final ServiceDiscovery<String> discovery;
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final QueryWatcher queryWatcher;
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
      QueryWatcher queryWatcher,
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
    this.queryWatcher = queryWatcher;
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

    final CacheStrategy<T, Object, Query<T>> strategy =
        cache == Cache.NULL || !cacheConfig.isQueryCacheable(query) ? null : toolChest.getCacheStrategyIfExists(query);

    final boolean useCache = strategy != null && cacheConfig.isUseCache() && BaseQuery.isUseCache(query, true);
    final boolean populateCache = strategy != null && cacheConfig.isPopulateCache() && BaseQuery.isPopulateCache(query, true);

    final boolean explicitBySegment = BaseQuery.isBySegment(query);

    final String dataSource = DataSources.getName(query);
    final TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(dataSource);

    if (timeline == null) {
      return Sequences.empty(query.estimatedOutputColumns());
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
      return (Sequence<T>) Sequences.simple(
          query.estimatedOutputColumns(),
          new SegmentAnalysis(
              JodaUtils.condenseIntervals(
                  Iterables.transform(
                      filteredServersLookup, TimelineObjectHolder::getInterval
                  )
              )
          )
      );
    }

    for (TimelineObjectHolder<String, ServerSelector> holder : filteredServersLookup) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        final SegmentDescriptor descriptor = new SegmentDescriptor(
            dataSource, holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
        );
        segments.add(Pair.of(chunk.getObject(), descriptor));
      }
    }

    final byte[] queryCacheKey;

    if ((populateCache || useCache) // implies strategy != null
        && !explicitBySegment) // explicit bySegment queries are never cached
    {
      queryCacheKey = strategy.computeCacheKey(query, cacheConfig.getKeyLimit());
    } else {
      queryCacheKey = null;
    }

    final List<Pair<Interval, byte[]>> cachedResults;
    final Map<ObjectArray, CachePopulator> cachePopulatorMap;

    if (queryCacheKey != null) {
      cachedResults = Lists.newArrayList();
      cachePopulatorMap = Maps.newHashMap();

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
        LOG.debug(
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
              ObjectArray.of(segmentIdentifier, segmentQueryInterval),
              new CachePopulator(cache, objectMapper, segmentCacheKey)
          );
        }
      }
    } else {
      cachedResults = Collections.emptyList();
      cachePopulatorMap = Collections.emptyMap();
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

    final Map<QueryableDruidServer, MutableInt> counts = new HashMap<>();
    final Map<DruidServer, List<SegmentDescriptor>> serverSegments = Maps.newTreeMap();

    // Compile list of all segments not pulled from cache
    for (Pair<ServerSelector, SegmentDescriptor> segment : segments) {
      final QueryableDruidServer selected = segment.lhs.pick(predicate, counts);

      if (selected == null) {
        LOG.makeAlert(
            "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
            segment.rhs,
            query.getDataSource()
        ).emit();
      } else {
        serverSegments.computeIfAbsent(selected.getServer(), k -> Lists.newArrayList()).add(segment.rhs);
        counts.computeIfAbsent(selected, s -> new MutableInt()).increment();
      }
    }

    final Query<T> prepared = prepareQuery(query, populateCache);
    final List<String> columns = prepared.estimatedOutputColumns();
    final Comparator<T> ordering = prepared.getMergeOrdering(columns);

    return new Supplier<Sequence<T>>()
    {
      @Override
      public Sequence<T> get()
      {
        CacheAccessor cacheAccessor = strategy == null ? null : new CacheAccessor(strategy, prepared);

        List<ListenableFuture<Sequence>> sequencesByInterval = Lists.newArrayList();
        if (cacheAccessor != null && !cachedResults.isEmpty()) {
          addSequencesFromCache(sequencesByInterval, cacheAccessor);
        }
        addSequencesFromServer(sequencesByInterval);

        Sequence<T> sequence = mergeCachedAndUncachedSequences(prepared, ordering, columns, sequencesByInterval);
        if (cachedResults.size() > 0 && cacheAccessor != null && LOG.isDebugEnabled()) {
          sequence = Sequences.withBaggage(
              sequence,
              () -> LOG.debug(
                  "Deserialized %,d rows from %,d cached segments, took %,d msec",
                  cacheAccessor.rows(), cachedResults.size(), cacheAccessor.time()
              )
          );
        }
        return sequence;
      }

      private void addSequencesFromCache(List<ListenableFuture<Sequence>> listOfSequences, CacheAccessor cacheAccessor)
      {
        final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
        for (Pair<Interval, byte[]> cachedResultPair : cachedResults) {
          final byte[] cachedResult = cachedResultPair.rhs;
          Sequence<Object> cachedSequence = Sequences.simple(
              columns,
              new Iterable<Object>()
              {
                @Override
                public Iterator<Object> iterator()
                {
                  if (cachedResult.length == 0) {
                    return Collections.emptyIterator();
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
          listOfSequences.add(Futures.immediateFuture(Sequences.map(columns, cachedSequence, cacheAccessor)));
        }
      }

      private void addSequencesFromServer(List<ListenableFuture<Sequence>> listOfSequences)
      {
        final Function<T, T> deserializer = toolChest.makePreComputeManipulatorFn(
            prepared, MetricManipulatorFns.deserializing()
        );
        final Function<Result<BySegmentResultValue<T>>, Sequence<T>> populator = bySegmentPopulator(
            columns, deserializer, strategy, cachePopulatorMap
        );
        final int parallelism = queryConfig.getQueryParallelism(prepared);
        final Execs.ExecutorQueue<Sequence> queue = new Execs.ExecutorQueue(parallelism);

        // Loop through each server, setting up the query and initiating it.
        // The data gets handled as a Future and parsed in the long Sequence chain in the resultSeqToAdd setter.
        for (Map.Entry<DruidServer, List<SegmentDescriptor>> entry : serverSegments.entrySet()) {
          final DruidServer server = entry.getKey();
          final List<SegmentDescriptor> descriptors = entry.getValue();

          final Query<T> running = prepared.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(descriptors));
          final QueryRunner runner = serverView.getQueryRunner(running, server);
          if (runner == null) {
            LOG.info("server [%s] has disappeared.. skipping", server);
            continue;
          }

          if (!BaseQuery.isBySegment(running)) {
            queue.add(() -> runner.run(running, responseContext));
          } else if (!populateCache) {
            queue.add(() -> Sequences.map(
                runner.run(running, responseContext), BySegmentResultValue.applyAll(deserializer))
            );
          } else if (!explicitBySegment) {
            queue.add(() -> QueryUtils.mergeSort(
                columns, ordering, Sequences.map(runner.run(running, responseContext), populator))
            );
          } else {
            queue.add(() -> Sequences.map(
                runner.run(running, responseContext),
                (Result<BySegmentResultValue<T>> input) -> {
                  final BySegmentResultValue value = input.getValue();
                  return new Result<BySegmentResultValue<T>>(
                      input.getTimestamp(),
                      toolChest.bySegment(running, populator.apply(input), value.getSegmentId())
                  );
                }
            ));
          }
        }
        ExecutorService exec = parallelism <= 1 ? Execs.newDirectExecutorService() : executorService;
        listOfSequences.addAll(queue.execute(exec, BaseQuery.getContextPriority(query, 0)));
      }
    }.get();
  }

  private Function<Result<BySegmentResultValue<T>>, Sequence<T>> bySegmentPopulator(
      List<String> columns,
      Function<T, T> deserializer,
      CacheStrategy strategy,
      Map<ObjectArray, CachePopulator> cachePopulatorMap
  )
  {
    if (strategy == null) {
      return null;
    }
    final Function<T, Object> prepareForCache = strategy.prepareForCache();
    return new Function<Result<BySegmentResultValue<T>>, Sequence<T>>()
    {
      // Acctually do something with the results
      @Override
      public Sequence<T> apply(Result<BySegmentResultValue<T>> input)
      {
        final BySegmentResultValue<T> value = input.getValue();
        final CachePopulator cachePopulator = cachePopulatorMap.get(
            ObjectArray.of(value.getSegmentId(), value.getInterval())
        );
        if (cachePopulator == null) {
          return Sequences.<T>simple(columns, Iterables.transform(value.getResults(), deserializer));
        }

        final Iterable<T> transform = Iterables.transform(
            value.getResults(),
            new Function<T, T>()
            {
              @Override
              public T apply(final T input)
              {
                cachePopulator.write(input, prepareForCache);
                return deserializer.apply(input);
              }
            }
        );
        return Sequences.<T>withEffect(
            Sequences.<T>simple(columns, transform),
            cachePopulator,
            backgroundExecutorService
        );
      }
    };
  }

  private Query<T> prepareQuery(Query<T> query, boolean populateCache)
  {
    Map<String, Object> override = Maps.newHashMap();
    if (queryConfig.useCustomSerdeForDateTime(query)) {
      override.put(Query.DATETIME_CUSTOM_SERDE, true);
    }
    if (queryConfig.useBulkRow(query)) {
      override.put(Query.USE_BULK_ROW, true);
    }
    if (populateCache) {
      // prevent down-stream nodes from caching results as well if we are populating the cache
      override.put(Query.POPULATE_CACHE, false);
      override.put(Query.BY_SEGMENT, true);
    }
    if (!override.isEmpty()) {
      query = query.withOverriddenContext(override);
    }
    return QueryUtils.compress(query.toLocalQuery());
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
      final Comparator<T> ordering,
      final List<String> columns,
      final List<ListenableFuture<Sequence>> futures
  )
  {
    StopWatch watch = queryWatcher.register(query, Futures.allAsList(futures));
    if (ordering != null) {
      List<Sequence<T>> sequences = GuavaUtils.transform(futures, future -> QueryRunners.waitOn(future, watch));
      return QueryUtils.mergeSort(columns, ordering, sequences);
    }
    return Sequences.concat(columns, Iterables.transform(futures, future -> QueryRunners.waitOn(future, watch)));
  }

  private static class CacheAccessor<T> extends Pair<AtomicLong, AtomicInteger> implements Function<Object, T>
  {
    private final ToIntFunction counter;
    private final Function<Object, T> pullFromCacheFunction;

    public CacheAccessor(CacheStrategy<T, Object, Query<T>> strategy, Query<T> query)
    {
      super(new AtomicLong(), new AtomicInteger());
      this.counter = strategy.numRows(query);
      this.pullFromCacheFunction = strategy.pullFromCache();
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
}
