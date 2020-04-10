/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.client.selector.TierSelectorStrategy;
import io.druid.concurrent.Execs;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;

/**
 */
public class CachingClusteredClientFunctionalityTest {

  public CachingClusteredClient client;

  protected VersionedIntervalTimeline<String, ServerSelector> timeline;
  protected TimelineServerView serverView;
  protected Cache cache;

  @Before
  public void setUp() throws Exception
  {
    timeline = new VersionedIntervalTimeline<>(Ordering.<String>natural());
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    cache = MapCache.create(100000);
    client = makeClient(Execs.newDirectExecutorService());
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @Test
  public void testUncoveredInterval() throws Exception {
    addToTimeline(new Interval("2015-01-02/2015-01-03"), "1");
    addToTimeline(new Interval("2015-01-04/2015-01-05"), "1");
    addToTimeline(new Interval("2015-02-04/2015-02-05"), "1");

    final BaseAggregationQuery.Builder<TimeseriesQuery> builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2015-01-02/2015-01-03")
              .granularity("day")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
              .context(ImmutableMap.<String, Object>of("uncoveredIntervalsLimit", 3));

    Map<String, Object> responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    Assert.assertNull(responseContext.get("uncoveredIntervals"));

    EasyMock.reset(serverView);
    builder.intervals("2015-01-01/2015-01-03");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-01/2015-01-02");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-01/2015-01-04");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-01/2015-01-02", "2015-01-03/2015-01-04");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-02/2015-01-04");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-03/2015-01-04");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-01/2015-01-30");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-01/2015-01-02", "2015-01-03/2015-01-04", "2015-01-05/2015-01-30");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-02/2015-01-30");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-03/2015-01-04", "2015-01-05/2015-01-30");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-04/2015-01-30");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-05/2015-01-30");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-10/2015-01-30");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-10/2015-01-30");

    EasyMock.reset(serverView);
    builder.intervals("2015-01-01/2015-02-25");
    responseContext = new HashMap<>();
    client.run(builder.build(), responseContext);
    assertUncovered(responseContext, true, "2015-01-01/2015-01-02", "2015-01-03/2015-01-04", "2015-01-05/2015-02-04");
  }

  private void assertUncovered(Map<String, Object> context, boolean uncoveredIntervalsOverflowed, String... intervals) {
    List<Interval> expectedList = Lists.newArrayListWithExpectedSize(intervals.length);
    for (String interval : intervals) {
      expectedList.add(new Interval(interval));
    }
    Assert.assertEquals((Object) expectedList, context.get("uncoveredIntervals"));
    Assert.assertEquals(uncoveredIntervalsOverflowed, context.get("uncoveredIntervalsOverflowed"));
  }

  private void addToTimeline(Interval interval, String version) {
    timeline.add(interval, version, new SingleElementPartitionChunk<>(
        new ServerSelector(
            DataSegment.builder()
                .dataSource("test")
                .interval(interval)
                .version(version)
                .build()
        )
    ));
  }

  protected CachingClusteredClient makeClient(final ListeningExecutorService backgroundExecutorService)
  {
    return makeClient(backgroundExecutorService, cache, 10);
  }

  protected CachingClusteredClient makeClient(
      final ListeningExecutorService backgroundExecutorService,
      final Cache cache,
      final int mergeLimit
  )
  {
    TierSelectorStrategy strategy = new TierSelectorStrategy()
    {
      @Override
      public Comparator<Integer> getComparator()
      {
        return Ordering.natural();
      }

      @Override
      public QueryableDruidServer pick(
          TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers,
          DataSegment segment
      )
      {
        return new QueryableDruidServer(
            new DruidServer("localhost", "localhost", 100, "historical", "a", 10),
            EasyMock.createNiceMock(DirectDruidClient.class)
        );
      }
    };
    return new CachingClusteredClient(
        null,
        CachingClusteredClientTest.WAREHOUSE,
        new TimelineServerView()
        {
          @Override
          public void registerSegmentCallback(Executor exec, SegmentCallback callback)
          {
          }

          @Override
          public void removeSegmentCallback(SegmentCallback callback)
          {
          }

          @Override
          public Iterable<String> getDataSources()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public VersionedIntervalTimeline<String, ServerSelector> getTimeline(DataSource dataSource)
          {
            return timeline;
          }

          @Override
          public Iterable<ServerSelector> getSelectors(String dataSource)
          {
            return timeline.getAll();
          }

          @Nullable
          @Override
          public List<ImmutableDruidServer> getDruidServers()
          {
            return null;
          }

          @Override
          public <T> QueryRunner<T> getQueryRunner(Query<T> query, DruidServer server)
          {
            return serverView.getQueryRunner(query, server);
          }

          @Override
          public List<QueryableDruidServer> getServers()
          {
            return null;
          }

          @Override
          public void registerTimelineCallback(Executor exec, TimelineCallback callback)
          {
          }

          @Override
          public void registerServerCallback(Executor exec, ServerCallback callback)
          {
          }
        },
        strategy,
        cache,
        CachingClusteredClientTest.jsonMapper,
        backgroundExecutorService,
        backgroundExecutorService,
        new QueryConfig(),
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return true;
          }

          @Override
          public boolean isUseCache()
          {
            return true;
          }

          @Override
          public boolean isQueryCacheable(Query query)
          {
            return true;
          }

          @Override
          public int getCacheBulkMergeLimit()
          {
            return mergeLimit;
          }
        }
    );
  }
}
