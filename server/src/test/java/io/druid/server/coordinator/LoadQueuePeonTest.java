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

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DataSegmentChangeCallback;
import io.druid.server.coordination.DataSegmentChangeHandler;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LoadQueuePeonTest extends CuratorTestBase
{
  private static final String LOAD_QUEUE_PATH = "/druid/loadqueue/localhost:1234";

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private LoadQueuePeon loadQueuePeon;
  private PathChildrenCache loadQueueCache;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    curator.create().creatingParentsIfNeeded().forPath(LOAD_QUEUE_PATH);

    loadQueueCache = new PathChildrenCache(
        curator,
        LOAD_QUEUE_PATH,
        true,
        true,
        Execs.singleThreaded("load_queue_cache-%d")
    );
  }

  @Test(timeout = 20_000L)
  public void testMultipleLoadDropSegments() throws Exception
  {
    loadQueuePeon = new LoadQueuePeon(
        curator,
        "/druid/loadqueue",
        "localhost:1234",
        jsonMapper,
        Execs.scheduledSingleThreaded("test_load_queue_peon_scheduled-%d"),
        Execs.singleThreaded("test_load_queue_peon-%d"),
        new TestDruidCoordinatorConfig(null, null, null, null, null, null, 10, null, false, false)
    );

    final CountDownLatch[] loadRequestSignal = new CountDownLatch[5];
    final CountDownLatch[] dropRequestSignal = new CountDownLatch[5];
    final CountDownLatch[] segmentLoadedSignal = new CountDownLatch[5];
    final CountDownLatch[] segmentDroppedSignal = new CountDownLatch[5];

    for (int i = 0; i < 5; ++i) {
      loadRequestSignal[i] = new CountDownLatch(1);
      dropRequestSignal[i] = new CountDownLatch(1);
      segmentLoadedSignal[i] = new CountDownLatch(1);
      segmentDroppedSignal[i] = new CountDownLatch(1);
    }

    final List<DataSegment> segmentToDrop = GuavaUtils.transform(
        Arrays.asList(
            "2014-10-26T00:00:00Z/P1D",
            "2014-10-25T00:00:00Z/P1D",
            "2014-10-24T00:00:00Z/P1D",
            "2014-10-23T00:00:00Z/P1D",
            "2014-10-22T00:00:00Z/P1D"
        ), this::dataSegmentWithInterval
    );

    final List<DataSegment> segmentToLoad = GuavaUtils.transform(
        Arrays.asList(
            "2014-10-27T00:00:00Z/P1D",
            "2014-10-29T00:00:00Z/P1M",
            "2014-10-31T00:00:00Z/P1D",
            "2014-10-30T00:00:00Z/P1D",
            "2014-10-28T00:00:00Z/P1D"
        ), this::dataSegmentWithInterval
    );

    final DataSegmentChangeHandler handler = new DataSegmentChangeHandler()
    {
      @Override
      public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
      {
        loadRequestSignal[segmentToLoad.indexOf(segment)].countDown();
      }

      @Override
      public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
      {
        dropRequestSignal[segmentToDrop.indexOf(segment)].countDown();
      }
    };

    loadQueueCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
          {
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              DataSegmentChangeRequest request = jsonMapper.readValue(event.getData().getData(), DataSegmentChangeRequest.class);
              request.go(handler, null);
            }
          }
        }
    );
    loadQueueCache.start();

    for (int i = 0; i < segmentToDrop.size(); i++) {
      int ix = i;
      loadQueuePeon.dropSegment(segmentToDrop.get(i), x -> segmentDroppedSignal[ix].countDown());
    }

    for (int i = 0; i < segmentToLoad.size(); i++) {
      int ix = i;
      loadQueuePeon.loadSegment(segmentToLoad.get(i), x -> segmentLoadedSignal[ix].countDown());
    }

    for (DataSegment segment : segmentToDrop) {
      int ix = segmentToDrop.indexOf(segment);
      String dropRequestPath = ZKPaths.makePath(LOAD_QUEUE_PATH, segment.getIdentifier());
      Assert.assertTrue(timing.forWaiting().awaitLatch(dropRequestSignal[ix]));
      Assert.assertNotNull(curator.checkExists().forPath(dropRequestPath));
      Assert.assertEquals(
          segment,
          ((SegmentChangeRequestDrop) jsonMapper.readValue(
              curator.getData()
                     .decompressed()
                     .forPath(dropRequestPath), DataSegmentChangeRequest.class
          )).getSegment()
      );
      // simulate completion of drop request by historical
      curator.delete().guaranteed().forPath(dropRequestPath);
      Assert.assertTrue(timing.forWaiting().awaitLatch(segmentDroppedSignal[ix]));
    }

    for (DataSegment segment : segmentToLoad) {
      int ix = segmentToLoad.indexOf(segment);
      String loadRequestPath = ZKPaths.makePath(LOAD_QUEUE_PATH, segment.getIdentifier());
      Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequestSignal[ix]));
      Assert.assertNotNull(curator.checkExists().forPath(loadRequestPath));
      Assert.assertEquals(
          segment, ((SegmentChangeRequestLoad) jsonMapper.readValue(
              curator.getData()
                     .decompressed()
                     .forPath(loadRequestPath),
              DataSegmentChangeRequest.class
          )).getSegment()
      );

      // simulate completion of load request by historical
      curator.delete().guaranteed().forPath(loadRequestPath);
      Assert.assertTrue(timing.forWaiting().awaitLatch(segmentLoadedSignal[ix]));
    }
  }

  @Test
  public void testFailAssign() throws Exception
  {
    final DataSegment segment = dataSegmentWithInterval("2014-10-22T00:00:00Z/P1D");

    final CountDownLatch loadRequestSignal = new CountDownLatch(1);
    final CountDownLatch segmentLoadedSignal = new CountDownLatch(1);

    loadQueuePeon = new LoadQueuePeon(
        curator,
        "/druid/loadqueue",
        "localhost:1234",
        jsonMapper,
        Execs.scheduledSingleThreaded("test_load_queue_peon_scheduled-%d"),
        Execs.singleThreaded("test_load_queue_peon-%d"),
        // set time-out to 1 ms so that LoadQueuePeon will fail the assignment quickly
        new TestDruidCoordinatorConfig(null, null, null, new Duration(1), null, null, 10, null, false, false)
    );

    loadQueueCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
          {
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              loadRequestSignal.countDown();
            }
          }
        }
    );
    loadQueueCache.start();

    loadQueuePeon.loadSegment(segment, success -> segmentLoadedSignal.countDown());

    String loadRequestPath = ZKPaths.makePath(LOAD_QUEUE_PATH, segment.getIdentifier());
    Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequestSignal));
    Assert.assertNotNull(curator.checkExists().forPath(loadRequestPath));
    Assert.assertEquals(
        segment, ((SegmentChangeRequestLoad) jsonMapper.readValue(
            curator.getData()
                   .decompressed()
                   .forPath(loadRequestPath),
            DataSegmentChangeRequest.class
        )).getSegment()
    );

    // don't simulate completion of load request here
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentLoadedSignal));
    Assert.assertEquals(0, loadQueuePeon.getNumSegmentsToLoad());
    Assert.assertEquals(0, loadQueuePeon.getLoadQueueSize());   // fail queue
    loadQueuePeon.tick(1);
    Assert.assertEquals(0, loadQueuePeon.getLoadQueueSize());   // still fail queue
    loadQueuePeon.tick(1);
    Assert.assertEquals(1200, loadQueuePeon.getLoadQueueSize());   // checked in
  }

  private DataSegment dataSegmentWithInterval(String intervalStr)
  {
    return DataSegment.builder()
                      .dataSource("test_load_queue_peon")
                      .interval(new Interval(intervalStr))
                      .loadSpec(ImmutableMap.<String, Object>of())
                      .version("2015-05-27T03:38:35.683Z")
                      .dimensions(ImmutableList.<String>of())
                      .metrics(ImmutableList.<String>of())
                      .binaryVersion(9)
                      .size(1200)
                      .build();
  }

  @After
  public void tearDown() throws Exception
  {
    loadQueueCache.close();
    loadQueuePeon.stop();
    tearDownServerAndCurator();
  }
}
