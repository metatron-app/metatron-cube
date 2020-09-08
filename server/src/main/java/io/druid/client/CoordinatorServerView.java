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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.query.DataSource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.log.Events;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * ServerView of coordinator for the state of segments being loaded in the cluster.
 */
public class CoordinatorServerView implements InventoryView
{
  private static final Logger log = new Logger(CoordinatorServerView.class);

  private final Object lock = new Object();

  private final Map<String, SegmentLoadInfo> segmentLoadInfos;
  private final Map<String, VersionedIntervalTimeline<String, SegmentLoadInfo>> timelines;

  private final ServerInventoryView baseView;
  private final Emitter emitter;

  private volatile boolean initialized = false;

  @Inject
  public CoordinatorServerView(
      ServerInventoryView baseView,
      @Events Emitter emitter
  )
  {
    this.baseView = baseView;
    this.emitter = emitter;
    this.segmentLoadInfos = Maps.newHashMap();
    this.timelines = Maps.newHashMap();

    ExecutorService exec = Execs.singleThreaded("CoordinatorServerView-%s");
    baseView.registerSegmentCallback(
        exec,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            serverAddedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, DataSegment segment)
          {
            serverRemovedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initialized = true;
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    baseView.registerServerCallback(
        exec,
        new ServerView.AbstractServerCallback()
        {
          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            removeServer(server);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  public boolean isInitialized()
  {
    return initialized;
  }

  public void clear()
  {
    synchronized (lock) {
      timelines.clear();
      segmentLoadInfos.clear();
    }
  }

  private void removeServer(DruidServer server)
  {
    for (DataSegment segment : server.getSegments().values()) {
      serverRemovedSegment(server.getMetadata(), segment);
    }
  }

  private void serverAddedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    final String segmentId = segment.getIdentifier();

    log.debug("Adding segment[%s] to server[%s]", segmentId, server);

    synchronized (lock) {
      SegmentLoadInfo loadInfo = segmentLoadInfos.computeIfAbsent(segmentId, id -> {
        SegmentLoadInfo info = new SegmentLoadInfo(segment.toDescriptor());
        VersionedIntervalTimeline<String, SegmentLoadInfo> timeline = timelines.computeIfAbsent(
            segment.getDataSource(), ds -> {
              emitter.emit(
                  new Events.SimpleEvent(
                      ImmutableMap.<String, Object>of(
                          "feed", "CoordinatorServerView",
                          "type", "newDataSource",
                          "createdDate", System.currentTimeMillis(),
                          "dataSource", ds
                      )
                  )
              );
              return new VersionedIntervalTimeline<>();
            });
        timeline.add(
            segment.getInterval(),
            segment.getVersion(),
            segment.getShardSpecWithDefault().createChunk(info)
        );
        return info;
      });
      loadInfo.addServer(server);
    }
  }

  private void serverRemovedSegment(DruidServerMetadata server, DataSegment segment)
  {
    final String segmentId = segment.getIdentifier();

    log.debug("Removing segment[%s] from server[%s]", segmentId, server);

    synchronized (lock) {
      segmentLoadInfos.compute(segmentId, (id, segmentLoadInfo) -> {
        if (segmentLoadInfo == null) {
          log.warn("Told to remove non-existant segment[%s]", segmentId);
          return null;
        }
        segmentLoadInfo.removeServer(server);
        if (segmentLoadInfo.isEmpty()) {
          final VersionedIntervalTimeline<String, SegmentLoadInfo> timeline = timelines.get(segment.getDataSource());
          final PartitionChunk<SegmentLoadInfo> removedPartition = timeline.remove(
              segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(null)
          );

          if (removedPartition == null) {
            log.warn(
                "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
                segment.getInterval(),
                segment.getVersion()
            );
          }
          return null;  // remove
        }
        return segmentLoadInfo;
      });
    }
  }

  public VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getNames());
    synchronized (lock) {
      return timelines.get(table);
    }
  }


  @Override
  public DruidServer getInventoryValue(String string)
  {
    return baseView.getInventoryValue(string);
  }

  @Override
  public Iterable<DruidServer> getInventory()
  {
    return baseView.getInventory();
  }

  @Override
  public boolean isStarted()
  {
    return baseView.isStarted();
  }

  @Override
  public int getInventorySize()
  {
    return baseView.getInventorySize();
  }
}
