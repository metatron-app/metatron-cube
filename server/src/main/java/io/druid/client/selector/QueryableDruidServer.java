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

package io.druid.client.selector;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.client.DirectDruidClient;
import io.druid.client.DruidServer;
import io.druid.client.QueryableServer;
import io.druid.common.guava.KVArraySortedMap;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.ReferenceCountingSegment.LocalSegment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class QueryableDruidServer implements QueryableServer
{
  private final DruidServer server;
  private final DirectDruidClient client;
  private final Map<String, VersionedIntervalTimeline<LocalSegment>> localTimelineView = Maps.newHashMap();

  public QueryableDruidServer(DruidServer server, DirectDruidClient client)
  {
    this.server = server;
    this.client = client;
  }

  @Override
  public DruidServer getServer()
  {
    return server;
  }

  public DirectDruidClient getClient()
  {
    return client;
  }

  public List<String> getLocalDataSources()
  {
    return Lists.newArrayList(localTimelineView.keySet());
  }

  public Interval getLocalDataSourceCoverage(String dataSource)
  {
    VersionedIntervalTimeline<LocalSegment> segmentMap = localTimelineView.get(dataSource);
    if (segmentMap != null) {
      return segmentMap.coverage();
    }
    return null;
  }

  public Map<String, Object> getLocalDataSourceMetaData(Iterable<String> dataSources, final String queryId)
  {
    for (String dataSource : dataSources) {
      VersionedIntervalTimeline<LocalSegment> timeline = localTimelineView.get(dataSource);
      Map<String, Object> found = timeline.find(
          new Function<Map.Entry<Interval, KVArraySortedMap<String, VersionedIntervalTimeline<LocalSegment>.TimelineEntry>>, Map<String, Object>>()
          {
            @Override
            public Map<String, Object> apply(Map.Entry<Interval, KVArraySortedMap<String, VersionedIntervalTimeline<LocalSegment>.TimelineEntry>> input)
            {
              for (VersionedIntervalTimeline<LocalSegment>.TimelineEntry entry : input.getValue().values()) {
                for (PartitionChunk<LocalSegment> chunk : entry.getPartitionHolder()) {
                  Map<String, Object> metaData = chunk.getObject().metaData();
                  if (metaData != null && queryId.equals(metaData.get("queryId"))) {
                    return metaData;
                  }
                }
              }
              return null;
            }
          }
      );
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  public Map<String, VersionedIntervalTimeline<LocalSegment>> getLocalTimelineView()
  {
    return localTimelineView;
  }

  public boolean addLocalDataSource(String dataSource)
  {
    VersionedIntervalTimeline<LocalSegment> segmentMap = localTimelineView.get(dataSource);
    if (segmentMap == null) {
      localTimelineView.put(dataSource, new VersionedIntervalTimeline<LocalSegment>());
      return true;
    }
    return false;
  }

  public void addIndex(DataSegment segment, QueryableIndex index, Map<String, Object> metaData)
  {
    VersionedIntervalTimeline<LocalSegment> segmentMap = localTimelineView.computeIfAbsent(
        segment.getDataSource(), ds -> new VersionedIntervalTimeline<LocalSegment>()
    );
    LocalSegment countingSegment = new LocalSegment(new QueryableIndexSegment(index, segment), metaData);
    segmentMap.add(segment.getInterval(), segment.getVersion(), NoneShardSpec.instance().createChunk(countingSegment));
  }
}
