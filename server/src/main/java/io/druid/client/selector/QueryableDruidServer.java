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

package io.druid.client.selector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.client.DirectDruidClient;
import io.druid.client.DruidServer;
import io.druid.client.ReferenceCountingSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class QueryableDruidServer
{
  private final DruidServer server;
  private final DirectDruidClient client;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> localTimelineView = Maps.newHashMap();

  public QueryableDruidServer(DruidServer server, DirectDruidClient client)
  {
    this.server = server;
    this.client = client;
  }

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

  public Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> getLocalTimelineView()
  {
    return localTimelineView;
  }

  public void addIndex(DataSegment segment, QueryableIndex index)
  {
    String dataSource = segment.getDataSource();
    VersionedIntervalTimeline<String, ReferenceCountingSegment> segmentMap = localTimelineView.get(dataSource);
    if (segmentMap == null) {
      localTimelineView.put(
          dataSource,
          segmentMap = new VersionedIntervalTimeline<String, ReferenceCountingSegment>(Ordering.natural())
      );
    }
    ReferenceCountingSegment countingSegment = new ReferenceCountingSegment(
        segment,
        new QueryableIndexSegment(segment.getIdentifier(), index)
    );
    segmentMap.add(segment.getInterval(), segment.getVersion(), NoneShardSpec.instance().createChunk(countingSegment));
  }
}
