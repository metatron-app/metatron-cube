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

package io.druid.sql.calcite.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

public class TestServerInventoryView implements TimelineServerView
{
  private static final DruidServerMetadata DUMMY_SERVER = new DruidServerMetadata(
      "dummy",
      "dummy",
      0,
      "historical",
      "dummy",
      0
  );
  private final Set<String> dataSources;
  private final List<DataSegment> segments;

  public TestServerInventoryView(List<DataSegment> segments)
  {
    this.segments = ImmutableList.copyOf(segments);
    this.dataSources = Sets.newHashSet();
    for (DataSegment segment : segments) {
      dataSources.add(segment.getDataSource());
    }
  }

  @Override
  public Iterable<String> getDataSources()
  {
    return ImmutableList.copyOf(dataSources);
  }

  @Override
  public TimelineLookup<String, ServerSelector> getTimeline(String dataSource)
  {
    return dataSources.contains(dataSource) ? new TimelineLookup.NotSupport<>() : null;
  }

  @Override
  public Iterable<ServerSelector> getSelectors(String dataSource)
  {
    return ImmutableList.of();
  }

  @Nullable
  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
  }

  @Override
  public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
  {
    for (final DataSegment segment : segments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER, segment));
    }

    exec.execute(callback::segmentViewInitialized);
  }

  @Override
  public void removeSegmentCallback(SegmentCallback callback)
  {
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    for (DataSegment segment : segments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER, segment));
    }

    exec.execute(callback::timelineInitialized);
  }

  @Override
  public List<QueryableDruidServer> getServers()
  {
    return null;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query, DruidServer server)
  {
    throw new UnsupportedOperationException();
  }
}
