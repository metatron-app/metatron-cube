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
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.sql.calcite.util.TestQuerySegmentWalker.PopulatingMap;
import io.druid.timeline.TimelineLookup;

import java.util.List;
import java.util.concurrent.Executor;

public class TestServerInventoryView implements TimelineServerView
{
  private final PopulatingMap timelines;

  public TestServerInventoryView(PopulatingMap timelines)
  {
    this.timelines = timelines;
  }

  @Override
  public Iterable<String> getDataSources()
  {
    return ImmutableList.copyOf(timelines.getDataSource());
  }

  @Override
  public TimelineLookup<ServerSelector> getTimeline(String dataSource)
  {
    return timelines.getDataSource().contains(dataSource) ? new TimelineLookup.NotSupport<>() : null;
  }

  @Override
  public Iterable<ServerSelector> getSelectors(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback) {}

  @Override
  public void registerSegmentCallback(Executor exec, final SegmentCallback callback) {}

  @Override
  public void removeSegmentCallback(SegmentCallback callback) {}

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback) {}

  @Override
  public List<QueryableDruidServer> getServers()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query, DruidServer server)
  {
    throw new UnsupportedOperationException();
  }
}
