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

package io.druid.server.coordination.broker;

import com.google.common.base.Predicates;
import com.google.inject.Inject;
import io.druid.client.FilteredServerInventoryView;
import io.druid.client.ServerView;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;
import io.druid.server.QueryManager;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

@ManageLifecycle
public class DruidBroker
{
  private static final long CHECK_INTERVAL = 10;  // 10 seconds

  private final DruidNode self;
  private final QueryManager queryManager;
  private final ServiceAnnouncer serviceAnnouncer;
  private volatile boolean started = false;

  @Inject
  public DruidBroker(
      final QueryManager queryManager,
      final FilteredServerInventoryView serverInventoryView,
      final @Self DruidNode self,
      final ServiceAnnouncer serviceAnnouncer
  )
  {
    this.self = self;
    this.queryManager = queryManager;
    this.serviceAnnouncer = serviceAnnouncer;

    serverInventoryView.registerSegmentCallback(
        Execs.newDirectExecutorService(),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            serviceAnnouncer.announce(self);
            return ServerView.CallbackAction.UNREGISTER;
          }
        },
        // We are not interested in any segment callbacks except view initialization
        Predicates.<Pair<DruidServerMetadata, DataSegment>>alwaysFalse()
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (self) {
      if(started) {
        return;
      }
      started = true;
    }
    queryManager.start(CHECK_INTERVAL);
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (self) {
      if (!started) {
        return;
      }
      serviceAnnouncer.unannounce(self);
      started = false;
    }
    queryManager.stop();
  }
}
