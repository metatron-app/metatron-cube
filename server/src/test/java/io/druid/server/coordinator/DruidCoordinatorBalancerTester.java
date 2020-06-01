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

import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.timeline.DataSegment;

public class DruidCoordinatorBalancerTester extends DruidCoordinatorBalancer
{
  public DruidCoordinatorBalancerTester(DruidCoordinator coordinator)
  {
    super(coordinator);
  }

  @Override
  public boolean moveSegment(final DataSegment segment, final ServerHolder fromServer, final ServerHolder toServer)
  {
    final LoadQueuePeon toPeon = toServer.getPeon();
    final String segmentName = segment.getIdentifier();

    if (!toPeon.getSegmentsToLoad().contains(segment) &&
        !currentlyMovingSegments.get("normal").containsKey(segmentName) &&
        !toServer.isServingSegment(segment) && toServer.getAvailableSize() > segment.getSize()) {
      log.info(
          "Moving [%s] from [%s] to [%s]", segmentName, fromServer.getName(), toServer.getName()
      );
      try {
        toPeon.loadSegment(segment, null);

        currentlyMovingSegments.get("normal").put(segmentName, new BalancerSegmentHolder(fromServer, segment));

        return true;
      }
      catch (Exception e) {
        log.info(e, String.format("[%s] : Moving exception", segmentName));
      }
    } else {
      currentlyMovingSegments.get("normal").remove(new BalancerSegmentHolder(fromServer, segment));
    }
    return false;
  }
}
