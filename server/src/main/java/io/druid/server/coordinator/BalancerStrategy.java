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

import java.util.List;

public interface BalancerStrategy
{
  int balance(
      final List<ServerHolder> holders,
      final DruidCoordinatorBalancer balancer,
      final DruidCoordinatorRuntimeParams params
  );

  ServerHolder findNewSegmentHomeReplicator(DataSegment segment, List<ServerHolder> holders);

  default void emitStats(String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList) {}

  abstract class Abstract implements BalancerStrategy
  {
    @Override
    public int balance(
        final List<ServerHolder> holders,
        final DruidCoordinatorBalancer balancer,
        final DruidCoordinatorRuntimeParams params
    )
    {
      // why split selection logic into two ?
      int balanced = 0;
      int maxSegmentsToMove = params.getMaxSegmentsToMove();
      for (int iter = 0; iter < maxSegmentsToMove; iter++) {
        final BalancerSegmentHolder segmentToMove = pickSegmentToMove(holders);
        if (segmentToMove == null) {
          continue;
        }
        final DataSegment segment = segmentToMove.getSegment();
        if (balancer.isAvailable(segment)) {
          final ServerHolder holder = findNewSegmentHomeBalancer(segment, holders);
          if (holder != null && balancer.moveSegment(segment, segmentToMove.getServerHolder(), holder)) {
            balanced++;
          }
        }
      }
      return balanced;
    }

    public abstract BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders);

    public abstract ServerHolder findNewSegmentHomeBalancer(DataSegment segment, List<ServerHolder> holders);
  }
}
