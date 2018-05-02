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

package io.druid.server.coordinator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Random;

public class RandomBalancerStrategy extends BalancerStrategy.Abstract
{
  private final ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();

  @Override
  public ServerHolder findNewSegmentHomeReplicator(
      final DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    return chooseRandomServer(proposalSegment, serverHolders, false);
  }

  @Override
  public ServerHolder findNewSegmentHomeBalancer(
      final DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    return chooseRandomServer(proposalSegment, serverHolders, true);
  }

  private ServerHolder chooseRandomServer(
      DataSegment proposalSegment,
      List<ServerHolder> serverHolders,
      boolean includeCurrentServer
  )
  {
    serverHolders = filter(proposalSegment, serverHolders, includeCurrentServer);
    return serverHolders.isEmpty() ? null : serverHolders.get(new Random().nextInt(serverHolders.size()));
  }

  public static List<ServerHolder> filter(
      final DataSegment proposalSegment,
      final List<ServerHolder> serverHolders,
      final boolean includeCurrentServer
  )
  {
    final long proposalSegmentSize = proposalSegment.getSize();
    return Lists.newArrayList(
        Iterables.filter(
            serverHolders, new Predicate<ServerHolder>()
            {
              @Override
              public boolean apply(ServerHolder holder)
              {
                return holder.getAvailableSize() > proposalSegmentSize &&
                       !holder.isLoadingSegment(proposalSegment) &&
                       (includeCurrentServer || !holder.isServingSegment(proposalSegment));
              }
            }
        )
    );
  }

  @Override
  public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
  {
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }

  @Override
  public void emitStats(
      String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList
  )
  {
  }
}
