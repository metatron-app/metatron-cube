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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.timeline.DataSegment;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class RandomBalancerStrategy extends BalancerStrategy.Abstract
{
  private final Random random = new Random();
  private final ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();

  @Override
  public ServerHolder findNewSegmentHomeReplicator(DataSegment segment, List<ServerHolder> holders, int maxLoad)
  {
    return choose(filter(segment, holders, maxLoad, false).iterator());
  }

  @Override
  public ServerHolder findNewSegmentHomeBalancer(DataSegment segment, List<ServerHolder> holders, int maxLoad)
  {
    return choose(filter(segment, holders, maxLoad, true).iterator());
  }

  private ServerHolder choose(Iterator<ServerHolder> filtered)
  {
    if (filtered.hasNext()) {
      List<ServerHolder> candidates = Lists.newArrayList(filtered);
      return candidates.get(random.nextInt(candidates.size()));
    }
    return null;
  }

  static Iterable<ServerHolder> filter(
      DataSegment segment,
      List<ServerHolder> holders,
      int maxLoad,
      boolean includeCurrentServer
  )
  {
    return Iterables.filter(
        holders,
        holder -> !holder.isDecommissioned() &&
                  !holder.isLoadingSegment(segment) &&
                  (maxLoad < 0 || holder.getNumSegmentsToLoad() < maxLoad) &&
                  holder.getAvailableSize() >= segment.getSize() &&
                  (includeCurrentServer || !holder.isServingSegment(segment))
    );
  }

  @Override
  public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
  {
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }
}
