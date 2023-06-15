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

import com.google.common.base.Preconditions;
import io.druid.client.ImmutableDruidServer;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.util.Objects;

/**
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Logger log = new Logger(ServerHolder.class);
  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;
  private final String[] balancingReasons;

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon)
  {
    this.server = Preconditions.checkNotNull(server);
    this.peon = Preconditions.checkNotNull(peon);
    this.balancingReasons = new String[] {"balancing from " + server.getName(), "balanced to " + server.getName()};
  }

  public ImmutableDruidServer getServer()
  {
    return server;
  }

  public String getName()
  {
    return server.getName();
  }

  public String getTier()
  {
    return server.getTier();
  }

  public LoadQueuePeon getPeon()
  {
    return peon;
  }

  public long getMaxSize()
  {
    return server.getMaxSize();
  }

  public long getCurrServerSize()
  {
    return server.getCurrSize();
  }

  public long getLoadQueueSize()
  {
    return peon.getLoadQueueSize();
  }

  public long getSizeUsed()
  {
    return getCurrServerSize() + getLoadQueueSize();
  }

  public double getPercentUsed()
  {
    return 100d * getSizeUsed() / getMaxSize();
  }

  public boolean isDecommissioned()
  {
    return server.isDecommissioned();
  }

  public int getNumSegmentsToLoad()
  {
    return peon.getNumSegmentsToLoad();
  }

  public int getNumExpectedSegments()
  {
    return peon.getNumSegmentsToLoad() - peon.getNumSegmentsToDrop() + server.getSegments().size();
  }

  public long getAvailableSize()
  {
    if (server.isDecommissioned()) {
      return -1;
    }
    final long maxSize = server.getMaxSize();
    final long currSize = server.getCurrSize();
    final long loadQueueSize = peon.getLoadQueueSize();
    final long sizeUsed = currSize + loadQueueSize;
    final long availableSize = maxSize - sizeUsed;

    log.debug(
        "Server[%s], MaxSize[%,d], CurrSize[%,d], QueueSize[%,d], SizeUsed[%,d], AvailableSize[%,d]",
        server.getName(),
        maxSize,
        currSize,
        loadQueueSize,
        sizeUsed,
        availableSize
    );

    return availableSize;
  }

  public boolean isServingSegment(DataSegment segment)
  {
    return server.contains(segment);
  }

  public boolean isLoadingSegment(DataSegment segment)
  {
    return peon.isLoadingSegment(segment);
  }

  public boolean isDroppingSegment(DataSegment segment)
  {
    return peon.isDroppingSegment(segment);
  }

  public String balancingFrom()
  {
    return balancingReasons[0];
  }

  public String balancedTo()
  {
    return balancingReasons[1];
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    return Long.compare(getAvailableSize(), serverHolder.getAvailableSize());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return Objects.equals(server, ((ServerHolder) o).server);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(server);
  }

  @Override
  public String toString()
  {
    return server.getName();
  }
}
