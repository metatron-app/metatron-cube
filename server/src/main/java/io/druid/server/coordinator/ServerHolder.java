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

import io.druid.java.util.common.logger.Logger;
import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;

/**
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Logger log = new Logger(ServerHolder.class);
  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;

  public ServerHolder(
      ImmutableDruidServer server,
      LoadQueuePeon peon
  )
  {
    this.server = server;
    this.peon = peon;
  }

  public ImmutableDruidServer getServer()
  {
    return server;
  }

  public String getName()
  {
    return server.getName();
  }

  public LoadQueuePeon getPeon()
  {
    return peon;
  }

  public Long getMaxSize()
  {
    return server.getMaxSize();
  }

  public Long getCurrServerSize()
  {
    return server.getCurrSize();
  }

  public Long getLoadQueueSize()
  {
    return peon.getLoadQueueSize();
  }

  public Long getSizeUsed()
  {
    return getCurrServerSize() + getLoadQueueSize();
  }

  public Double getPercentUsed()
  {
    return (100 * getSizeUsed().doubleValue()) / getMaxSize();
  }

  public boolean isDecommissioned()
  {
    return server.isDecommissioned();
  }

  public int getNumExpectedSegments()
  {
    return peon.getSegmentsToLoad().size() - peon.getSegmentsToDrop().size() + server.getSegments().size();
  }

  public Long getAvailableSize()
  {
    return getAvailableSize(server, peon);
  }

  public static long getAvailableSize(ImmutableDruidServer server, LoadQueuePeon peon)
  {
    if (server.isDecommissioned()) {
      return -1;
    }
    long maxSize = server.getMaxSize();
    long sizeUsed = server.getCurrSize() + peon.getLoadQueueSize();
    long availableSize = maxSize - sizeUsed;

    log.debug(
        "Server[%s], MaxSize[%,d], CurrSize[%,d], QueueSize[%,d], SizeUsed[%,d], AvailableSize[%,d]",
        server.getName(),
        maxSize,
        server.getCurrSize(),
        peon.getLoadQueueSize(),
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

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    return getAvailableSize().compareTo(serverHolder.getAvailableSize());
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

    ServerHolder that = (ServerHolder) o;

    if (peon != null ? !peon.equals(that.peon) : that.peon != null) {
      return false;
    }
    if (server != null ? !server.equals(that.server) : that.server != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = server != null ? server.hashCode() : 0;
    result = 31 * result + (peon != null ? peon.hashCode() : 0);
    return result;
  }
}
