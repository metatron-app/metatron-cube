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

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class DruidServer implements Comparable
{
  public static DruidServer of(DruidNode node, String type)
  {
    return new DruidServer(node, new DruidServerConfig(), type);
  }

  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_NUM_REPLICANTS = 2;
  public static final String DEFAULT_TIER = "_default_tier";

  private static final Logger log = new Logger(DruidServer.class);

  private final Object lock = new Object();

  private final ConcurrentMap<String, DruidDataSource> dataSources;
  private final ConcurrentMap<String, DataSegment> segments;

  private final DruidServerMetadata metadata;

  private volatile long currSize;

  public DruidServer(
      DruidNode node,
      DruidServerConfig config,
      String type
  )
  {
    this(
        node.getHostAndPort(),
        node.getHostAndPort(),
        config.getMaxSize(),
        type,
        config.getTier(),
        DEFAULT_PRIORITY
    );
  }

  @JsonCreator
  public DruidServer(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") String type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this.metadata = new DruidServerMetadata(name, host, maxSize, type, tier, priority);

    this.dataSources = new ConcurrentHashMap<String, DruidDataSource>();
    this.segments = new ConcurrentHashMap<String, DataSegment>();
  }

  public DruidServer(
      DruidServerMetadata metadata,
      ConcurrentMap<String, DruidDataSource> dataSources,
      ConcurrentMap<String, DataSegment> segments,
      long currSize
  )
  {
    this.metadata = metadata;
    this.dataSources = dataSources;
    this.segments = segments;
    this.currSize = currSize;
  }

  public String getName()
  {
    return metadata.getName();
  }

  public DruidServerMetadata getMetadata()
  {
    return metadata;
  }

  public String getHost()
  {
    return metadata.getHost();
  }

  public long getCurrSize()
  {
    return currSize;
  }

  public long getMaxSize()
  {
    return metadata.getMaxSize();
  }

  public String getType()
  {
    return metadata.getType();
  }

  public String getTier()
  {
    return metadata.getTier();
  }

  public boolean isAssignable()
  {
    return metadata.isAssignable();
  }

  public boolean isDecommissioned()
  {
    return metadata.isDecommissioned();
  }

  public int getPriority()
  {
    return metadata.getPriority();
  }

  public Map<String, DataSegment> getSegments()
  {
    // Copying the map slows things down a lot here, don't use Immutable Map here
    return Collections.unmodifiableMap(segments);
  }

  public DataSegment getSegment(String segmentName)
  {
    return segments.get(segmentName);
  }

  private static final Map<String, String> DUMMY = ImmutableMap.of("client", "side"); // what is this?

  public boolean addDataSegment(DataSegment segment)
  {
    final String segmentId = segment.getIdentifier();

    synchronized (lock) {
      if (segments.putIfAbsent(segmentId, segment) != null) {
        log.debug("Asked to add data segment that already exists!? server[%s], segment[%s]", getName(), segmentId);
        return false;
      }

      if (dataSources.computeIfAbsent(segment.getDataSource(), name -> new DruidDataSource(name, DUMMY))
                     .addSegmentIfAbsent(segment)) {
        currSize += segment.getSize();
      }
    }
    return true;
  }

  public DruidServer addDataSegments(DruidServer server)
  {
    synchronized (lock) {
      if (segments.isEmpty() && dataSources.isEmpty()) {
        return new DruidServer(metadata, server.dataSources, server.segments, server.currSize);
      }
      for (Map.Entry<String, DataSegment> entry : server.segments.entrySet()) {
        addDataSegment(entry.getValue());
      }
    }
    return this;
  }

  public DataSegment removeDataSegment(String segmentId)
  {
    synchronized (lock) {
      DataSegment segment = segments.get(segmentId);

      if (segment == null) {
        log.debug("Asked to remove data segment that doesn't exist. server[%s], segment[%s]", getName(), segmentId);
        return segment;
      }

      DruidDataSource dataSource = dataSources.get(segment.getDataSource());

      if (dataSource == null) {
        log.warn(
            "Asked to remove data segment from dataSource[%s] that doesn't exist, but the segment[%s] exists? server[%s]",
            segment.getDataSource(),
            segmentId,
            getName()
        );
        return segment;
      }

      dataSource.removeSegment(segmentId);

      if (segments.remove(segmentId) != null) {
        currSize -= segment.getSize();
      }

      if (dataSource.isEmpty()) {
        dataSources.remove(dataSource.getName());
      }
      return segment;
    }
  }

  public DruidDataSource getDataSource(String dataSource)
  {
    return dataSources.get(dataSource);
  }

  public Iterable<DruidDataSource> getDataSources()
  {
    return dataSources.values();
  }

  public void removeAllSegments()
  {
    synchronized (lock) {
      dataSources.clear();
      segments.clear();
      currSize = 0;
    }
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

    DruidServer that = (DruidServer) o;

    if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return getName() != null ? getName().hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return metadata.toString();
  }

  @Override
  public int compareTo(Object o)
  {
    if (this == o) {
      return 0;
    }
    if (o == null || getClass() != o.getClass()) {
      return 1;
    }

    return getName().compareTo(((DruidServer) o).getName());
  }

  public ImmutableDruidServer toImmutableDruidServer()
  {
    return new ImmutableDruidServer(
        metadata,
        currSize,
        ImmutableMap.copyOf(Maps.transformValues(dataSources, DruidDataSource::toImmutableDruidDataSource)),
        ImmutableMap.copyOf(segments)
    );
  }
}
