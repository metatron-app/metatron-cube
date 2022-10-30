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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.druid.common.utils.UUIDUtils;
import io.druid.curator.announcement.Announcer;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class BatchDataSegmentAnnouncer implements DataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchDataSegmentAnnouncer.class);

  private final BatchDataSegmentAnnouncerConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final String liveSegmentLocation;
  private final DruidServerMetadata server;
  private final DataSegmentServerAnnouncer serverAnnouncer;

  private final Object lock = new Object();
  private final AtomicLong counter = new AtomicLong(0);

  private final Set<SegmentZNode> availableZNodes = new HashSet<>();
  private final Map<DataSegment, SegmentZNode> segmentLookup = Maps.newHashMap();
  private final Function<DataSegment, DataSegment> segmentTransformer;

  private final SegmentChangeRequestHistory changes = new SegmentChangeRequestHistory();

  @Inject
  public BatchDataSegmentAnnouncer(
      DruidServerMetadata server,
      DataSegmentServerAnnouncer serverAnnouncer,
      final BatchDataSegmentAnnouncerConfig config,
      ZkPathsConfig zkPaths,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
    this.server = server;
    this.serverAnnouncer = serverAnnouncer;
    this.liveSegmentLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), server.getName());
    this.segmentTransformer = toTransformer(config);
  }

  private static Function<DataSegment, DataSegment> toTransformer(BatchDataSegmentAnnouncerConfig config)
  {
    if (config.isSkipDimensionsAndMetrics() && config.isSkipLoadSpec()) {
      return ds -> ds.withMinimum();
    } else if (config.isSkipDimensionsAndMetrics()) {
      return ds -> ds.withDimensionsMetrics(null, null);
    } else if (config.isSkipLoadSpec()) {
      return ds -> ds.withLoadSpec(null);
    } else {
      return ds -> ds;
    }
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    synchronized (lock) {
      if (segmentLookup.containsKey(segment)) {
        log.debug("Skipping announcement of segment [%s]. Announcement exists already.", segment);
        return;
      }

      final DataSegment toAnnounce = segmentTransformer.apply(segment);

      final byte[] newBytes = jsonMapper.writeValueAsBytes(toAnnounce);
      if (newBytes.length > config.getMaxBytesPerNode()) {
        throw new ISE("byte size %,d exceeds %,d", newBytes.length, config.getMaxBytesPerNode());
      }

      boolean done = false;
      if (!availableZNodes.isEmpty()) {
        // update existing batch
        Iterator<SegmentZNode> iter = availableZNodes.iterator();
        while (iter.hasNext() && !done) {
          final SegmentZNode availableZNode = iter.next();
          if (availableZNode.currentLength + newBytes.length < config.getMaxBytesPerNode()) {
            availableZNode.addSegment(toAnnounce, newBytes);

            log.debug(
                "Announcing segment[%s] at existing path[%s]",
                toAnnounce.getIdentifier(),
                availableZNode.getPath()
            );
            segmentLookup.put(toAnnounce, availableZNode);
            if (availableZNode.getCount() >= config.getSegmentsPerNode()) {
              availableZNodes.remove(availableZNode);
            }
            announcer.update(availableZNode.getPath(), availableZNode.asBytes());
            done = true;
          } else {
            // We could have kept the znode around for later use, however we remove it since segment announcements should
            // have similar size unless there are significant schema changes. Removing the znode reduces the number of
            // znodes that would be scanned at each announcement.
            iter.remove();
          }
        }
      }

      if (!done) {
        assert (availableZNodes.isEmpty());
        // create new batch

        SegmentZNode availableZNode = new SegmentZNode(makeServedSegmentPath());
        availableZNode.addSegment(toAnnounce, newBytes);

        log.debug("Announcing segment[%s] at new path[%s]", toAnnounce.getIdentifier(), availableZNode.getPath());
        segmentLookup.put(toAnnounce, availableZNode);
        availableZNodes.add(availableZNode);
        announcer.announce(availableZNode.getPath(), availableZNode.asBytes());
      }

      changes.addSegmentChangeRequest(new SegmentChangeRequestLoad(toAnnounce));
    }
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    synchronized (lock) {
      final SegmentZNode segmentZNode = segmentLookup.remove(segment);

      if (segmentZNode == null) {
        log.info("No path to unannounce segment[%s]", segment.getIdentifier());
        return;
      }

      segmentZNode.removeSegment(segment);

      log.debug("Unannouncing segment[%s] at path[%s]", segment.getIdentifier(), segmentZNode.getPath());

      if (segmentZNode.getCount() == 0) {
        availableZNodes.remove(segmentZNode);
        announcer.unannounce(segmentZNode.getPath());
      } else {
        availableZNodes.add(segmentZNode);
        announcer.update(segmentZNode.getPath(), segmentZNode.asBytes());
      }

      changes.addSegmentChangeRequest(new SegmentChangeRequestDrop(segment));
    }
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    SegmentZNode segmentZNode = new SegmentZNode(makeServedSegmentPath());
    List<DataSegmentChangeRequest> changesBatch = new ArrayList<>();

    synchronized (lock) {
      for (DataSegment ds : segments) {
        if (segmentLookup.containsKey(ds)) {
          log.debug("Skipping announcement of segment [%s]. Announcement exists already.", ds);
          return;
        }

        final DataSegment segment = segmentTransformer.apply(ds);

        final byte[] newBytes = jsonMapper.writeValueAsBytes(segment);

        if (newBytes.length > config.getMaxBytesPerNode()) {
          throw new ISE("byte size %,d exceeds %,d", newBytes.length, config.getMaxBytesPerNode());
        }

        if (segmentZNode.getCount() >= config.getSegmentsPerNode() ||
            segmentZNode.currentLength + newBytes.length > config.getMaxBytesPerNode()) {
          announcer.announce(segmentZNode.getPath(), segmentZNode.asBytes());
          segmentZNode = new SegmentZNode(makeServedSegmentPath());
        }

        // skip logging before server announce
        if (serverAnnouncer.isAnnounced()) {
          log.debug("Announcing segment[%s] at path[%s]", segment.getIdentifier(), segmentZNode.getPath());
        }
        segmentLookup.put(segment, segmentZNode);
        segmentZNode.addSegment(segment, newBytes);

        changesBatch.add(new SegmentChangeRequestLoad(segment));
      }
      if (segmentZNode.getCount() < config.getSegmentsPerNode() &&
          segmentZNode.currentLength < config.getMaxBytesPerNode()) {
        availableZNodes.add(segmentZNode);
      }
      announcer.announce(segmentZNode.getPath(), segmentZNode.asBytes());
    }

    changes.addSegmentChangeRequests(changesBatch);
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegment segment : segments) {
      unannounceSegment(segment);
    }
  }

  /**
   * Returns Future that lists the segment load/drop requests since given counter.
   */
  public ListenableFuture<SegmentChangeRequestsSnapshot> getSegmentChangesSince(SegmentChangeRequestHistory.Counter counter)
  {
    if (counter.getCounter() < 0) {
      synchronized (lock) {
        return Futures.immediateFuture(
            SegmentChangeRequestsSnapshot.success(
                changes.getLastCounter(),
                Lists.newArrayList(Iterables.transform(segmentLookup.keySet(), k -> new SegmentChangeRequestLoad(k)))
            )
        );
      }
    } else {
      return changes.getRequestsSince(counter);
    }
  }

  private String makeServedSegmentPath()
  {
    // server.getName() is already in the zk path
    return makeServedSegmentPath(
        UUIDUtils.generateUuid(
            server.getHost(),
            server.getType(),
            server.getTier(),
            new DateTime().toString()
        )
    );
  }

  private String makeServedSegmentPath(String zNode)
  {
    return ZKPaths.makePath(liveSegmentLocation, String.format("%s%s", zNode, counter.getAndIncrement()));
  }

  private static class SegmentZNode implements Comparable<SegmentZNode>
  {
    private final String path;

    private int currentLength;  // no need to be exact
    private final Map<String, byte[]> segments;

    public SegmentZNode(String path)
    {
      this.path = path;
      this.segments = Maps.newHashMap();
    }

    public String getPath()
    {
      return path;
    }

    public int getCount()
    {
      return segments.size();
    }

    public byte[] asBytes()
    {
      Iterator<byte[]> values = segments.values().iterator();
      if (!values.hasNext()) {
        return "[]".getBytes();
      }
      BytesOutputStream b = new BytesOutputStream(currentLength + segments.size() + 1);
      b.write('[');
      b.write(values.next());
      while (values.hasNext()) {
        b.write(',');
        b.write(values.next());
      }
      b.write(']');
      return b.toByteArray();
    }

    public void addSegment(DataSegment segment, byte[] bytes)
    {
      if (segments.put(segment.getIdentifier(), bytes) == null) {
        currentLength += bytes.length;
      }
    }

    public void removeSegment(DataSegment segment)
    {
      byte[] bytes = segments.remove(segment.getIdentifier());
      if (bytes != null) {
        currentLength -= bytes.length;
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

      SegmentZNode that = (SegmentZNode) o;

      if (!path.equals(that.path)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return path.hashCode();
    }

    @Override
    public int compareTo(SegmentZNode segmentZNode)
    {
      return path.compareTo(segmentZNode.getPath());
    }
  }

}
