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

import com.google.inject.Inject;
import io.druid.segment.loading.SegmentScanner;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

/**
 * Standalone (no coordinator / no ZooKeeper / no metadata DB) bootstrap: at lifecycle start, discover the
 * segment set straight from deep storage via a {@link SegmentScanner}, keep only the non-overshadowed (latest
 * version) segments, and hand each to {@link ServerManager#loadSegment} — which applies the auto residency
 * policy (hot in tmpfs, cold tail off-heap range). The node then serves queries directly at :8083. Replaces
 * ZkCoordinator; startup-only (a restart re-scans, picking up new ingestion).
 */
public class StandaloneSegmentLoader
{
  private static final Logger log = new Logger(StandaloneSegmentLoader.class);

  private final SegmentScanner scanner;
  private final ServerManager serverManager;

  @Inject
  public StandaloneSegmentLoader(SegmentScanner scanner, ServerManager serverManager)
  {
    this.scanner = scanner;
    this.serverManager = serverManager;
  }

  @LifecycleStart
  public void start() throws Exception
  {
    final List<DataSegment> all = scanner.scan();
    final List<DataSegment> visible = nonOvershadowed(all);
    int loaded = 0;
    for (DataSegment segment : visible) {
      try {
        serverManager.loadSegment(segment);
        loaded++;
      }
      catch (Exception e) {
        log.warn(e, "standalone: failed to load segment[%s]", segment.getIdentifier());
      }
    }
    log.info("standalone: scanned %d descriptor(s), loaded %d of %d visible segment(s) from deep storage",
             all.size(), loaded, visible.size());
  }

  @LifecycleStop
  public void stop() {}

  /** Keep only the segments a query would actually see (highest version per interval; drop superseded ones). */
  private static List<DataSegment> nonOvershadowed(List<DataSegment> all)
  {
    final VersionedIntervalTimeline<DataSegment> timeline = new VersionedIntervalTimeline<>();
    for (DataSegment s : all) {
      timeline.add(s.getInterval(), s.getVersion(), s.getShardSpecWithDefault().createChunk(s));
    }
    final List<DataSegment> visible = new ArrayList<>();
    final Interval eternity = new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);
    for (TimelineObjectHolder<DataSegment> holder : timeline.lookup(eternity)) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        visible.add(chunk.getObject());
      }
    }
    return visible;
  }
}
