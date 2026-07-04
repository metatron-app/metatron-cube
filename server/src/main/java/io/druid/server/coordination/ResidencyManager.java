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

import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The residency POLICY for {@code loadMode=auto}: periodically reconcile each segment's residency toward the
 * hottest-fit-budget set — PROMOTE a queried range (off-heap) segment that is clearly hotter than the coldest
 * local (tmpfs mmap) resident (displacing it when the cache is full), plus a pressure safety-valve that DEMOTES
 * the coldest resident when the cache is over budget. The MECHANISM (timeline swap, cache sizing) lives behind
 * {@link Host}, which {@link ServerManager} implements; this class holds only the policy (heat, watermarks,
 * hysteresis margin, min-dwell, rate limit) and the scheduler.
 */
public class ResidencyManager
{
  private static final Logger log = new Logger(ResidencyManager.class);

  // Constants for now (a future step exposes these via SegmentLoaderConfig).
  private static final double HIGH_WM = 0.9;         // pressure demote when local cache used > 90% of budget
  private static final double LOW_WM = 0.8;          // ...until back under 80%
  private static final int MAX_MIGRATE = 8;          // migrations per direction per cycle
  private static final long EVAL_MS = 120_000;       // every 2 min
  private static final long MARGIN_MS = 3_600_000;   // promote only if clearly hotter than the coldest resident
  private static final long DWELL_MS = 600_000;      // don't re-migrate a segment within 10 min (anti-thrash)

  /** The timeline/cache mechanism the policy drives. Implemented by {@link ServerManager}. */
  public interface Host
  {
    long localUsedBytes();
    long localMaxBytes();
    /** Split currently-loaded range-capable segments into {@code tmpfs} (downloaded) and {@code range} (Lazy). */
    void snapshotResident(List<ReferenceCountingSegment> tmpfs, List<ReferenceCountingSegment> range);
    boolean demoteToRange(DataSegment segment);
    boolean promoteToTmpfs(DataSegment segment);
  }

  private final Host host;
  private final Map<String, Long> lastMigration = new ConcurrentHashMap<>();   // id -> millis
  private ScheduledExecutorService exec;

  public ResidencyManager(Host host)
  {
    this.host = host;
  }

  public void start()
  {
    exec = Execs.scheduledSingleThreaded("residency-%d");
    exec.scheduleWithFixedDelay(this::reconcile, EVAL_MS, EVAL_MS, TimeUnit.MILLISECONDS);
    log.info("ResidencyManager started (auto): high=%.2f low=%.2f maxMigrate=%d marginMs=%d dwellMs=%d period=%dms",
             HIGH_WM, LOW_WM, MAX_MIGRATE, MARGIN_MS, DWELL_MS, EVAL_MS);
  }

  public void stop()
  {
    if (exec != null) {
      exec.shutdownNow();
    }
  }

  private void reconcile()
  {
    try {
      final long max = host.localMaxBytes();
      if (max <= 0) {
        return;
      }
      final long now = System.currentTimeMillis();
      final List<ReferenceCountingSegment> tmpfs = new ArrayList<>();
      final List<ReferenceCountingSegment> range = new ArrayList<>();
      host.snapshotResident(tmpfs, range);
      tmpfs.sort(Comparator.comparingLong(ResidencyManager::heatKey));            // coldest first
      range.sort(Comparator.comparingLong(ResidencyManager::heatKey).reversed()); // hottest first

      long used = host.localUsedBytes();
      int promoted = 0;

      // A) promote hot, actually-queried range segments (conservative: only if clearly hotter than the coldest).
      for (ReferenceCountingSegment pc : range) {
        if (promoted >= MAX_MIGRATE) {
          break;
        }
        if (pc.getAccessCount() <= 0 || !dwellOk(pc, now)) {
          continue;   // only promote segments a query actually touched, and not one just migrated
        }
        final ReferenceCountingSegment coldest = tmpfs.isEmpty() ? null : tmpfs.get(0);
        if (coldest != null && heatKey(pc) <= heatKey(coldest) + MARGIN_MS) {
          continue;   // not clearly hotter than the coldest resident -> hysteresis, leave in range
        }
        final long size = pc.getDescriptor().getSize();
        if (used + size > max) {              // no room: displace the coldest resident (already known colder)
          if (coldest == null || !dwellOk(coldest, now)) {
            continue;
          }
          final DataSegment cds = coldest.getDescriptor();
          if (!host.demoteToRange(cds)) {
            continue;
          }
          lastMigration.put(cds.getIdentifier(), now);
          used -= cds.getSize();
          tmpfs.remove(0);
        }
        if (used + size <= max && host.promoteToTmpfs(pc.getDescriptor())) {
          lastMigration.put(pc.getDescriptor().getIdentifier(), now);
          used += size;
          promoted++;
        }
      }

      // B) pressure safety-valve: still over budget -> demote coldest resident to the low watermark.
      int demoted = 0;
      if (used > HIGH_WM * max) {
        final List<ReferenceCountingSegment> resident = new ArrayList<>();
        host.snapshotResident(resident, new ArrayList<>());
        resident.sort(Comparator.comparingLong(ResidencyManager::heatKey));   // coldest first
        for (ReferenceCountingSegment rcs : resident) {
          if (used <= LOW_WM * max || demoted >= MAX_MIGRATE) {
            break;
          }
          final DataSegment ds = rcs.getDescriptor();
          if (host.demoteToRange(ds)) {
            lastMigration.put(ds.getIdentifier(), now);
            used -= ds.getSize();
            demoted++;
          }
        }
      }
      if (promoted > 0 || demoted > 0) {
        log.info("[residency] promoted %d, demoted %d; local cache ~%,d / %,d bytes", promoted, demoted, used, max);
      }
    }
    catch (Throwable t) {
      log.warn(t, "[residency] eval failed");
    }
  }

  private boolean dwellOk(ReferenceCountingSegment rcs, long now)
  {
    final Long last = lastMigration.get(rcs.getIdentifier());
    return last == null || now - last > DWELL_MS;
  }

  /** Coldness key: least-recently-queried first; a never-queried segment is ranked by its data recency (prior). */
  private static long heatKey(ReferenceCountingSegment rcs)
  {
    return rcs.getAccessCount() > 0 ? rcs.getLastAccessTime() : rcs.getInterval().getEndMillis();
  }
}
