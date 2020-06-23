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

package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.client.DruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.collections.CountingMap;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.query.DruidMetrics;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class DruidCoordinatorLogger implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorLogger.class);
  private static final int DUMP_INTERVAL = 10;

  private int counter;
  private final DruidCoordinator coordinator;

  public DruidCoordinatorLogger(DruidCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  private <T extends Number> void emitTieredStats(
      final ServiceEmitter emitter,
      final String metricName,
      final Map<String, T> statMap
  )
  {
    if (statMap != null) {
      for (Map.Entry<String, T> entry : statMap.entrySet()) {
        String tier = entry.getKey();
        Number value = entry.getValue();
        emitter.emit(
            new ServiceMetricEvent.Builder()
                .setDimension(DruidMetrics.TIER, tier)
                .build(
                    metricName, value.doubleValue()
                )
        );
      }
    }
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DruidCluster cluster = params.getDruidCluster();
    CoordinatorStats stats = params.getCoordinatorStats();
    ServiceEmitter emitter = params.getEmitter();

    Map<String, AtomicLong> assigned = stats.getPerTierStats().get("assignedCount");
    if (assigned != null) {
      for (Map.Entry<String, AtomicLong> entry : assigned.entrySet()) {
        if (entry.getValue().get() > 0) {
          log.info(
              "[%s] : Assigned %s segments among %,d servers",
              entry.getKey(), entry.getValue().get(), cluster.get(entry.getKey()).size()
          );
        }
      }
    }

    emitTieredStats(
        emitter, "segment/assigned/count",
        assigned
    );

    Map<String, AtomicLong> dropped = stats.getPerTierStats().get("droppedCount");
    if (dropped != null) {
      for (Map.Entry<String, AtomicLong> entry : dropped.entrySet()) {
        if (entry.getValue().get() > 0) {
          log.info(
              "[%s] : Dropped %s segments among %,d servers",
              entry.getKey(), entry.getValue().get(), cluster.get(entry.getKey()).size()
          );
        }
      }
    }

    emitTieredStats(
        emitter, "segment/dropped/count",
        dropped
    );

    emitTieredStats(
        emitter, "segment/cost/raw",
        stats.getPerTierStats().get("initialCost")
    );

    emitTieredStats(
        emitter, "segment/cost/normalization",
        stats.getPerTierStats().get("normalization")
    );

    emitTieredStats(
        emitter, "segment/moved/count",
        stats.getPerTierStats().get("movedCount")
    );

    emitTieredStats(
        emitter, "segment/deleted/count",
        stats.getPerTierStats().get("deletedCount")
    );

    Map<String, AtomicLong> normalized = stats.getPerTierStats().get("normalizedInitialCostTimesOneThousand");
    if (normalized != null) {
      emitTieredStats(
          emitter, "segment/cost/normalized",
          Maps.transformEntries(
              normalized, (key, value) -> value.doubleValue() / 1000d
          )
      );
    }

    Map<String, AtomicLong> unneeded = stats.getPerTierStats().get("unneededCount");
    if (unneeded != null) {
      for (Map.Entry<String, AtomicLong> entry : unneeded.entrySet()) {
        if (entry.getValue().get() > 0) {
          log.info(
              "[%s] : Removed %s unneeded segments among %,d servers",
              entry.getKey(), entry.getValue().get(), cluster.get(entry.getKey()).size()
          );
        }
      }
    }

    emitTieredStats(
        emitter, "segment/unneeded/count",
        stats.getPerTierStats().get("unneededCount")
    );

    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "segment/overShadowed/count", stats.getGlobalStats().get("overShadowedCount")
        )
    );

    Map<String, AtomicLong> moved = stats.getPerTierStats().get("movedCount");
    if (moved != null) {
      for (Map.Entry<String, AtomicLong> entry : moved.entrySet()) {
        if (entry.getValue().get() > 0) {
          log.info("[%s] : Moved %,d segment(s)", entry.getKey(), entry.getValue().get());
        }
      }
    }
    boolean printedHeader = false;
    boolean dumpAll = ++counter % DUMP_INTERVAL == 0;
    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> tiers : cluster.getCluster().entrySet()) {
      final String tier = tiers.getKey();
      final List<ServerHolder> servers = Lists.newArrayList(tiers.getValue());
      Collections.sort(servers, (o1, o2) -> o1.getName().compareTo(o2.getName()));
      for (ServerHolder serverHolder : servers) {
        ImmutableDruidServer server = serverHolder.getServer();
        LoadQueuePeon queuePeon = serverHolder.getPeon();
        int segments = server.getSegments().size();
        int toLoad = queuePeon.getNumSegmentsToLoad();
        int toDrop = queuePeon.getNumSegmentsToDrop();
        long queued = queuePeon.getLoadQueueSize();
        if (dumpAll || toLoad > 0 || toDrop > 0 || queued > 0) {
          if (!printedHeader) {
            printedHeader = true;
            log.info("%sLoad Queues:", dumpAll ? "*" : "");
          }
          log.info(
              "Server[%s, %s, %s] has %,d left to load, %,d left to drop, %s queued, %s served (%,d segments, %.2f%% full).",
              server.getName(),
              server.getType(),
              server.getTier(),
              toLoad,
              toDrop,
              io.druid.common.utils.StringUtils.toKMGT(queued),
              io.druid.common.utils.StringUtils.toKMGT(server.getCurrSize()),
              segments,
              server.getMaxSize() > 0 ? ((double) server.getCurrSize() / server.getMaxSize() * 100) : -1
          );
        }
      }
    }

    // Emit coordinator metrics
    final Set<Map.Entry<String, LoadQueuePeon>> peonEntries = params.getLoadManagementPeons().entrySet();
    for (Map.Entry<String, LoadQueuePeon> entry : peonEntries) {
      String serverName = entry.getKey();
      LoadQueuePeon queuePeon = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/loadQueue/size", queuePeon.getLoadQueueSize()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/loadQueue/failed", queuePeon.getAndResetFailedAssignCount()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/loadQueue/count", queuePeon.getNumSegmentsToLoad()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/dropQueue/count", queuePeon.getNumSegmentsToDrop()
          )
      );
    }
    for (Map.Entry<String, AtomicLong> entry : coordinator.getSegmentAvailability().entrySet()) {
      String datasource = entry.getKey();
      Long count = entry.getValue().get();
      emitter.emit(
              new ServiceMetricEvent.Builder()
                      .setDimension(DruidMetrics.DATASOURCE, datasource).build(
                      "segment/unavailable/count", count
              )
      );
    }
    for (Map.Entry<String, CountingMap<String>> entry : coordinator.getReplicationStatus().entrySet()) {
      String tier = entry.getKey();
      CountingMap<String> datasourceAvailabilities = entry.getValue();
      for (Map.Entry<String, AtomicLong> datasourceAvailability : datasourceAvailabilities.entrySet()) {
        String datasource = datasourceAvailability.getKey();
        Long count = datasourceAvailability.getValue().get();
        emitter.emit(
                new ServiceMetricEvent.Builder()
                        .setDimension(DruidMetrics.TIER, tier)
                        .setDimension(DruidMetrics.DATASOURCE, datasource).build(
                        "segment/underReplicated/count", count
                )
        );
      }
    }

    // Emit segment metrics
    CountingMap<String> segmentSizes = new CountingMap<String>();
    CountingMap<String> segmentCounts = new CountingMap<String>();
    for (DruidDataSource dataSource : params.getDataSources()) {
      for (DataSegment segment : dataSource.getCopyOfSegments()) {
        segmentSizes.add(dataSource.getName(), segment.getSize());
        segmentCounts.add(dataSource.getName(), 1L);
      }
    }
    for (Map.Entry<String, Long> entry : segmentSizes.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long size = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
              "segment/size", size
          )
      );
    }
    for (Map.Entry<String, Long> entry : segmentCounts.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long count = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
              "segment/count", count
          )
      );
    }


    return params;
  }
}
