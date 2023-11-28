package io.druid.server.coordinator;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.client.DruidDataSource;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.helper.DruidCoordinatorHelper;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DruidCoordinatorCleanupPendingSegments implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorCleanupPendingSegments.class);
  private static final Period KEEP_PENDING_SEGMENTS_OFFSET = new Period("P1D");

  private final IndexingServiceClient indexingServiceClient;

  @Inject
  public DruidCoordinatorCleanupPendingSegments(IndexingServiceClient indexingServiceClient)
  {
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final List<DateTime> createdTimes = new ArrayList<>();
    createdTimes.add(
            indexingServiceClient
                    .getRunningTasks()
                    .stream()
                    .map(TaskStatusPlus::getCreatedTime)
                    .min(Comparators.naturalNullsFirst())
                    .orElse(DateTimes.nowUtc()) // If there is no running tasks, this returns the current time.
    );
    createdTimes.add(
            indexingServiceClient
                    .getPendingTasks()
                    .stream()
                    .map(TaskStatusPlus::getCreatedTime)
                    .min(Comparators.naturalNullsFirst())
                    .orElse(DateTimes.nowUtc()) // If there is no pending tasks, this returns the current time.
    );
    createdTimes.add(
            indexingServiceClient
                    .getWaitingTasks()
                    .stream()
                    .map(TaskStatusPlus::getCreatedTime)
                    .min(Comparators.naturalNullsFirst())
                    .orElse(DateTimes.nowUtc()) // If there is no waiting tasks, this returns the current time.
    );

    final TaskStatusPlus completeTaskStatus = indexingServiceClient.getLastCompleteTask();
    if (completeTaskStatus != null) {
      createdTimes.add(completeTaskStatus.getCreatedTime());
    }
    createdTimes.sort(Comparators.naturalNullsFirst());

    // There should be at least one createdTime because the current time is added to the 'createdTimes' list if there
    // is no running/pending/waiting tasks.
    Preconditions.checkState(!createdTimes.isEmpty(), "Failed to gather createdTimes of tasks");

    final DateTime cleanupTo = createdTimes.get(0).minus(KEEP_PENDING_SEGMENTS_OFFSET);
    final Set<String> skipList = params.getCoordinatorDynamicConfig().getKillPendingSegmentsSkipList();
    final MutableInt total = new MutableInt();

    long p = System.currentTimeMillis();
    log.info("Finding pending segments to be cleaned-up, created before [%s]", cleanupTo);
    // If there is no running/pending/waiting/complete tasks, pendingSegmentsCleanupEndTime is
    // (DateTimes.nowUtc() - KEEP_PENDING_SEGMENTS_OFFSET).
    params.getDataSources().stream().map(DruidDataSource::getName).filter(ds -> !skipList.contains(ds)).forEach(
        ds -> {
          int killed = indexingServiceClient.killPendingSegments(ds, cleanupTo);
          if (killed > 0) {
            log.info("Killed [%d] pending segment(s) for dataSource[%s]", killed, ds);
            total.add(killed);
          }
        }
    );
    log.info("Killed total [%,d] pending segment(s) in %,d msec", total.intValue(), System.currentTimeMillis() - p);
    return params;
  }
}
