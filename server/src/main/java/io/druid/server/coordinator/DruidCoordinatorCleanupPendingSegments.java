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
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.List;

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

    // If there is no running/pending/waiting/complete tasks, pendingSegmentsCleanupEndTime is
    // (DateTimes.nowUtc() - KEEP_PENDING_SEGMENTS_OFFSET).
    final DateTime pendingSegmentsCleanupEndTime = createdTimes.get(0).minus(KEEP_PENDING_SEGMENTS_OFFSET);
    for (DruidDataSource dataSource : params.getDataSources()) {
      if (!params.getCoordinatorDynamicConfig().getKillPendingSegmentsSkipList().contains(dataSource.getName())) {
        log.info(
                "Killed [%d] pendingSegments created until [%s] for dataSource[%s]",
                indexingServiceClient.killPendingSegments(dataSource.getName(), pendingSegmentsCleanupEndTime),
                pendingSegmentsCleanupEndTime,
                dataSource
        );
      }
    }
    return params;
  }
}
