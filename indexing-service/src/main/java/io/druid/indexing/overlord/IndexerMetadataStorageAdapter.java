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

package io.druid.indexing.overlord;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.java.util.common.DateTimes;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

public class IndexerMetadataStorageAdapter
{
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  @Inject
  public IndexerMetadataStorageAdapter(
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator
  )
  {
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
  }

  public int deletePendingSegments(String dataSource, Interval deleteInterval)
  {
    // Check the given interval overlaps the interval(minCreatedDateOfActiveTasks, MAX)
    final Optional<DateTime> minCreatedDateOfActiveTasks = taskStorageQueryAdapter
            .getActiveTasks()
            .stream()
            .map(task -> Preconditions.checkNotNull(
                    taskStorageQueryAdapter.getCreatedTime(task.getId()),
                    "Can't find the createdTime for task[%s]",
                    task.getId()
            ))
            .min(Comparator.naturalOrder());

    final Interval activeTaskInterval = new Interval(
            minCreatedDateOfActiveTasks.orElse(DateTimes.MAX),
            DateTimes.MAX
    );

    Preconditions.checkArgument(
            !deleteInterval.overlaps(activeTaskInterval),
            "Cannot delete pendingSegments because there is at least one active task created at %s",
            activeTaskInterval.getStart()
    );

    return indexerMetadataStorageCoordinator.deletePendingSegments(dataSource, deleteInterval);
  }

  /**
   * Publishes externally-built segments (e.g. produced by a Spark job) directly into the metadata
   * store. Unlike {@link io.druid.indexing.common.actions.SegmentInsertAction}, this is not gated by
   * a task lock, so callers must use a distinct segment version per batch to avoid clobbering.
   */
  public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments) throws IOException
  {
    return indexerMetadataStorageCoordinator.announceHistoricalSegments(segments);
  }

}
