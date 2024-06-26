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

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.indexer.TaskInfo;
import io.druid.indexing.common.TaskLock;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.Pair;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Wraps a {@link TaskStorage}, providing a useful collection of read-only methods.
 */
public class TaskStorageQueryAdapter
{
  private final TaskStorage storage;

  @Inject
  public TaskStorageQueryAdapter(TaskStorage storage)
  {
    this.storage = storage;
  }

  public List<Task> getActiveTasks()
  {
    return storage.getActiveTasks();
  }

  public List<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(@Nullable String dataSource)
  {
    return storage.getActiveTaskInfo(dataSource);
  }

  public List<TaskInfo<Task, TaskStatus>> getRecentlyCompletedTaskInfo(
      @Nullable Integer maxTaskStatuses,
      @Nullable Duration duration,
      @Nullable String dataSource
  )
  {
    return storage.getRecentlyFinishedTaskInfo(maxTaskStatuses, duration, dataSource);
  }

  @Nullable
  public DateTime getCreatedTime(String taskId)
  {
    final Pair<DateTime, String> pair = storage.getCreatedDateTimeAndDataSource(taskId);
    return pair == null ? null : pair.lhs;
  }


  public Optional<Task> getTask(final String taskid)
  {
    return storage.getTask(taskid);
  }

  public List<TaskLock> getLocks(String taskid)
  {
    return storage.getLocks(taskid);
  }

  public Optional<TaskStatus> getStatus(final String taskid)
  {
    return storage.getStatus(taskid);
  }

  /**
   * Returns all segments created by this task.
   *
   * This method is useful when you want to figure out all of the things a single task spawned.  It does pose issues
   * with the result set perhaps growing boundlessly and we do not do anything to protect against that.  Use at your
   * own risk and know that at some point, we might adjust this to actually enforce some sort of limits.
   *
   * @param taskid task ID
   * @return set of segments created by the specified task
   */
  public Set<DataSegment> getInsertedSegments(final String taskid)
  {
    final Set<DataSegment> segments = Sets.newHashSet();
    for (final TaskAction action : storage.getAuditLogs(taskid)) {
      if (action instanceof SegmentInsertAction) {
        segments.addAll(((SegmentInsertAction) action).getSegments());
      }
    }
    return segments;
  }

}
