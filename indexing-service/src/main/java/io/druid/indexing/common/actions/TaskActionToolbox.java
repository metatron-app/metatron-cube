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

package io.druid.indexing.common.actions;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;

public class TaskActionToolbox
{
  private final TaskLockbox taskLockbox;
  private final CoordinatorClient coordinatorClient;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ServiceEmitter emitter;
  private final SupervisorManager supervisorManager;

  @Inject
  public TaskActionToolbox(
      TaskLockbox taskLockbox,
      CoordinatorClient coordinatorClient,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      ServiceEmitter emitter,
      SupervisorManager supervisorManager
  )
  {
    this.taskLockbox = taskLockbox;
    this.coordinatorClient = coordinatorClient;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.emitter = emitter;
    this.supervisorManager = supervisorManager;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public CoordinatorClient getCoordinatorClient()
  {
    return coordinatorClient;
  }

  public IndexerMetadataStorageCoordinator getIndexerMetadataStorageCoordinator()
  {
    return indexerMetadataStorageCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public SupervisorManager getSupervisorManager()
  {
    return supervisorManager;
  }

  public void verifyTaskLocks(
      final Task task,
      final Set<DataSegment> segments
  )
  {
    List<TaskLock> taskLocks = getTaskLockbox().findLocksForTask(task);
    DataSegment segment = taskLockCoversSegments(taskLocks, segments);
    if (segment != null) {
      throw new ISE("Segment %s is not covered by locks %s for task: %s", segment, taskLocks, task.getId());
    }
  }

  private DataSegment taskLockCoversSegments(
      final List<TaskLock> taskLocks,
      final Set<DataSegment> segments
  )
  {
    // Verify that each of these segments falls under some lock

    // NOTE: It is possible for our lock to be revoked (if the task has failed and given up its locks) after we check
    // NOTE: it and before we perform the segment insert, but, that should be OK since the worst that happens is we
    // NOTE: insert some segments from the task but not others.

    for (final DataSegment segment : segments) {
      final boolean ok = Iterables.any(
          taskLocks, new Predicate<TaskLock>()
          {
            @Override
            public boolean apply(TaskLock taskLock)
            {
              return taskLock.getDataSource().equals(segment.getDataSource())
                     && taskLock.getInterval().contains(segment.getInterval())
                     && taskLock.getVersion().compareTo(segment.getVersion()) >= 0;
            }
          }
      );

      if (!ok) {
        return segment;
      }
    }

    return null;
  }
}
