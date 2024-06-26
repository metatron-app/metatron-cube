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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.common.DateTimes;
import io.druid.indexing.common.TaskLock;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexer.TaskInfo;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.MetadataStorageActionHandler;
import io.druid.metadata.MetadataStorageActionHandlerFactory;
import io.druid.metadata.MetadataStorageActionHandlerTypes;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetadataTaskStorage implements TaskStorage
{
  private static final MetadataStorageActionHandlerTypes<Task, TaskStatus, TaskAction, TaskLock> TASK_TYPES = new MetadataStorageActionHandlerTypes<Task, TaskStatus, TaskAction, TaskLock>()
  {
    @Override
    public TypeReference<Task> getEntryType()
    {
      return new TypeReference<Task>()
      {
      };
    }

    @Override
    public TypeReference<TaskStatus> getStatusType()
    {
      return new TypeReference<TaskStatus>()
      {
      };
    }

    @Override
    public TypeReference<TaskAction> getLogType()
    {
      return new TypeReference<TaskAction>()
      {
      };
    }

    @Override
    public TypeReference<TaskLock> getLockType()
    {
      return new TypeReference<TaskLock>()
      {
      };
    }
  };

  private final MetadataStorageConnector metadataStorageConnector;
  private final TaskStorageConfig config;
  private final MetadataStorageActionHandler<Task, TaskStatus, TaskAction, TaskLock> handler;

  private static final EmittingLogger log = new EmittingLogger(MetadataTaskStorage.class);

  @Inject
  public MetadataTaskStorage(
      final MetadataStorageConnector metadataStorageConnector,
      final TaskStorageConfig config,
      final MetadataStorageActionHandlerFactory factory
  )
  {
    this.metadataStorageConnector = metadataStorageConnector;
    this.config = config;
    this.handler = factory.create(MetadataStorageTablesConfig.TASK_ENTRY_TYPE, TASK_TYPES);
  }

  @LifecycleStart
  public void start()
  {
    metadataStorageConnector.createTaskTables();
  }

  @LifecycleStop
  public void stop()
  {
    // do nothing
  }

  @Override
  public void insert(final Task task, final TaskStatus status) throws EntryExistsException
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(
        task.getId().equals(status.getId()),
        "Task/Status ID mismatch[%s/%s]",
        task.getId(),
        status.getId()
    );

    log.info("Inserting task %s with status: %s", task.getId(), status);

    try {
      handler.insert(
          task.getId(),
          new DateTime(),
          task.getDataSource(),
          task,
          status.isRunnable(),
          status
      );
    }
    catch (Exception e) {
      if(e instanceof EntryExistsException) {
        throw (EntryExistsException) e;
      } else {
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public void setStatus(final TaskStatus status)
  {
    Preconditions.checkNotNull(status, "status");

    log.info("Updating task %s to status: %s", status.getId(), status);

    final boolean set = handler.setStatus(
        status.getId(),
        status.isRunnable(),
        status
    );
    if (!set) {
      throw new IllegalStateException(String.format("Active task not found: %s", status.getId()));
    }
  }

  @Override
  public Optional<Task> getTask(final String taskId)
  {
      return handler.getEntry(taskId);
  }

  @Override
  public Optional<TaskStatus> getStatus(final String taskId)
  {
    return handler.getStatus(taskId);
  }

  @Override
  public List<Task> getActiveTasks()
  {
    List<Task> list = new ArrayList<>();
    for (TaskInfo<Task, TaskStatus> taskInfo : handler.getActiveTaskInfo(null)) {
      if (taskInfo.getStatus().isRunnable()) {
        Task task = taskInfo.getTask();
        list.add(task);
      }
    }
    return list;
  }

  @Override
  public List<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(@Nullable String dataSource)
  {
    return ImmutableList.copyOf(
        handler.getActiveTaskInfo(dataSource)
    );
  }

  @Override
  public List<TaskInfo<Task, TaskStatus>> getRecentlyFinishedTaskInfo(
      @Nullable Integer maxTaskStatuses,
      @Nullable Duration duration,
      @Nullable String datasource
  )
  {

    return ImmutableList.copyOf(
        handler.getCompletedTaskInfo(
            DateTimes.nowUtc().minus(duration == null ? config.getRecentlyFinishedThreshold() : duration),
            maxTaskStatuses,
            datasource
        )
    );
  }

  @Override
  public void addLock(final String taskid, final TaskLock taskLock)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(taskLock, "taskLock");

    log.info(
        "Adding lock on interval[%s] version[%s] for task: %s",
        taskLock.getInterval(),
        taskLock.getVersion(),
        taskid
    );

    handler.addLock(taskid, taskLock);
  }

  @Override
  public void removeLock(String taskid, TaskLock taskLockToRemove)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(taskLockToRemove, "taskLockToRemove");

    final Map<Long, TaskLock> taskLocks = getLocksWithIds(taskid);

    for (final Map.Entry<Long, TaskLock> taskLockWithId : taskLocks.entrySet()) {
      final long id = taskLockWithId.getKey();
      final TaskLock taskLock = taskLockWithId.getValue();

      if (taskLock.equals(taskLockToRemove)) {
        log.info("Deleting TaskLock with id[%d]: %s", id, taskLock);
        handler.removeLock(id);
      }
    }
  }

  @Override
  public void removeTasksOlderThan(long timestamp)
  {
    handler.removeTasksOlderThan(timestamp);
  }

  @Override
  public List<TaskLock> getLocks(String taskid)
  {
    return ImmutableList.copyOf(
        Iterables.transform(
            getLocksWithIds(taskid).entrySet(), new Function<Map.Entry<Long, TaskLock>, TaskLock>()
            {
              @Override
              public TaskLock apply(Map.Entry<Long, TaskLock> e)
              {
                return e.getValue();
              }
            }
        )
    );
  }

  @Override
  public <T> void addAuditLog(final Task task, final TaskAction<T> taskAction)
  {
    Preconditions.checkNotNull(taskAction, "taskAction");

    log.info("Logging action for task[%s]: %s", task.getId(), taskAction);

    handler.addLog(task.getId(), taskAction);
  }

  @Override
  public List<TaskAction> getAuditLogs(final String taskId)
  {
    return handler.getLogs(taskId);
  }

  private Map<Long, TaskLock> getLocksWithIds(final String taskid)
  {
    return handler.getLocks(taskid);
  }

  @Nullable
  @Override
  public Pair<DateTime, String> getCreatedDateTimeAndDataSource(String taskId)
  {
    return handler.getCreatedDateAndDataSource(taskId);
  }

}
