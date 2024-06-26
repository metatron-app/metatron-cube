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

import com.google.common.util.concurrent.SettableFuture;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.worker.Worker;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

/**
 */
public class RemoteTaskRunnerWorkItem extends TaskRunnerWorkItem
{
  private final SettableFuture<TaskStatus> result;
  private final Worker worker;
  private TaskLocation location;

  public RemoteTaskRunnerWorkItem(
      String taskId,
      Worker worker,
      TaskLocation location
  )
  {
    this(taskId, SettableFuture.<TaskStatus>create(), worker, location);
  }

  public RemoteTaskRunnerWorkItem(
      String taskId,
      DateTime createdTime,
      DateTime queueInsertionTime,
      Worker worker,
      TaskLocation location
  )
  {
    this(taskId, SettableFuture.<TaskStatus>create(), createdTime, queueInsertionTime, worker, location);
  }

  private RemoteTaskRunnerWorkItem(
      String taskId,
      SettableFuture<TaskStatus> result,
      Worker worker,
      TaskLocation location
  )
  {
    super(taskId, result);
    this.result = result;
    this.worker = worker;
    this.location = location == null ? TaskLocation.unknown() : location;
  }

  private RemoteTaskRunnerWorkItem(
      String taskId,
      SettableFuture<TaskStatus> result,
      DateTime createdTime,
      DateTime queueInsertionTime,
      Worker worker,
      TaskLocation location
  )
  {
    super(taskId, result, createdTime, queueInsertionTime);
    this.result = result;
    this.worker = worker;
    this.location = location == null ? TaskLocation.unknown() : location;
  }

  public void setLocation(TaskLocation location)
  {
    this.location = location;
  }

  @Override
  public TaskLocation getLocation()
  {
    return location;
  }

  @Nullable
  @Override
  public String getTaskType()
  {
    // TODO #5102
    return "";
  }

  @Override
  public String getDataSource()
  {
    // TODO #5260
    return "";
  }

  public Worker getWorker()
  {
    return worker;
  }

  public void setResult(TaskStatus status)
  {
    result.set(status);
  }

  public RemoteTaskRunnerWorkItem withQueueInsertionTime(DateTime time)
  {
    return new RemoteTaskRunnerWorkItem(getTaskId(), result, getCreatedTime(), time, worker, location);
  }

  public RemoteTaskRunnerWorkItem withWorker(Worker theWorker, TaskLocation location)
  {
    return new RemoteTaskRunnerWorkItem(
        getTaskId(),
        result,
        getCreatedTime(),
        getQueueInsertionTime(),
        theWorker,
        location
    );
  }
}
