/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.druid.common.utils.ExceptionUtils;

import java.util.Map;

/**
 * Represents the status of a task from the perspective of the coordinator. The task may be ongoing
 * ({@link #isComplete()} false) or it may be complete ({@link #isComplete()} true).
 * <p>
 * TaskStatus objects are immutable.
 */
public class TaskStatus
{
  public static Predicate<TaskStatus> asPredicate(final TaskState status)
  {
    return new Predicate<TaskStatus>()
    {
      @Override
      public boolean apply(TaskStatus input)
      {
        return input.status == status;
      }
    };
  }

  public static TaskStatus running(String taskId)
  {
    return new TaskStatus(taskId, TaskState.RUNNING, -1, null, null);
  }

  public static TaskStatus success(String taskId)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, -1, null, null);
  }

  public static TaskStatus success(String taskId, Map<String, Object> stats)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, -1, null, stats);
  }

  public static TaskStatus failure(String taskId, String reason)
  {
    return new TaskStatus(taskId, TaskState.FAILED, -1, reason, null);
  }

  public static TaskStatus failure(String taskId, Throwable t)
  {
    return new TaskStatus(taskId, TaskState.FAILED, -1, "Exception: " + ExceptionUtils.stackTrace(t), null);
  }

  public static TaskStatus fromCode(String taskId, TaskState code, String reason)
  {
    return new TaskStatus(taskId, code, -1, reason, null);
  }

  private final String id;
  private final TaskState status;
  private final long duration;
  private final String reason;
  private final Map<String, Object> stats;

  @JsonCreator
  private TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") TaskState status,
      @JsonProperty("duration") long duration,
      @JsonProperty("reason") String reason,
      @JsonProperty("stats") Map<String, Object> stats
  )
  {
    this.id = id;
    this.status = status;
    this.duration = duration;
    this.reason = reason;
    this.stats = stats;

    // Check class invariants.
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkNotNull(status, "status");
  }

  @JsonProperty("id")
  public String getId()
  {
    return id;
  }

  @JsonProperty("status")
  public TaskState getStatusCode()
  {
    return status;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  @JsonProperty("reason")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getReason()
  {
    return reason;
  }

  @JsonProperty("stats")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getStats()
  {
    return stats;
  }

  /**
   * Signals that a task is not yet complete, and is still runnable on a worker. Exactly one of isRunnable,
   * isSuccess, or isFailure will be true at any one time.
   *
   * @return whether the task is runnable.
   */
  @JsonIgnore
  public boolean isRunnable()
  {
    return status == TaskState.RUNNING;
  }

  /**
   * Inverse of {@link #isRunnable}.
   *
   * @return whether the task is complete.
   */
  @JsonIgnore
  public boolean isComplete()
  {
    return !isRunnable();
  }

  /**
   * Returned by tasks when they spawn subtasks. Exactly one of isRunnable, isSuccess, or isFailure will
   * be true at any one time.
   *
   * @return whether the task succeeded.
   */
  @JsonIgnore
  public boolean isSuccess()
  {
    return status == TaskState.SUCCESS;
  }

  /**
   * Returned by tasks when they complete unsuccessfully. Exactly one of isRunnable, isSuccess, or
   * isFailure will be true at any one time.
   *
   * @return whether the task failed
   */
  @JsonIgnore
  public boolean isFailure()
  {
    return status == TaskState.FAILED;
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, _duration, reason, stats);
  }

  @Override
  public String toString()
  {
    return "TaskStatus{" +
            "id='" + id + '\'' +
            ", status='" + status + '\'' +
            ", duration=" + duration +
            ", stats=" + stats +
            '}';
  }
}
