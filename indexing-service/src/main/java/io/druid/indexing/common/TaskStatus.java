/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.druid.query.QueryInterruptedException;

/**
 * Represents the status of a task from the perspective of the coordinator. The task may be ongoing
 * ({@link #isComplete()} false) or it may be complete ({@link #isComplete()} true).
 *
 * TaskStatus objects are immutable.
 */
public class TaskStatus
{
  public static enum Status
  {
    RUNNING,
    SUCCESS,
    FAILED
  }

  public static Predicate<TaskStatus> asPredicate(final Status status)
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
    return new TaskStatus(taskId, Status.RUNNING, -1, null);
  }

  public static TaskStatus success(String taskId)
  {
    return new TaskStatus(taskId, Status.SUCCESS, -1, null);
  }

  public static TaskStatus failure(String taskId, String reason)
  {
    return new TaskStatus(taskId, Status.FAILED, -1, reason);
  }

  public static TaskStatus failure(String taskId, Throwable t)
  {
    return new TaskStatus(taskId, Status.FAILED, -1, "Exception: " + QueryInterruptedException.stackTrace(t));
  }

  public static TaskStatus fromCode(String taskId, Status code, String reason)
  {
    return new TaskStatus(taskId, code, -1, reason);
  }

  private final String id;
  private final Status status;
  private final long duration;
  private final String reason;

  @JsonCreator
  private TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") Status status,
      @JsonProperty("duration") long duration,
      @JsonProperty("reason") String reason
  )
  {
    this.id = id;
    this.status = status;
    this.duration = duration;
    this.reason = reason;

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
  public Status getStatusCode()
  {
    return status;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  @JsonProperty("reason")
  public String getReason()
  {
    return reason;
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
    return status == Status.RUNNING;
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
    return status == Status.SUCCESS;
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
    return status == Status.FAILED;
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, _duration, reason);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("duration", duration)
                  .toString();
  }
}
