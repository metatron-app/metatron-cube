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

package io.druid.indexing.common.actions;

import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Event;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskActions;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.server.log.EventForwarder;

import java.io.IOException;
import java.util.Objects;

public class LocalTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final TaskStorage storage;
  private final TaskActionToolbox toolbox;
  private final EventForwarder forwarder;

  private static final EmittingLogger log = new EmittingLogger(LocalTaskActionClient.class);

  public LocalTaskActionClient(Task task, TaskStorage storage, TaskActionToolbox toolbox, EventForwarder forwarder)
  {
    this.task = task;
    this.storage = storage;
    this.toolbox = toolbox;
    this.forwarder = forwarder;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    if (taskAction.isAudited()) {
      // Add audit log
      try {
        storage.addAuditLog(task, taskAction);
      }
      catch (Exception e) {
        final String actionClass = taskAction.getClass().getName();
        log.makeAlert(e, "Failed to record action in audit log")
           .addData("task", task.getId())
           .addData("actionClass", actionClass)
           .emit();
        throw new ISE(e, "Failed to record action [%s] in audit log", actionClass);
      }
    }
    RetType result = taskAction.perform(task, toolbox);

    if (forwarder != null) {
      Event event = TaskActions.toEvent(task, taskAction);
      if (event != null) {
        log.info("Emitting.. %s", event.toMap());
        String postURL = Objects.toString(task.getContextValue("task.action.postURL"), null);
        Long timeout = null;
        Object timeoutValue = task.getContextValue("task.action.readTimeout");
        if (timeoutValue instanceof Long) {
          timeout = (Long)timeoutValue;
        } else if (timeoutValue instanceof String) {
          timeout = Longs.tryParse((String)timeoutValue);
        }
        forwarder.forward(postURL, event, timeout);
      }
    }
    return result;
  }
}
