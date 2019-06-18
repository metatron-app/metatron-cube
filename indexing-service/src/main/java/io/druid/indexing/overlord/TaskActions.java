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

import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.core.Event;
import io.druid.indexing.common.actions.DataSourceMetadataUpdateAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.task.Task;
import io.druid.server.log.Events;

import java.util.Map;

/**
 */
public class TaskActions
{
  private static final Logger logger = new Logger(TaskActions.class);

  public static Event toEvent(Task task, TaskAction action)
  {
    logger.debug("Making event.. %s : %s", task, action);
    Map<String, Object> event = Maps.newHashMap();
    event.put("feed", "TaskAction");
    event.put("type", action.getClass().getSimpleName());
    event.put("createdTime", System.currentTimeMillis());
    if (action instanceof SegmentInsertAction) {
      SegmentInsertAction insert = (SegmentInsertAction) action;
      event.put("dataSource", task.getDataSource());
      event.put("segments", insert.getSegments());
    } else if (action instanceof DataSourceMetadataUpdateAction) {
      DataSourceMetadataUpdateAction dsUpdate = (DataSourceMetadataUpdateAction)action;
      event.put("dataSource", task.getDataSource());
      event.put("metaData", dsUpdate.getMetaData());
    } else if (action instanceof SegmentTransactionalInsertAction) {
      SegmentTransactionalInsertAction dsUpdate = (SegmentTransactionalInsertAction)action;
      event.put("dataSource", task.getDataSource());
      event.put("segments", dsUpdate.getSegments());
      event.put("startMetaData", dsUpdate.getStartMetadata());
      event.put("endMetaData", dsUpdate.getEndMetadata());
    } else if (action instanceof LockTryAcquireAction) {
      LockTryAcquireAction tryLock = (LockTryAcquireAction)action;
      event.put("dataSource", task.getDataSource());
      event.put("interval", tryLock.getInterval());
    } else if (action instanceof SegmentAllocateAction) {
      SegmentAllocateAction allocate = (SegmentAllocateAction)action;
      event.put("dataSource", task.getDataSource());
      event.put("timestamp", allocate.getTimestamp());
      event.put("sequenceName", allocate.getSequenceName());
      event.put("preferredSegmentGranularity", allocate.getPreferredSegmentGranularity());
      event.put("previousSegmentId", allocate.getPreviousSegmentId());
    }
    return new Events.SimpleEvent(event);
  }
}
