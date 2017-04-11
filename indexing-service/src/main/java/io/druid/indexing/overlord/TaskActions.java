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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonValue;
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
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class TaskActions
{
  private static final Logger logger = new Logger(TaskActions.class);

  public static Event toEvent(Task task, TaskAction action)
  {
    logger.debug("Making event.. %s : %s", task, action);
    if (action instanceof SegmentInsertAction) {
      SegmentInsertAction insert = (SegmentInsertAction) action;
      Map<String, Object> event = Maps.newHashMap();
      event.put("feed", "action");
      event.put("type", "insert");
      event.put("createdTime", System.currentTimeMillis());
      event.put("dataSource", task.getDataSource());
      event.put("segments", insert.getSegments());
      return new TaskActionEvent(event);
    } else if (action instanceof DataSourceMetadataUpdateAction) {
      DataSourceMetadataUpdateAction dsUpdate = (DataSourceMetadataUpdateAction)action;
      Map<String, Object> event = Maps.newHashMap();
      event.put("feed", "action");
      event.put("type", "dataSourceMetaUpdate");
      event.put("createdTime", System.currentTimeMillis());
      event.put("dataSource", task.getDataSource());
      event.put("metaData", dsUpdate.getMetaData());
      return new TaskActionEvent(event);
    } else if (action instanceof SegmentTransactionalInsertAction) {
      SegmentTransactionalInsertAction dsUpdate = (SegmentTransactionalInsertAction)action;
      Map<String, Object> event = Maps.newHashMap();
      event.put("feed", "action");
      event.put("type", "transactionalInsert");
      event.put("createdTime", System.currentTimeMillis());
      event.put("dataSource", task.getDataSource());
      event.put("segments", dsUpdate.getSegments());
      event.put("startMetaData", dsUpdate.getStartMetadata());
      event.put("endMetaData", dsUpdate.getEndMetadata());
      return new TaskActionEvent(event);
    } else if (action instanceof LockTryAcquireAction) {
      LockTryAcquireAction tryLock = (LockTryAcquireAction)action;
      Map<String, Object> event = Maps.newHashMap();
      event.put("feed", "action");
      event.put("type", "tryLock");
      event.put("createdTime", System.currentTimeMillis());
      event.put("dataSource", task.getDataSource());
      event.put("interval", tryLock.getInterval());
      return new TaskActionEvent(event);
    } else if (action instanceof SegmentAllocateAction) {
      SegmentAllocateAction allocate = (SegmentAllocateAction)action;
      Map<String, Object> event = Maps.newHashMap();
      event.put("feed", "action");
      event.put("type", "segmentAllocate");
      event.put("createdTime", System.currentTimeMillis());
      event.put("dataSource", task.getDataSource());
      event.put("timestamp", allocate.getTimestamp());
      event.put("sequenceName", allocate.getSequenceName());
      event.put("preferredSegmentGranularity", allocate.getPreferredSegmentGranularity());
      event.put("previousSegmentId", allocate.getPreviousSegmentId());
      return new TaskActionEvent(event);
    }
    return null;
  }

  public static final class TaskActionEvent implements Event
  {
    private final Map<String, Object> event;

    public TaskActionEvent(Map<String, Object> event)
    {
      this.event = event;
    }

    @Override
    @JsonValue
    public Map<String, Object> toMap()
    {
      return event;
    }

    @Override
    public String getFeed()
    {
      return "action";
    }

    @Override
    public DateTime getCreatedTime()
    {
      return new DateTime(event.get("createdTime"));
    }

    @Override
    public boolean isSafeToBuffer()
    {
      return true;
    }
  }
}
