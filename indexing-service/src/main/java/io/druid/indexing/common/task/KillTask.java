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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUnusedAction;
import io.druid.indexing.common.actions.SegmentNukeAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class KillTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(KillTask.class);
  private static final int NUKE_BATCH = 16;

  @JsonCreator
  public KillTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        makeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
  }

  @Override
  public String getType()
  {
    return "kill";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    // Confirm we have a lock (will throw if there isn't exactly one element)
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));

    if (!myLock.getDataSource().equals(getDataSource())) {
      throw new ISE("Lock dataSource[%s] != task dataSource[%s]", myLock.getDataSource(), getDataSource());
    }

    if (!myLock.getInterval().equals(getInterval())) {
      throw new ISE("Lock interval[%s] != task interval[%s]", myLock.getInterval(), getInterval());
    }

    // List unused segments
    final TaskActionClient actionClient = toolbox.getTaskActionClient();
    final List<DataSegment> unusedSegments = actionClient
        .submit(new SegmentListUnusedAction(myLock.getDataSource(), myLock.getInterval()));

    // Verify none of these segments have versions > lock version
    for (final DataSegment unusedSegment : unusedSegments) {
      if (unusedSegment.getVersion().compareTo(myLock.getVersion()) > 0) {
        throw new ISE(
            "Unused segment[%s] has version[%s] > task version[%s]",
            unusedSegment.getIdentifier(),
            unusedSegment.getVersion(),
            myLock.getVersion()
        );
      }

      log.info("OK to kill segment: %s", unusedSegment.getIdentifier());
    }

    // Kill segments
    Set<DataSegment> segments = Sets.newHashSet();
    for (DataSegment segment : unusedSegments) {
      toolbox.getDataSegmentKiller().kill(segment);
      segments.add(segment);
      if (segments.size() >= NUKE_BATCH) {
        actionClient.submit(new SegmentNukeAction(segments));
        segments.clear();
      }
    }
    if (!segments.isEmpty()) {
      actionClient.submit(new SegmentNukeAction(segments));
      segments.clear();
    }

    return TaskStatus.success(getId());
  }
}
