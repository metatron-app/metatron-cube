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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import io.druid.common.guava.GuavaUtils;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.query.DruidMetrics;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Set;

/**
 * Insert segments into metadata storage. The segment versions must all be less than or equal to a lock held by
 * your task for the segment intervals.
 * <p/>
 * Word of warning: Very large "segments" sets can cause oversized audit log entries, which is bad because it means
 * that the task cannot actually complete. Callers should avoid this by avoiding inserting too many segments in the
 * same action.
 */
public class SegmentTransactionalInsertAction implements TaskAction<SegmentPublishResult>
{
  private final Set<DataSegment> segments;
  private final DataSourceMetadata startMetadata;
  private final DataSourceMetadata endMetadata;

  public SegmentTransactionalInsertAction(
      Set<DataSegment> segments
  )
  {
    this(segments, null, null);
  }

  @JsonCreator
  public SegmentTransactionalInsertAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("startMetadata") DataSourceMetadata startMetadata,
      @JsonProperty("endMetadata") DataSourceMetadata endMetadata
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
    this.startMetadata = startMetadata;
    this.endMetadata = endMetadata;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public DataSourceMetadata getStartMetadata()
  {
    return startMetadata;
  }

  @JsonProperty
  public DataSourceMetadata getEndMetadata()
  {
    return endMetadata;
  }

  public TypeReference<SegmentPublishResult> getReturnTypeReference()
  {
    return new TypeReference<SegmentPublishResult>()
    {
    };
  }

  /**
   * Behaves similarly to
   * {@link io.druid.indexing.overlord.IndexerMetadataStorageCoordinator#announceHistoricalSegments(Iterable, DataSourceMetadata, DataSourceMetadata)}.
   */
  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    toolbox.verifyTaskLocks(task, segments);

    final SegmentPublishResult retVal = toolbox.getIndexerMetadataStorageCoordinator().announceHistoricalSegments(
        segments,
        startMetadata,
        endMetadata
    );

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
        .setDimension(DruidMetrics.DATASOURCE, task.getDataSource())
        .setDimension(DruidMetrics.TASK_TYPE, task.getType());

    if (retVal.isSuccess()) {
      toolbox.getEmitter().emit(metricBuilder.build("segment/txn/success", 1));
    } else {
      toolbox.getEmitter().emit(metricBuilder.build("segment/txn/failure", 1));
    }

    for (DataSegment segment : retVal.getSegments()) {
      metricBuilder.setDimension(DruidMetrics.INTERVAL, segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("segment/added/bytes", segment.getSize()));
    }

    return retVal;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentInsertAction{" +
           "segments=" + GuavaUtils.transform(segments, DataSegment::getIdentifier) +
           ", startMetadata=" + startMetadata +
           ", endMetadata=" + endMetadata +
           '}';
  }
}
