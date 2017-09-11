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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.utils.JodaUtils;
import io.druid.indexing.common.task.Task;
import io.druid.query.SegmentDescriptor;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SegmentAppendingAction implements TaskAction<List<SegmentDescriptor>>
{
  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final List<Interval> intervals;   // this should be split on segment granularity

  @JsonCreator
  public SegmentAppendingAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.intervals = Preconditions.checkNotNull(intervals);
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  public TypeReference<List<SegmentDescriptor>> getReturnTypeReference()
  {
    return new TypeReference<List<SegmentDescriptor>>()
    {
    };
  }

  @Override
  public List<SegmentDescriptor> perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    Map<Interval, List<DataSegment>> segmentsMap = Maps.newHashMap();
    for (DataSegment segment : toolbox.getIndexerMetadataStorageCoordinator()
                                      .getUsedSegmentsForIntervals(dataSource, intervals)) {
      List<DataSegment> segments = segmentsMap.get(segment.getInterval());
      if (segments == null) {
        segmentsMap.put(segment.getInterval(), segments = Lists.newArrayList());
      }
      segments.add(segment);
    }

    List<SegmentDescriptor> appendingSegments = Lists.newArrayList();
    for (Interval interval : intervals) {
      List<DataSegment> segments = segmentsMap.remove(interval);
      if (segments == null) {
        if (JodaUtils.overlaps(interval, segmentsMap.keySet())) {
          throw new IllegalStateException("some overlapping interval");
        }
        continue;
      }
      String version = null;
      int maxPartitionNum = -1;
      for (DataSegment segment : segments) {
        ShardSpec shardSpec = segment.getShardSpec();
        if (!(shardSpec instanceof LinearShardSpec)) {
          throw new IllegalStateException("cannot append to non-linear shard");
        }
        if (version == null) {
          version = segment.getVersion();
          maxPartitionNum = shardSpec.getPartitionNum();
          continue;
        }
        if (!version.equals(segment.getVersion())) {
          throw new IllegalStateException("segment version conflicts");
        }
        maxPartitionNum = Math.max(maxPartitionNum, shardSpec.getPartitionNum());
      }
      appendingSegments.add(new SegmentDescriptor(interval, version, maxPartitionNum + 1));
    }
    return appendingSegments;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SegmentAppendingAction that = (SegmentAppendingAction) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    return intervals.equals(that.intervals);

  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + intervals.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "FindAppendingSegmentsAction{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           '}';
  }
}
