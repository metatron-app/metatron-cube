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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import io.druid.common.utils.JodaUtils;
import io.druid.granularity.Granularity;
import io.druid.indexing.common.task.Task;
import io.druid.query.SegmentDescriptor;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SegmentAppendingAction implements TaskAction<List<SegmentDescriptor>>
{
  private final String dataSource;
  private final List<Interval> intervals;   // this should be split on segment granularity
  private final Granularity segmentGranularity;

  @JsonCreator
  public SegmentAppendingAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") List<Interval> intervals,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
      )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.intervals = Preconditions.checkNotNull(intervals);   // assured not overlapping
    this.segmentGranularity = segmentGranularity;
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
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
    final Map<Interval, List<DataSegment>> segmentsMap = Maps.newHashMap();
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
      if (segments != null && !segments.isEmpty()) {
        appendingSegments.add(makeSegment(interval, segments));
        continue;
      }
      if (segmentGranularity == null) {
        checkOverlapped(segmentsMap.keySet(), interval);
      } else {
        for (Interval segmented : segmentGranularity.getIterable(interval)) {
          segments = segmentsMap.remove(segmented);
          if (segments != null && !segments.isEmpty()) {
            appendingSegments.add(makeSegment(segmented, segments));
          } else {
            checkOverlapped(segmentsMap.keySet(), segmented);
          }
        }
      }
    }
    return appendingSegments;
  }

  private void checkOverlapped(Set<Interval> intervals, Interval interval)
  {
    List<Interval> overlapping = JodaUtils.overlapping(interval, intervals);
    if (!overlapping.isEmpty()) {
      throw new ISE("%s is overlapping with existing %s", interval, overlapping);
    }
  }

  private SegmentDescriptor makeSegment(Interval interval, List<DataSegment> segments)
  {
    String version = null;
    int maxPartitionNum = -1;
    for (DataSegment segment : segments) {
      ShardSpec shardSpec = segment.getShardSpec();
      if (!(shardSpec instanceof LinearShardSpec)) {
        throw new ISE("segment %s is not linear shard", segment);
      }
      if (!interval.contains(segment.getInterval())) {
        throw new ISE("segment %s is not contained in the interval %s", segment, interval);
      }
      if (version == null) {
        version = segment.getVersion();
        maxPartitionNum = shardSpec.getPartitionNum();
      } else {
        Preconditions.checkArgument(
            version.equals(segment.getVersion()), "version conflict in a chunk.. should not be happened"
        );
        maxPartitionNum = Math.max(maxPartitionNum, shardSpec.getPartitionNum());
      }
    }
    return new SegmentDescriptor(interval, version, maxPartitionNum + 1);
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
