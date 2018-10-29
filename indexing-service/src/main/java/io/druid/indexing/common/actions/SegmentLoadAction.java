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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.druid.indexing.common.task.Task;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SegmentLoadAction implements TaskAction<Map<String, Object>>
{
  private final Set<DataSegment> segments;

  @JsonCreator
  public SegmentLoadAction(
      @JsonProperty("segments") Set<DataSegment> segments
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  public TypeReference<Map<String, Object>> getReturnTypeReference()
  {
    return new TypeReference<Map<String, Object>>()
    {
    };
  }

  @Override
  public Map<String, Object> perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    // can be null in tests & hard to mock that
    if (toolbox.getCoordinatorClient() != null) {
      return toolbox.getCoordinatorClient().scheduleNow(segments, 0L, false);
    }
    return ImmutableMap.of();
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentLoadAction{" +
           "segments=" + Iterables.transform(segments, DataSegment.GET_ID) +
           '}';
  }
}
