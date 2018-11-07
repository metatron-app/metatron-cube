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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import io.druid.granularity.Granularity;
import io.druid.query.SegmentDescriptor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;

/**
 */
public class AppendingGranularitySpec implements GranularitySpec
{
  private final GranularitySpec granularitySpec;
  private final List<SegmentDescriptor> descriptors;

  @JsonCreator
  public AppendingGranularitySpec(
      @JsonProperty("queryGranularity") GranularitySpec granularitySpec,
      @JsonProperty("descriptors") List<SegmentDescriptor> descriptors
  )
  {
    this.granularitySpec = granularitySpec;
    this.descriptors = descriptors;
  }

  @JsonProperty("queryGranularity")
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty("descriptors")
  public List<SegmentDescriptor> getSegmentDescriptors()
  {
    return descriptors;
  }

  @Override
  public Interval umbrellaInterval()
  {
    return granularitySpec.umbrellaInterval();
  }

  @Override
  public Optional<SortedSet<Interval>> bucketIntervals()
  {
    return granularitySpec.bucketIntervals();
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return granularitySpec.bucketInterval(dt);
  }

  public SegmentDescriptor getSegmentDescriptor(long timestamp)
  {
    for (SegmentDescriptor descriptor : descriptors) {
      if (descriptor.getInterval().contains(timestamp)) {
        return descriptor;
      }
    }
    return null;
  }

  @Override
  public Granularity getSegmentGranularity()
  {
    return granularitySpec.getSegmentGranularity();
  }

  @Override
  public boolean isRollup()
  {
    return granularitySpec.isRollup();
  }

  @Override
  public Granularity getQueryGranularity()
  {
    return granularitySpec.getQueryGranularity();
  }

  @Override
  public boolean isAppending()
  {
    return true;
  }
}
