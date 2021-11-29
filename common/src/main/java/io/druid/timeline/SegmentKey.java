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

package io.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.utils.JodaUtils;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.Objects;

public class SegmentKey implements Comparable<SegmentKey>
{
  private final Interval interval;
  private final String version;

  @JsonCreator
  public SegmentKey(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version
  )
  {
    this.interval = interval;
    this.version = version;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
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
    SegmentKey record = (SegmentKey) o;
    return Objects.equals(version, record.version) && Objects.equals(interval, record.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(version, interval);
  }

  private static final Comparator<Interval> COMPARATOR = JodaUtils.intervalsByStartThenEnd();

  @Override
  public int compareTo(SegmentKey o)
  {
    return COMPARATOR.compare(interval, o.interval);
  }
}
