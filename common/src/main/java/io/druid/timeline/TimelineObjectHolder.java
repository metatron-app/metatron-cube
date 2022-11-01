/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Preconditions;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

/**
*/
public class TimelineObjectHolder<ObjectType> implements LogicalSegment
{
  private final Interval interval;
  private final String version;
  private final PartitionHolder<ObjectType> object;

  public TimelineObjectHolder(
      Interval interval,
      String version,
      PartitionHolder<ObjectType> object
  )
  {
    this.interval = interval;
    this.version = version;
    this.object = object;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  public String getVersion()
  {
    return version;
  }

  public PartitionHolder<ObjectType> getObject()
  {
    return object;
  }

  public TimelineObjectHolder<ObjectType> withOverlap(Interval interval)
  {
    return withOverlap(interval.getStartMillis(), interval.getEndMillis());
  }

  public TimelineObjectHolder<ObjectType> withOverlap(long start, long end)
  {
    start = Math.max(start, interval.getStartMillis());
    end = Math.min(end, interval.getEndMillis());
    Preconditions.checkArgument(start < end);
    return new TimelineObjectHolder<>(new Interval(start, end, interval.getChronology()), version, object);
  }

  @Override
  public String toString()
  {
    return "TimelineObjectHolder{" +
           "interval=" + interval +
           ", version=" + version +
           ", object=" + object +
           '}';
  }
}
