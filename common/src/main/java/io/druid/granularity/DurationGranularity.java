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

package io.druid.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.DateTimes;
import io.druid.common.KeyBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

/**
 * DurationGranularity buckets data based on the length of a duration
 */
public class DurationGranularity extends Granularity
{
  private final long duration;
  private final long origin;

  @JsonCreator
  public DurationGranularity(
      @JsonProperty("duration") long duration,
      @JsonProperty("origin") DateTime origin
  )
  {
    this(duration, origin == null ? 0 : origin.getMillis());
  }

  public DurationGranularity(long duration, long origin)
  {
    Preconditions.checkArgument(duration > 0, "duration should be greater than 0!");
    this.duration = duration;
    this.origin = origin % duration;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  @JsonProperty("origin")
  public DateTime getOrigin()
  {
    return DateTimes.utc(origin);
  }

  public long getOriginMillis()
  {
    return origin;
  }

  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public DateTime increment(DateTime time)
  {
    return time.plus(getDuration());
  }

  @Override
  public DateTime decrement(DateTime time)
  {
    return time.minus(getDuration());
  }

  @Override
  public DateTime bucketStart(DateTime time)
  {
    return new DateTime(bucketStart(time.getMillis()), time.getChronology());
  }

  @Override
  public long bucketStart(long current)
  {
    final long duration = getDurationMillis();
    long offset = current % duration - origin;
    if (offset < 0) {
      offset += duration;
    }
    return current - offset;
  }

  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(duration).append(origin);
  }

  public long getDurationMillis()
  {
    return duration;
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

    DurationGranularity that = (DurationGranularity) o;

    if (duration != that.duration) {
      return false;
    }
    if (origin != that.origin) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (int) (duration ^ (duration >>> 32));
    result = 31 * result + (int) (origin ^ (origin >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "DurationGranularity{" +
           "duration=" + duration +
           ", origin=" + origin +
           '}';
  }
}
