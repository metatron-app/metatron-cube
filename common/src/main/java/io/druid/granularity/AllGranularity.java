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

import com.google.common.collect.ImmutableList;
import io.druid.common.DateTimes;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.JodaUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

/**
 * AllGranularty buckets everything into a single bucket
 */
public class AllGranularity extends Granularity
{
  /**
   * This constructor is public b/c it is serialized and deserialized
   * based on type in GranularityModule
   */
  public AllGranularity() {}

  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public DateTime increment(DateTime time)
  {
    return DateTimes.MAX;
  }

  @Override
  public DateTime decrement(DateTime time)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public DateTime bucketStart(DateTime time)
  {
    return DateTimes.MIN;
  }

  @Override
  public long bucketStart(long current)
  {
    return JodaUtils.MIN_INSTANT;
  }

  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append((byte) 0x7f);
  }

  @Override
  public Iterable<Interval> getIterable(Interval input)
  {
    return ImmutableList.of(input);
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

    return true;
  }

  @Override
  public int hashCode()
  {
    return getClass().hashCode();
  }

  @Override
  public String toString()
  {
    return "AllGranularity";
  }
}
