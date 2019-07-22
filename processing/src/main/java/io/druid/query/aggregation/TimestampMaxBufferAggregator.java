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

package io.druid.query.aggregation;

import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.segment.ObjectColumnSelector;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class TimestampMaxBufferAggregator extends BufferAggregator.Abstract
{
  private final ObjectColumnSelector selector;
  private final DefaultTimestampSpec timestampSpec;

  public TimestampMaxBufferAggregator(ObjectColumnSelector selector, DefaultTimestampSpec timestampSpec)
  {
    this.selector = selector;
    this.timestampSpec = timestampSpec;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Long newTime = convertLong();
    if (newTime != null) {
      buf.putLong(position, Math.max(buf.getLong(position), newTime));
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  public Long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  private Long convertLong()
  {
    Object o = selector.get();
    if (o instanceof Number) {
      return ((Number)o).longValue();
    } else if (o instanceof DateTime) {
      return ((DateTime)o).getMillis();
    } else if (o instanceof Timestamp) {
      return ((Timestamp)o).getTime();
    } else if (o instanceof String) {
      return timestampSpec.parseDateTime(o).getMillis();
    }

    return null;
  }
}
