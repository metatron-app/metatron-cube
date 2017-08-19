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

package io.druid.query.aggregation;

import io.druid.data.input.impl.TimestampSpec;
import io.druid.segment.ObjectColumnSelector;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Comparator;

public class TimestampMaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongMaxAggregator.COMPARATOR;

  static long combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number)lhs).longValue(), ((Number)rhs).longValue());
  }

  private final ObjectColumnSelector selector;
  private final TimestampSpec timestampSpec;

  private long max;

  public TimestampMaxAggregator(ObjectColumnSelector selector, TimestampSpec timestampSpec)
  {
    this.selector = selector;
    this.timestampSpec = timestampSpec;

    reset();
  }

  @Override
  public void aggregate() {
    Object o = selector.get();
    if (o instanceof Number) {
      max = Math.max(max, ((Number)o).longValue());
    } else if (o instanceof DateTime) {
      max = Math.max(max, ((DateTime)o).getMillis());
    } else if (o instanceof Timestamp) {
      max = Math.max(max, ((Timestamp)o).getTime());
    } else if (o instanceof String) {
      max = Math.max(max, timestampSpec.parseDateTime(o).getMillis());
    }
  }

  @Override
  public void reset()
  {
    max = Long.MIN_VALUE;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return (float)max;
  }

  @Override
  public void close()
  {
    // no resource to cleanup
  }

  @Override
  public long getLong()
  {
    return max;
  }

  @Override
  public double getDouble() {
    return (double)max;
  }

  @Override
  public Aggregator clone()
  {
    return new TimestampMaxAggregator(selector, timestampSpec);
  }
}
