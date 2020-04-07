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
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Comparator;

public class TimestampMaxAggregator implements Aggregator<MutableLong>
{
  static final Comparator COMPARATOR = LongMaxAggregator.COMPARATOR;

  static long combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  private final ObjectColumnSelector selector;
  private final DefaultTimestampSpec timestampSpec;

  public TimestampMaxAggregator(ObjectColumnSelector selector, DefaultTimestampSpec timestampSpec)
  {
    this.selector = selector;
    this.timestampSpec = timestampSpec;
  }

  @Override
  public Long get(MutableLong current)
  {
    return current == null ? null : current.longValue();
  }

  @Override
  public MutableLong aggregate(MutableLong current)
  {
    final Object o = selector.get();
    if (o == null) {
      return current;
    }
    final long value;
    if (o instanceof Number) {
      value = ((Number) o).longValue();
    } else if (o instanceof DateTime) {
      value = ((DateTime) o).getMillis();
    } else if (o instanceof Timestamp) {
      value = ((Timestamp) o).getTime();
    } else if (o instanceof String) {
      value = timestampSpec.parseDateTime(o).getMillis();
    } else {
      return current;
    }
    if (current == null) {
      current = new MutableLong(value);
    } else {
      current.setValue(Math.max(current.longValue(), value));
    }
    return current;
  }
}
