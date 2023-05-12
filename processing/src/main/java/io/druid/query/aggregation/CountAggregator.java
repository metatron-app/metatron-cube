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

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.bitmap.IntIterators;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.util.Comparator;

/**
 */
public class CountAggregator implements Aggregator.FromMutableLong, Aggregator.LongScannable
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static Object combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  private final ValueMatcher predicate;

  public CountAggregator(ValueMatcher predicate)
  {
    this.predicate = predicate;
  }

  public CountAggregator()
  {
    this(ValueMatcher.TRUE);
  }

  @Override
  public Long get(final MutableLong current)
  {
    return current == null ? 0L : current.longValue();
  }

  @Override
  public boolean getLong(MutableLong current, MutableLong handover)
  {
    handover.setValue(current == null ? 0L : current.longValue());
    return true;
  }

  @Override
  public MutableLong aggregate(MutableLong current)
  {
    if (predicate.matches()) {
      if (current == null) {
        current = new MutableLong(1);
      } else {
        current.add(1);
      }
    }
    return current;
  }

  @Override
  public boolean supports()
  {
    return predicate == ValueMatcher.TRUE;
  }

  @Override
  public Object aggregate(IntIterator iterator)
  {
    return (long) IntIterators.count(iterator);
  }
}
