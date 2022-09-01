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
import io.druid.segment.FloatColumnSelector;
import org.apache.commons.lang.mutable.MutableFloat;

import java.util.Comparator;

/**
 */
public abstract class FloatMinAggregator implements Aggregator.FromMutableFloat
{
  static final Comparator COMPARATOR = FloatMaxAggregator.COMPARATOR;

  public static FloatMinAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    return new FloatMinAggregator()
    {
      private final MutableFloat handover = new MutableFloat();

      @Override
      public MutableFloat aggregate(final MutableFloat current)
      {
        if (predicate.matches() && selector.getFloat(handover)) {
          if (current == null) {
            return new MutableFloat(handover.floatValue());
          }
          current.setValue(Math.min(current.floatValue(), handover.floatValue()));
        }
        return current;
      }
    };
  }
}
