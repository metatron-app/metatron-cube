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

package io.druid.query.aggregation.variance;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class VarianceBufferAggregator implements BufferAggregator
{
  private static final int COUNT_OFFSET = 0;
  private static final int SUM_OFFSET = Longs.BYTES;
  private static final int NVARIANCE_OFFSET = SUM_OFFSET + Doubles.BYTES;

  protected final String name;

  public VarianceBufferAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position + COUNT_OFFSET, 0)
       .putDouble(position + SUM_OFFSET, 0)
       .putDouble(position + NVARIANCE_OFFSET, 0);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    VarianceAggregatorCollector holder = new VarianceAggregatorCollector();
    holder.count = buf.getLong(position);
    holder.sum = buf.getDouble(position + SUM_OFFSET);
    holder.nvariance = buf.getDouble(position + NVARIANCE_OFFSET);
    return holder;
  }

  static BufferAggregator create(String name, final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new VarianceBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final Float v = selector.get();
          if (v != null) {
            long count = buf.getLong(position + COUNT_OFFSET) + 1;
            double sum = buf.getDouble(position + SUM_OFFSET) + v;
            buf.putLong(position, count);
            buf.putDouble(position + SUM_OFFSET, sum);
            if (count > 1) {
              double t = count * v - sum;
              double variance = buf.getDouble(position + NVARIANCE_OFFSET) + (t * t) / ((double) count * (count - 1));
              buf.putDouble(position + NVARIANCE_OFFSET, variance);
            }
          }
        }
      }
    };
  }

  static BufferAggregator create(String name, final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new VarianceBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final Double v = selector.get();
          if (v == null) {
            return;
          }
          long count = buf.getLong(position + COUNT_OFFSET) + 1;
          double sum = buf.getDouble(position + SUM_OFFSET) + v;
          buf.putLong(position, count);
          buf.putDouble(position + SUM_OFFSET, sum);
          if (count > 1) {
            double t = count * v - sum;
            double variance = buf.getDouble(position + NVARIANCE_OFFSET) + (t * t) / ((double) count * (count - 1));
            buf.putDouble(position + NVARIANCE_OFFSET, variance);
          }
        }
      }
    };
  }

  static BufferAggregator create(String name, final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new VarianceBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final Long v = selector.get();
          if (v != null) {
            long count = buf.getLong(position + COUNT_OFFSET) + 1;
            double sum = buf.getDouble(position + SUM_OFFSET) + v;
            buf.putLong(position, count);
            buf.putDouble(position + SUM_OFFSET, sum);
            if (count > 1) {
              double t = count * v - sum;
              double variance = buf.getDouble(position + NVARIANCE_OFFSET) + (t * t) / ((double) count * (count - 1));
              buf.putDouble(position + NVARIANCE_OFFSET, variance);
            }
          }
        }
      }
    };
  }

  static BufferAggregator create(String name, final ObjectColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new VarianceBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          Object input = selector.get();
          if (!(input instanceof VarianceAggregatorCollector)) {
            return;
          }
          VarianceAggregatorCollector holder2 = (VarianceAggregatorCollector) input;
          if (holder2.count == 0) {
            return;
          }

          long count = buf.getLong(position + COUNT_OFFSET);
          if (count == 0) {
            buf.putLong(position, holder2.count);
            buf.putDouble(position + SUM_OFFSET, holder2.sum);
            buf.putDouble(position + NVARIANCE_OFFSET, holder2.nvariance);
            return;
          }

          double sum = buf.getDouble(position + SUM_OFFSET);
          double nvariance = buf.getDouble(position + NVARIANCE_OFFSET);

          final double ratio = count / (double) holder2.count;
          final double t = sum / ratio - holder2.sum;

          nvariance += holder2.nvariance + (ratio / (count + holder2.count) * t * t);
          count += holder2.count;
          sum += holder2.sum;

          buf.putLong(position, count);
          buf.putDouble(position + SUM_OFFSET, sum);
          buf.putDouble(position + NVARIANCE_OFFSET, nvariance);
        }
      }
    };
  }
}
