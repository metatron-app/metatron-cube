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

package io.druid.query.aggregation.kurtosis;

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class KurtosisBufferAggregator implements BufferAggregator
{
  private static final int COUNT_OFFSET = 0;
  private static final int MEAN_OFFSET = Longs.BYTES;
  private static final int M2_OFFSET = MEAN_OFFSET + Double.BYTES;
  private static final int M3_OFFSET = M2_OFFSET + Double.BYTES;
  private static final int M4_OFFSET = M3_OFFSET + Double.BYTES;

  protected final String name;

  public KurtosisBufferAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position + COUNT_OFFSET, 0)
       .putDouble(position + MEAN_OFFSET, 0)
       .putDouble(position + M2_OFFSET, 0)
       .putDouble(position + M3_OFFSET, 0)
       .putDouble(position + M4_OFFSET, 0);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    KurtosisAggregatorCollector holder = new KurtosisAggregatorCollector();
    holder.n = buf.getLong(position + COUNT_OFFSET);
    holder.mean = buf.getDouble(position + MEAN_OFFSET);
    holder.M2 = buf.getDouble(position + M2_OFFSET);
    holder.M3 = buf.getDouble(position + M3_OFFSET);
    holder.M4 = buf.getDouble(position + M4_OFFSET);
    return holder;
  }

  @Override
  public Float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("KurtosisBufferAggregator does not support getFloat()");
  }

  @Override
  public Double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("KurtosisBufferAggregator does not support getDouble()");
  }

  @Override
  public Long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("KurtosisBufferAggregator does not support getFloat()");
  }

  @Override
  public void close()
  {
  }

  static BufferAggregator create(
      String name,
      final DoubleColumnSelector selector,
      final ValueMatcher predicate
  )
  {
    if (selector == null) {
      return Aggregators.noopBufferAggregator();
    }
    return new KurtosisBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final Double v = selector.get();
          if (v == null) {
            return;
          }
          long n = buf.getLong(position + COUNT_OFFSET);
          double mean = buf.getDouble(position + MEAN_OFFSET);
          double M2 = buf.getDouble(position + M2_OFFSET);
          double M3 = buf.getDouble(position + M3_OFFSET);
          double M4 = buf.getDouble(position + M4_OFFSET);

          final double x = v;
          final long n1 = n;
          n = n + 1;
          final double delta = x - mean;
          final double delta_n = delta / n;
          final double delta_n2 = delta_n * delta_n;
          final double term1 = delta * delta_n * n1;

          mean = mean + delta_n;
          M4 += term1 * delta_n2 * (n * n - 3 * n + 3) + 6 * delta_n2 * M2 - 4 * delta_n * M3;
          M3 += term1 * delta_n * (n - 2) - 3 * delta_n * M2;
          M2 += term1;

          buf.putLong(position + COUNT_OFFSET, n)
             .putDouble(position + MEAN_OFFSET, mean)
             .putDouble(position + M2_OFFSET, M2)
             .putDouble(position + M3_OFFSET, M3)
             .putDouble(position + M4_OFFSET, M4);
        }
      }
    };
  }

  static BufferAggregator create(final String name, final ObjectColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopBufferAggregator();
    }
    return new KurtosisBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final KurtosisAggregatorCollector holder = (KurtosisAggregatorCollector) selector.get();
          if (holder == null || holder.n == 0) {
            return;
          }

          final long n1 = buf.getLong(position + COUNT_OFFSET);
          final long n2 = holder.n;

          if (n1 == 0) {
            buf.putLong(position + COUNT_OFFSET, holder.n)
               .putDouble(position + MEAN_OFFSET, holder.mean)
               .putDouble(position + M2_OFFSET, holder.M2)
               .putDouble(position + M3_OFFSET, holder.M3)
               .putDouble(position + M4_OFFSET, holder.M4);
          }

          final double mean1 = buf.getDouble(position + MEAN_OFFSET);
          final double mean2 = holder.mean;

          final long n = n1 + n2;
          final double delta = mean2 - mean1;
          final double deltaN = n == 0 ? 0 : delta / n;

          final double mean = mean1 + deltaN * n2;

          final double M2_1 = buf.getDouble(position + M2_OFFSET);
          final double M2_2 = holder.M2;

          double M2 = M2_1 + M2_2 + delta * deltaN * n1 * n2;

          final double M3_1 = buf.getDouble(position + M3_OFFSET);
          final double M3_2 = holder.M3;

          final double M3 = M3_1 + M3_2 + deltaN * deltaN * delta * n1 * n2 * (n1 - n2) +
                            3.0 * deltaN * (n1 * M2_2 - n2 * M2_1);

          final double M4_1 = buf.getDouble(position + M4_OFFSET);
          final double M4_2 = holder.M4;

          final double M4 = M4_1 + M4_2 + deltaN * deltaN * deltaN * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2) +
                            deltaN * deltaN * 6.0 * (n1 * n1 * M2_2 + n2 * n2 * M2_1) +
                            4.0 * deltaN * (n1 * M3_2 - n2 * M3_1);

          buf.putLong(position + COUNT_OFFSET, n)
               .putDouble(position + MEAN_OFFSET, mean)
               .putDouble(position + M2_OFFSET, M2)
               .putDouble(position + M3_OFFSET, M3)
               .putDouble(position + M4_OFFSET, M4);
        }
      }
    };
  }
}
