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

package io.druid.query.aggregation.covariance;

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class CovarianceBufferAggregator implements BufferAggregator
{
  private static final int COUNT_OFFSET = 0;
  private static final int XAVG_OFFSET = Longs.BYTES;
  private static final int YAVG_OFFSET = XAVG_OFFSET + Double.BYTES;
  private static final int COVAR_OFFSET = YAVG_OFFSET + Double.BYTES;

  protected final String name;

  public CovarianceBufferAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position + COUNT_OFFSET, 0)
       .putDouble(position + XAVG_OFFSET, 0)
       .putDouble(position + YAVG_OFFSET, 0)
       .putDouble(position + COVAR_OFFSET, 0);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    CovarianceAggregatorCollector holder = new CovarianceAggregatorCollector();
    holder.count = buf.getLong(position + COUNT_OFFSET);
    holder.xavg = buf.getDouble(position + XAVG_OFFSET);
    holder.yavg = buf.getDouble(position + YAVG_OFFSET);
    holder.covar = buf.getDouble(position + COVAR_OFFSET);
    return holder;
  }

  @Override
  public Float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CovarianceBufferAggregator does not support getFloat()");
  }

  @Override
  public Double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CovarianceBufferAggregator does not support getDouble()");
  }

  @Override
  public Long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CovarianceBufferAggregator does not support getFloat()");
  }

  @Override
  public void close()
  {
  }

  static BufferAggregator create(
      String name,
      final DoubleColumnSelector selector1,
      final DoubleColumnSelector selector2,
      final ValueMatcher predicate
  )
  {
    if (selector1 == null || selector2 == null) {
      return Aggregators.noopBufferAggregator();
    }
    return new CovarianceBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final Double v1 = selector1.get();
          final Double v2 = selector2.get();
          if (v1 == null || v2 == null) {
            return;
          }
          long count = buf.getLong(position + COUNT_OFFSET);
          double xavg = buf.getDouble(position + XAVG_OFFSET);
          double yavg = buf.getDouble(position + YAVG_OFFSET);
          double covar = buf.getDouble(position + COVAR_OFFSET);

          final double vx = v1;
          final double vy = v2;
          final double deltaX = vx - xavg;
          final double deltaY = vy - yavg;
          count++;
          xavg += deltaX / count;
          yavg += deltaY / count;
          if (count > 1) {
            covar += deltaX * (vy - yavg);
          }

          buf.putLong(position + COUNT_OFFSET, count)
             .putDouble(position + XAVG_OFFSET, xavg)
             .putDouble(position + YAVG_OFFSET, yavg)
             .putDouble(position + COVAR_OFFSET, covar);
        }
      }
    };
  }

  static BufferAggregator create(final String name, final ObjectColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopBufferAggregator();
    }
    return new CovarianceBufferAggregator(name)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final CovarianceAggregatorCollector holder = (CovarianceAggregatorCollector) selector.get();
          if (holder == null || holder.count == 0) {
            return;
          }

          final long nA = buf.getLong(position + COUNT_OFFSET);
          if (nA == 0) {
            buf.putLong(position + COUNT_OFFSET, holder.count)
               .putDouble(position + XAVG_OFFSET, holder.xavg)
               .putDouble(position + YAVG_OFFSET, holder.yavg)
               .putDouble(position + COVAR_OFFSET, holder.covar);
          } else {
            // Merge the two partials
            double xavgA = buf.getDouble(position + XAVG_OFFSET);
            double yavgA = buf.getDouble(position + YAVG_OFFSET);
            double covarA = buf.getDouble(position + COVAR_OFFSET);

            final double xavgB = holder.xavg;
            final double yavgB = holder.yavg;
            final double covarB = holder.covar;

            final long nB = holder.count;
            final long nSum = nA + nB;

            buf.putLong(position + COUNT_OFFSET, nSum)
               .putDouble(position + XAVG_OFFSET, (xavgA * nA + xavgB * nB) / nSum)
               .putDouble(position + XAVG_OFFSET, (yavgA * nA + yavgB * nB) / nSum)
               .putDouble(position + XAVG_OFFSET,
                          covarA + covarB + (xavgA - xavgB) * (yavgA - yavgB) * ((double) (nA * nB) / nSum)
               );
          }
        }
      }
    };
  }
}
