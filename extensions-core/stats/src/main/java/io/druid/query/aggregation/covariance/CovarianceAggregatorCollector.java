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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 *
 * Algorithm used here is copied from apache hive. This is description in GenericUDAFCovariance
 *
 * Compute the covariance covar_pop(x, y), using the following one-pass method
 * (ref. "Formulas for Robust, One-Pass Parallel Computation of Covariances and
 *  Arbitrary-Order Statistical Moments", Philippe Pebay, Sandia Labs):
 *
 *  Incremental:
 *   n : <count>
 *   mx_n = mx_(n-1) + [x_n - mx_(n-1)]/n : <xavg>
 *   my_n = my_(n-1) + [y_n - my_(n-1)]/n : <yavg>
 *   c_n = c_(n-1) + (x_n - mx_(n-1))*(y_n - my_n) : <covariance * n>
 *
 *  Merge:
 *   c_X = c_A + c_B + (mx_A - mx_B)*(my_A - my_B)*n_A*n_B/n_X
 *
 */
public class CovarianceAggregatorCollector
{
  public static CovarianceAggregatorCollector from(ByteBuffer buffer)
  {
    return new CovarianceAggregatorCollector(
        buffer.getLong(),
        buffer.getDouble(),
        buffer.getDouble(),
        buffer.getDouble()
    );
  }

  public static final Comparator<CovarianceAggregatorCollector> COMPARATOR = new Comparator<CovarianceAggregatorCollector>()
  {
    @Override
    public int compare(CovarianceAggregatorCollector o1, CovarianceAggregatorCollector o2)
    {
      int compare = Longs.compare(o1.count, o2.count);
      if (compare == 0) {
        compare = Doubles.compare(o1.xavg, o2.xavg);
        if (compare == 0) {
          compare = Doubles.compare(o1.yavg, o2.yavg);
          if (compare == 0) {
            compare = Doubles.compare(o1.covar, o2.covar);
          }
        }
      }
      return compare;
    }
  };

  static CovarianceAggregatorCollector combineValues(Object lhs, Object rhs)
  {
    final CovarianceAggregatorCollector holder1 = (CovarianceAggregatorCollector) lhs;
    final CovarianceAggregatorCollector holder2 = (CovarianceAggregatorCollector) rhs;

    if (holder2 == null || holder2.count == 0) {
      return holder1;
    }

    if (holder1.count == 0) {
      holder1.count = holder2.count;
      holder1.xavg = holder2.xavg;
      holder1.yavg = holder2.yavg;
      holder1.covar = holder2.covar;
      return holder1;
    }

    // Merge the two partials
    final double xavgA = holder1.xavg;
    final double yavgA = holder1.yavg;
    final double xavgB = holder2.xavg;
    final double yavgB = holder2.yavg;
    final double covarB = holder2.covar;

    final long nA = holder1.count;
    final long nB = holder2.count;
    final long nSum = nA + nB;

    holder1.count = nSum;
    holder1.xavg = (xavgA * nA + xavgB * nB) / nSum;
    holder1.yavg = (yavgA * nA + yavgB * nB) / nSum;
    holder1.covar += covarB + (xavgA - xavgB) * (yavgA - yavgB) * ((double) (nA * nB) / nSum);

    return holder1;
  }

  static int getMaxIntermediateSize()
  {
    return Longs.BYTES + Doubles.BYTES + Doubles.BYTES + Doubles.BYTES + Doubles.BYTES + Doubles.BYTES;
  }

  long count; // number n of elements
  double xavg; // average of x elements
  double yavg; // average of y elements
  double covar; // n times the covariance

  public CovarianceAggregatorCollector()
  {
    this(0, 0, 0, 0);
  }

  public void reset()
  {
    count = 0;
    xavg = 0;
    yavg = 0;
    covar = 0;
  }

  public CovarianceAggregatorCollector(long count, double xavg, double yavg, double covar)
  {
    this.count = count;
    this.xavg = xavg;
    this.yavg = yavg;
    this.covar = covar;
  }

  public synchronized CovarianceAggregatorCollector add(final double vx, final double vy)
  {
    final double deltaX = vx - xavg;
    final double deltaY = vy - yavg;
    count++;
    yavg = yavg + deltaY / count;
    if (count > 1) {
      covar += deltaX * deltaY;
    }
    xavg = xavg + deltaX / count;
    return this;
  }

  public double getCorr()
  {
    if (count == 0) {
      // in SQL standard, we should return null for zero elements. But druid there should not be such a case
      return Double.NaN;
    } else {
      return covar / count;
    }
  }

  @JsonValue
  public byte[] toByteArray()
  {
    final ByteBuffer buffer = toByteBuffer();
    buffer.flip();
    byte[] theBytes = new byte[buffer.remaining()];
    buffer.get(theBytes);

    return theBytes;
  }

  public ByteBuffer toByteBuffer()
  {
    return ByteBuffer.allocate(Longs.BYTES + Doubles.BYTES * 3)
                     .putLong(count)
                     .putDouble(xavg)
                     .putDouble(yavg)
                     .putDouble(covar);
  }

  public CovarianceAggregatorCollector duplicate()
  {
    return new CovarianceAggregatorCollector(count, xavg, yavg, covar);
  }

  @VisibleForTesting
  boolean equalsWithEpsilon(CovarianceAggregatorCollector o, double epsilon)
  {
    if (this == o) {
      return true;
    }

    if (count != o.count) {
      return false;
    }
    if (Math.abs(xavg - o.xavg) > epsilon) {
      return false;
    }
    if (Math.abs(yavg - o.yavg) > epsilon) {
      return false;
    }
    if (Math.abs(covar - o.covar) > epsilon) {
      return false;
    }

    return true;
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

    CovarianceAggregatorCollector that = (CovarianceAggregatorCollector) o;

    if (count != that.count) {
      return false;
    }
    if (Double.compare(xavg, that.xavg) != 0) {
      return false;
    }
    if (Double.compare(yavg, that.yavg) != 0) {
      return false;
    }
    if (Double.compare(covar, that.covar) != 0) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    result = (int) (count ^ (count >>> 32));
    temp = Double.doubleToLongBits(xavg);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(yavg);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "CovarianceHolder{" +
           "count=" + count +
           ", xavg=" + xavg +
           ", yavg=" + yavg +
           ", covar=" + covar +
           '}';
  }
}
