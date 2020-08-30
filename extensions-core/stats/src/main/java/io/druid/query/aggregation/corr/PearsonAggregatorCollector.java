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

package io.druid.query.aggregation.corr;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 *
 * Algorithm used here is copied from apache hive. This is description in GenericUDAFCorrelation
 *
 * Compute the Pearson correlation coefficient corr(x, y), using the following
 * stable one-pass method, based on:
 * "Formulas for Robust, One-Pass Parallel Computation of Covariances and
 * Arbitrary-Order Statistical Moments", Philippe Pebay, Sandia Labs
 * and "The Art of Computer Programming, volume 2: Seminumerical Algorithms",
 * Donald Knuth.
 *
 *  Incremental:
 *   n : <count>
 *   mx_n = mx_(n-1) + [x_n - mx_(n-1)]/n : <xavg>
 *   my_n = my_(n-1) + [y_n - my_(n-1)]/n : <yavg>
 *   c_n = c_(n-1) + (x_n - mx_(n-1))*(y_n - my_n) : <covariance * n>
 *   vx_n = vx_(n-1) + (x_n - mx_n)(x_n - mx_(n-1)): <variance * n>
 *   vy_n = vy_(n-1) + (y_n - my_n)(y_n - my_(n-1)): <variance * n>
 *
 *  Merge:
 *   c_(A,B) = c_A + c_B + (mx_A - mx_B)*(my_A - my_B)*n_A*n_B/(n_A+n_B)
 *   vx_(A,B) = vx_A + vx_B + (mx_A - mx_B)*(mx_A - mx_B)*n_A*n_B/(n_A+n_B)
 *   vy_(A,B) = vy_A + vy_B + (my_A - my_B)*(my_A - my_B)*n_A*n_B/(n_A+n_B)
 *
 */
public class PearsonAggregatorCollector
{
  public static PearsonAggregatorCollector from(ByteBuffer buffer)
  {
    return new PearsonAggregatorCollector(
        buffer.getLong(),
        buffer.getDouble(),
        buffer.getDouble(),
        buffer.getDouble(),
        buffer.getDouble(),
        buffer.getDouble()
    );
  }

  public static final Comparator<PearsonAggregatorCollector> COMPARATOR = new Comparator<PearsonAggregatorCollector>()
  {
    @Override
    public int compare(PearsonAggregatorCollector o1, PearsonAggregatorCollector o2)
    {
      int compare = Longs.compare(o1.count, o2.count);
      if (compare == 0) {
        compare = Doubles.compare(o1.xavg, o2.xavg);
        if (compare == 0) {
          compare = Doubles.compare(o1.yavg, o2.yavg);
          if (compare == 0) {
            compare = Doubles.compare(o1.xvar, o2.xvar);
            if (compare == 0) {
              compare = Doubles.compare(o1.yvar, o2.yvar);
              if (compare == 0) {
                compare = Doubles.compare(o1.covar, o2.covar);
              }
            }
          }
        }
      }
      return compare;
    }
  };

  static PearsonAggregatorCollector combineValues(Object lhs, Object rhs)
  {
    final PearsonAggregatorCollector holder1 = (PearsonAggregatorCollector) lhs;
    final PearsonAggregatorCollector holder2 = (PearsonAggregatorCollector) rhs;

    if (holder2 == null || holder2.count == 0) {
      return holder1;
    }

    if (holder1.count == 0) {
      holder1.count = holder2.count;
      holder1.xavg = holder2.xavg;
      holder1.yavg = holder2.yavg;
      holder1.xvar = holder2.xvar;
      holder1.yvar = holder2.yvar;
      holder1.covar = holder2.covar;
      return holder1;
    }

    // Merge the two partials
    final double xavgA = holder1.xavg;
    final double yavgA = holder1.yavg;
    final double xavgB = holder2.xavg;
    final double yavgB = holder2.yavg;
    final double xvarB = holder2.xvar;
    final double yvarB = holder2.yvar;
    final double covarB = holder2.covar;

    final long nA = holder1.count;
    final long nB = holder2.count;
    final long nSum = nA + nB;

    holder1.count = nSum;
    holder1.xavg = (xavgA * nA + xavgB * nB) / nSum;
    holder1.yavg = (yavgA * nA + yavgB * nB) / nSum;
    holder1.xvar += xvarB + (xavgA - xavgB) * (xavgA - xavgB) * nA * nB / nSum;
    holder1.yvar += yvarB + (yavgA - yavgB) * (yavgA - yavgB) * nA * nB / nSum;
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
  double xvar; // n times the variance of x elements
  double yvar; // n times the variance of y elements
  double covar; // n times the covariance

  public PearsonAggregatorCollector()
  {
    this(0, 0, 0, 0, 0, 0);
  }

  public void reset()
  {
    count = 0;
    xavg = 0;
    yavg = 0;
    xvar = 0;
    yvar = 0;
    covar = 0;
  }

  public PearsonAggregatorCollector(long count, double xavg, double yavg, double xvar, double yvar, double covar)
  {
    this.count = count;
    this.xavg = xavg;
    this.yavg = yavg;
    this.xvar = xvar;
    this.yvar = yvar;
    this.covar = covar;
  }

  public synchronized PearsonAggregatorCollector add(final double vx, final double vy)
  {
    final double deltaX = vx - xavg;
    final double deltaY = vy - yavg;
    count++;
    xavg += deltaX / count;
    yavg += deltaY / count;
    if (count > 1) {
      covar += deltaX * (vy - yavg);
      xvar += deltaX * (vx - xavg);
      yvar += deltaY * (vy - yavg);
    }
    return this;
  }

  public Double getCorr()
  {
    // SQL standard - return null for zero or one pair
    return count < 2 ? null : covar / Math.sqrt(xvar) / Math.sqrt(yvar);
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
    return ByteBuffer.allocate(Longs.BYTES + Doubles.BYTES * 5)
                     .putLong(count)
                     .putDouble(xavg)
                     .putDouble(yavg)
                     .putDouble(xvar)
                     .putDouble(yvar)
                     .putDouble(covar);
  }

  @VisibleForTesting
  boolean equalsWithEpsilon(PearsonAggregatorCollector o, double epsilon)
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
    if (Math.abs(xvar - o.xvar) > epsilon) {
      return false;
    }
    if (Math.abs(yvar - o.yvar) > epsilon) {
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

    PearsonAggregatorCollector that = (PearsonAggregatorCollector) o;

    if (count != that.count) {
      return false;
    }
    if (Double.compare(xavg, that.xavg) != 0) {
      return false;
    }
    if (Double.compare(yavg, that.yavg) != 0) {
      return false;
    }
    if (Double.compare(xvar, that.xvar) != 0) {
      return false;
    }
    if (Double.compare(yvar, that.yvar) != 0) {
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
    return "PearsonHolder{" +
           "count=" + count +
           ", xavg=" + xavg +
           ", yavg=" + yavg +
           ", xvar=" + xvar +
           ", yvar=" + yvar +
           ", covar=" + covar +
           '}';
  }
}
