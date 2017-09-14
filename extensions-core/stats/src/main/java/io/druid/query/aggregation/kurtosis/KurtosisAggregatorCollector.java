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

package io.druid.query.aggregation.kurtosis;

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
 * Compute the Kurtosis correlation coefficient corr(x, y), using the following
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
class KurtosisAggregatorCollector
{
  public static KurtosisAggregatorCollector from(ByteBuffer buffer)
  {
    return new KurtosisAggregatorCollector(
        buffer.getLong(),
        buffer.getDouble(),
        buffer.getDouble(),
        buffer.getDouble(),
        buffer.getDouble()
    );
  }

  public static final Comparator<KurtosisAggregatorCollector> COMPARATOR = new Comparator<KurtosisAggregatorCollector>()
  {
    @Override
    public int compare(KurtosisAggregatorCollector o1, KurtosisAggregatorCollector o2)
    {
      int compare = Longs.compare(o1.n, o2.n);
      if (compare == 0) {
        compare = Doubles.compare(o1.mean, o2.mean);
        if (compare == 0) {
          compare = Doubles.compare(o1.M2, o2.M2);
          if (compare == 0) {
            compare = Doubles.compare(o1.M3, o2.M3);
            if (compare == 0) {
              compare = Doubles.compare(o1.M4, o2.M4);
            }
          }
        }
      }
      return compare;
    }
  };

  static KurtosisAggregatorCollector combineValues(Object lhs, Object rhs)
  {
    final KurtosisAggregatorCollector holder1 = (KurtosisAggregatorCollector) lhs;
    final KurtosisAggregatorCollector holder2 = (KurtosisAggregatorCollector) rhs;

    if (holder2 == null || holder2.n == 0) {
      return holder1;
    }

    final long n1 = holder1.n;
    final long n2 = holder2.n;

    final double mean1 = holder1.mean;
    final double mean2 = holder2.mean;

    final long n = n1 + n2;
    final double delta = mean2 - mean1;
    final double deltaN = n == 0 ? 0 : delta / n;

    final double mean = mean1 + deltaN * n2;

    final double M2_1 = holder1.M2;
    final double M2_2 = holder2.M2;

    double M2 = M2_1 + M2_2 + delta * deltaN * n1 * n2;

    final double M3_1 = holder1.M3;
    final double M3_2 = holder2.M3;

    final double M3 = M3_1 + M3_2 + deltaN * deltaN * delta * n1 * n2 * (n1 - n2) +
                      3.0 * deltaN * (n1 * M2_2 - n2 * M2_1);

    final double M4_1 = holder1.M4;
    final double M4_2 = holder2.M4;

    final double M4 = M4_1 + M4_2 + deltaN * deltaN * deltaN * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2) +
                      deltaN * deltaN * 6.0 * (n1 * n1 * M2_2 + n2 * n2 * M2_1) +
                      4.0 * deltaN * (n1 * M3_2 - n2 * M3_1);

    holder1.n = n;
    holder1.mean = mean;
    holder1.M2 = M2;
    holder1.M3 = M3;
    holder1.M4 = M4;

    return holder1;
  }

  static int getMaxIntermediateSize()
  {
    return Longs.BYTES + Doubles.BYTES + Doubles.BYTES + Doubles.BYTES + Doubles.BYTES;
  }

  long n;
  double mean;
  double M2;
  double M3;
  double M4;

  public KurtosisAggregatorCollector()
  {
    this(0, 0, 0, 0, 0);
  }

  public void reset()
  {
    n = 0;
    mean = 0;
    M2 = 0;
    M3 = 0;
    M4 = 0;
  }

  public KurtosisAggregatorCollector(long n, double mean, double M2, double M3, double M4)
  {
    this.n = n;
    this.mean = mean;
    this.M2 = M2;
    this.M3 = M3;
    this.M4 = M4;
  }

  public synchronized KurtosisAggregatorCollector add(final double x)
  {
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
    return this;
  }

  public double getKurtosis()
  {
    if (n < 4) {
      return Double.NaN;
    } else {
      return (n * M4) / (M2 * M2) - 3;
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
    return ByteBuffer.allocate(Longs.BYTES + Doubles.BYTES * 4)
                     .putLong(n)
                     .putDouble(mean)
                     .putDouble(M2)
                     .putDouble(M3)
                     .putDouble(M4);
  }

  @VisibleForTesting
  boolean equalsWithEpsilon(KurtosisAggregatorCollector o, double epsilon)
  {
    if (this == o) {
      return true;
    }

    if (n != o.n) {
      return false;
    }
    if (Math.abs(mean - o.mean) > epsilon) {
      return false;
    }
    if (Math.abs(M2 - o.M2) > epsilon) {
      return false;
    }
    if (Math.abs(M3 - o.M3) > epsilon) {
      return false;
    }
    if (Math.abs(M4 - o.M4) > epsilon) {
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

    KurtosisAggregatorCollector that = (KurtosisAggregatorCollector) o;

    if (n != that.n) {
      return false;
    }
    if (Double.compare(mean, that.mean) != 0) {
      return false;
    }
    if (Double.compare(M2, that.M2) != 0) {
      return false;
    }
    if (Double.compare(M3, that.M3) != 0) {
      return false;
    }
    if (Double.compare(M4, that.M4) != 0) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    result = (int) (n ^ (n >>> 32));
    temp = Double.doubleToLongBits(mean);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(M2);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "KurtosisHolder{" +
           "count=" + n +
           ", mean=" + mean +
           ", M2=" + M2 +
           ", M3=" + M3 +
           ", M4=" + M4 +
           '}';
  }
}
