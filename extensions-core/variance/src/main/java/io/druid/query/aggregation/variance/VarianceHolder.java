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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 *
 * Algorithm used here is copied from apache hive. This is description in GenericUDAFVariance
 *
 * Evaluate the variance using the algorithm described by Chan, Golub, and LeVeque in
 * "Algorithms for computing the sample variance: analysis and recommendations"
 * The American Statistician, 37 (1983) pp. 242--247.
 * <p/>
 * variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)
 * <p/>
 * where: - variance is sum[x-avg^2] (this is actually n times the variance)
 * and is updated at every step. - n is the count of elements in chunk1 - m is
 * the count of elements in chunk2 - t1 = sum of elements in chunk1, t2 =
 * sum of elements in chunk2.
 * <p/>
 * This algorithm was proven to be numerically stable by J.L. Barlow in
 * "Error analysis of a pairwise summation algorithm to compute sample variance"
 * Numer. Math, 58 (1991) pp. 583--590
 */
public class VarianceHolder
{
  public static VarianceHolder from(ByteBuffer buffer)
  {
    return new VarianceHolder(buffer.getLong(), buffer.getDouble(), buffer.getDouble());
  }

  public static final Comparator<VarianceHolder> COMPARATOR = new Comparator<VarianceHolder>()
  {
    @Override
    public int compare(VarianceHolder o1, VarianceHolder o2)
    {
      int compare = Longs.compare(o1.count, o2.count);
      if (compare == 0) {
        compare = Doubles.compare(o1.sum, o2.sum);
        if (compare == 0) {
          compare = Doubles.compare(o1.nvariance, o2.nvariance);
        }
      }
      return compare;
    }
  };

  static Object combineValues(Object lhs, Object rhs)
  {
    VarianceHolder holder1 = (VarianceHolder) lhs;
    VarianceHolder holder2 = (VarianceHolder) rhs;

    if (holder2.count == 0) {
      return holder1;
    }
    if (holder1.count == 0) {
      holder1.nvariance = holder2.nvariance;
      holder1.count = holder2.count;
      holder1.sum = holder2.sum;
      return holder1;
    }

    final double ratio = holder1.count / (double) holder2.count;
    final double t = holder1.sum / ratio - holder2.sum;

    holder1.nvariance += holder2.nvariance + (ratio / (holder1.count + holder2.count) * t * t);
    holder1.count += holder2.count;
    holder1.sum += holder2.sum;

    return holder1;
  }

  long count; // number of elements
  double sum; // sum of elements
  double nvariance; // sum[x-avg^2] (this is actually n times of the variance)

  public VarianceHolder()
  {
  }

  public void reset()
  {
    count = 0;
    sum = 0;
    nvariance = 0;
  }

  public VarianceHolder(long count, double sum, double nvariance)
  {
    this.count = count;
    this.sum = sum;
    this.nvariance = nvariance;
  }

  public VarianceHolder add(float v)
  {
    count++;
    sum += v;
    if (count > 1) {
      double t = count * v - sum;
      nvariance += (t * t) / ((double) count * (count - 1));
    }
    return this;
  }

  public double getVariance()
  {
    if (count == 0) {
      // in SQL standard, we should return null for zero elements. But druid there should not be such a case
      throw new IllegalStateException("should not be empty holder");
    } else if (count == 1) {
      return 0d;
    } else {
      return nvariance / count;
    }
  }

  @JsonValue
  public byte[] toByteArray()
  {
    final ByteBuffer buffer = toByteBuffer();
    byte[] theBytes = new byte[buffer.remaining()];
    buffer.get(theBytes);

    return theBytes;
  }

  public ByteBuffer toByteBuffer()
  {
    return ByteBuffer.allocate(Longs.BYTES + Doubles.BYTES + Doubles.BYTES)
                     .putLong(count)
                     .putDouble(sum)
                     .putDouble(nvariance);
  }

  @VisibleForTesting
  boolean equalsWithEpsilon(VarianceHolder o, double epsilon)
  {
    if (this == o) {
      return true;
    }

    if (count != o.count) {
      return false;
    }
    if (Math.abs(sum - o.sum) > epsilon) {
      return false;
    }
    if (Math.abs(nvariance - o.nvariance) > epsilon) {
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

    VarianceHolder that = (VarianceHolder) o;

    if (count != that.count) {
      return false;
    }
    if (Double.compare(that.sum, sum) != 0) {
      return false;
    }
    if (Double.compare(that.nvariance, nvariance) != 0) {
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
    temp = Double.doubleToLongBits(sum);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(nvariance);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "VarianceHolder{" +
           "count=" + count +
           ", sum=" + sum +
           ", nvariance=" + nvariance +
           '}';
  }
}
