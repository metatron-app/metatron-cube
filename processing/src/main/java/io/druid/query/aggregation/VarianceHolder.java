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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
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

    final double ratio = holder1.count / (double) holder2.count;
    final double t = ratio * holder1.sum - holder2.sum;

    holder1.nvariance += holder2.nvariance + (ratio / (holder1.count + holder2.count) * t * t);
    holder1.count += holder2.count;
    holder1.sum += holder2.sum;

    return holder1;
  }

  long count; // number of elements
  double sum; // sum of elements
  double nvariance; // sum[x-avg^2] (this is actually n times the nvariance)

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

  public VarianceHolder add(VarianceHolder another)
  {
    combineValues(this, another);
    return this;
  }

  public Double getVariance()
  {
    if (count == 0) { // SQL standard - return null for zero elements
      return null;
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
    return ByteBuffer.allocate(24)
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
