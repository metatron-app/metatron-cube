/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query.aggregation.area;

import com.fasterxml.jackson.annotation.JsonValue;

import java.nio.ByteBuffer;

public class MetricArea
{
  // protected for test purpose
  protected double sum;
  protected double min;
  volatile double c;
  protected int count;

  public MetricArea(
      double sum,
      int count,
      double min
  )
  {
    this.sum = sum;
    this.count = count;
    this.min = min;
    this.c = 0;
  }

  public MetricArea()
  {
    this(0, 0, Double.POSITIVE_INFINITY);
  }

  public MetricArea reset()
  {
    this.sum = 0;
    this.count = 0;
    this.c = 0;
    this.min = Double.POSITIVE_INFINITY;

    return this;
  }

  public synchronized MetricArea add(Object o)
  {
    if (o instanceof Double) {
      return add((double)o);
    } else if (o instanceof MetricArea) {
      return add((MetricArea)o);
    }

    return this;
  }

  public synchronized MetricArea add(double value)
  {
    if (min > value)
    {
      min = value;
    }

    sum = correctedAdd(sum, value);
    count++;

    return this;
  }

  private double correctedAdd(double sum, double value)
  {
    double cValue = value - c;
    double t = sum + cValue;
    c = (t - sum) - cValue;
    return t;
  }

  public MetricArea add(MetricArea other)
  {
    if (min > other.min)
    {
      min = other.min;
    }

    sum = correctedAdd(sum, other.sum);
    count += other.count;

    return this;
  }

  public double getArea()
  {
    if (count == 0) {
      return 0;
    }

    return sum - min * count;
  }

  @JsonValue
  public byte[] toBytes()
  {
    return ByteBuffer.allocate(Double.BYTES + Integer.BYTES + Double.BYTES)
        .putDouble(sum)
        .putInt(count)
        .putDouble(min)
        .array();
  }

  public void fill(ByteBuffer buffer)
  {
    buffer.putDouble(sum);
    buffer.putInt(count);
    buffer.putDouble(min);
  }

  public static MetricArea fromBytes(ByteBuffer buffer)
  {
    return new MetricArea(buffer.getDouble(), buffer.getInt(), buffer.getDouble());
  }

  public static MetricArea fromBytes(byte[] bytes)
  {
    return fromBytes(ByteBuffer.wrap(bytes));
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof MetricArea)
    {
      MetricArea other = (MetricArea) o;

      if (sum != other.sum) {
        return false;
      }
      if (count != other.count) {
        return false;
      }
      if (min != other.min) {
        return false;
      }

      return true;
    }

    return false;
  }

  @Override
  public String toString()
  {
    return "MetricArea{" +
        "sum=" + sum +
        ",count=" + count +
        ",min=" + min +
        ",area=" + getArea() +
        "}";
  }
}
