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

package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonValue;

import java.nio.ByteBuffer;

public class MetricRange
{
  private double min;
  private double max;

  public MetricRange(
      double min,
      double max
  )
  {
    this.min = min;
    this.max = max;
  }

  public MetricRange()
  {
    this(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
  }

  public MetricRange reset()
  {
    min = Double.POSITIVE_INFINITY;
    max = Double.NEGATIVE_INFINITY;

    return this;
  }

  public synchronized MetricRange add(Object o)
  {
    if (o instanceof Double) {
      return add((double)o);
    } else if (o instanceof MetricRange) {
      return add((MetricRange)o);
    }

    return this;
  }

  public synchronized MetricRange add(double value)
  {
    if (min > value) {
      min = value;
    }
    if (max < value) {
      max = value;
    }

    return this;
  }

  public synchronized MetricRange add(MetricRange other)
  {
    if (min > other.min) {
      min = other.min;
    }
    if (max < other.max) {
      max = other.max;
    }

    return this;
  }

  public double getRange()
  {
    if (min == Double.POSITIVE_INFINITY) {
      return 0;
    }
    return max - min;
  }

  @JsonValue
  public byte[] toBytes()
  {
    return ByteBuffer.allocate(Double.BYTES * 2)
        .putDouble(min)
        .putDouble(max)
        .array();
  }

  public void fill(ByteBuffer buffer)
  {
    buffer.putDouble(min);
    buffer.putDouble(max);
  }

  public static MetricRange fromBytes(ByteBuffer buffer)
  {
    return new MetricRange(buffer.getDouble(), buffer.getDouble());
  }

  public static MetricRange fromBytes(byte[] bytes)
  {
    return fromBytes(ByteBuffer.wrap(bytes));
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof MetricRange)
    {
      MetricRange other = (MetricRange)o;

      if (min != other.min) {
        return false;
      }
      if (max != other.max) {
        return false;
      }

      return true;
    }

    return false;
  }

  @Override
  public String toString()
  {
    return "MetricRange{" +
        "min=" + min +
        ",max=" + max +
        ",range=" + getRange() +
        "}";
  }
}
