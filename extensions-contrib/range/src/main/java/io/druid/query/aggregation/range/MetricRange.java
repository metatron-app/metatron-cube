package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.primitives.Doubles;

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
    return ByteBuffer.allocate(Doubles.BYTES * 2)
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
