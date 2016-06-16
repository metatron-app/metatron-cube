package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.primitives.Floats;

import java.nio.ByteBuffer;

public class MetricRange
{
  private float min;
  private float max;

  public MetricRange(
      float min,
      float max
  )
  {
    this.min = min;
    this.max = max;
  }

  public MetricRange()
  {
    this(Float.MAX_VALUE, Float.MIN_VALUE);
  }

  public MetricRange reset()
  {
    max = Float.MAX_VALUE;
    min = Float.MIN_VALUE;

    return this;
  }

  public MetricRange add(Object o)
  {
    if (o instanceof Float) {
      return add((float)o);
    } else if (o instanceof MetricRange) {
      return add((MetricRange)o);
    }

    return this;
  }

  public MetricRange add(float value)
  {
    if (min > value) {
      min = value;
    }
    if (max < value) {
      max = value;
    }

    return this;
  }

  public MetricRange add(MetricRange other)
  {
    if (min > other.min) {
      min = other.min;
    }
    if (max < other.max) {
      max = other.max;
    }

    return this;
  }

  public float getRange()
  {
    if (min == Float.MAX_VALUE) {
      return 0;
    }
    return max - min;
  }

  @JsonValue
  public byte[] toBytes()
  {
    return ByteBuffer.allocate(Floats.BYTES * 2)
        .putFloat(min)
        .putFloat(max)
        .array();
  }

  public void fill(ByteBuffer buffer)
  {
    buffer.putFloat(min);
    buffer.putFloat(max);
  }

  public static MetricRange fromBytes(ByteBuffer buffer)
  {
    return new MetricRange(buffer.getFloat(), buffer.getFloat());
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
