package io.druid.query.aggregation.area;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;

public class MetricArea
{
  // protected for test purpose
  protected double sum;
  protected float min;
  protected int count;

  public MetricArea(
      double sum,
      int count,
      float min
  )
  {
    this.sum = sum;
    this.count = count;
    this.min = min;
  }

  public MetricArea()
  {
    this(0, 0, Float.MAX_VALUE);
  }

  public MetricArea reset()
  {
    this.sum = 0;
    this.count = 0;
    this.min = Float.MAX_VALUE;

    return this;
  }

  public MetricArea add(Object o)
  {
    if (o instanceof Float) {
      return add((float)o);
    } else if (o instanceof MetricArea) {
      return add((MetricArea)o);
    }

    return this;
  }

  public MetricArea add(float value)
  {
    if (min > value)
    {
      min = value;
    }

    sum += value;
    count++;

    return this;
  }

  public MetricArea add(MetricArea other)
  {
    if (min > other.min)
    {
      min = other.min;
    }

    sum += other.sum;
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
    return ByteBuffer.allocate(Doubles.BYTES + Ints.BYTES + Floats.BYTES)
        .putDouble(sum)
        .putInt(count)
        .putFloat(min)
        .array();
  }

  public void fill(ByteBuffer buffer)
  {
    buffer.putDouble(sum);
    buffer.putInt(count);
    buffer.putFloat(min);
  }

  public static MetricArea fromBytes(ByteBuffer buffer)
  {
    return new MetricArea(buffer.getDouble(), buffer.getInt(), buffer.getFloat());
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
