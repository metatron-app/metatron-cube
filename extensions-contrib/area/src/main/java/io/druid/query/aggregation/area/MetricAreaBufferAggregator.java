package io.druid.query.aggregation.area;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class MetricAreaBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;

  public MetricAreaBufferAggregator(
      ObjectColumnSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.putDouble(0);
    mutationBuffer.putInt(0);
    mutationBuffer.putDouble(Double.POSITIVE_INFINITY);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    MetricArea metricArea = MetricArea.fromBytes(mutationBuffer);
    metricArea.add(selector.get());

    mutationBuffer.position(position);
    metricArea.fill(mutationBuffer);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return MetricArea.fromBytes(mutationBuffer);
  }

  @Override
  public Float getFloat(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return (float)MetricArea.fromBytes(mutationBuffer).getArea();
  }

  @Override
  public Double getDouble(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return MetricArea.fromBytes(mutationBuffer).getArea();
  }

  @Override
  public Long getLong(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return (long)MetricArea.fromBytes(mutationBuffer).getArea();
  }

  @Override
  public void close()
  {
  }
}
