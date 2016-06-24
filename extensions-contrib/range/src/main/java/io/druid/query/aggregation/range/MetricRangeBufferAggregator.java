package io.druid.query.aggregation.range;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class MetricRangeBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;

  public MetricRangeBufferAggregator(
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
    mutationBuffer.putFloat(Float.MAX_VALUE);
    mutationBuffer.putFloat(Float.MIN_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    MetricRange metricRange = new MetricRange(mutationBuffer.getFloat(), mutationBuffer.getFloat());
    metricRange.add(selector.get());

    mutationBuffer.position(position);
    metricRange.fill(mutationBuffer);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return MetricRange.fromBytes(mutationBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return MetricRange.fromBytes(mutationBuffer).getRange();
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return MetricRange.fromBytes(mutationBuffer).getRange();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return (long)MetricRange.fromBytes(mutationBuffer).getRange();
  }

  @Override
  public void close()
  {

  }
}
