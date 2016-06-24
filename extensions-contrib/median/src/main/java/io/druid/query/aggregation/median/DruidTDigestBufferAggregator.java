package io.druid.query.aggregation.median;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

public class DruidTDigestBufferAggregator implements BufferAggregator {

  private final FloatColumnSelector selector;
  private final double compression;

  public DruidTDigestBufferAggregator(
      FloatColumnSelector selector,
      int compression
  )
  {
    this.selector = selector;
    this.compression = compression;
  }

  @Override
  public void init(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    DruidTDigest digest = new DruidTDigest(compression);
    digest.asBytes(mutationBuffer);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    DruidTDigest digest = DruidTDigest.fromBytes(mutationBuffer);
    digest.add(selector.get());

    mutationBuffer.position(position);
    digest.asBytes(mutationBuffer);
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return DruidTDigest.fromBytes(mutationBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("DruidTDigestBufferAggregator does not support getFloat()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("DruidTDigestBufferAggregator does not support getDouble()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("DruidTDigestBufferAggregator does not support getLong()");
  }

  @Override
  public void close() {
    // no resources to cleanup
  }
}
