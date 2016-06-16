package io.druid.query.aggregation.median;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class DruidTDigestCombiningBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector<DruidTDigest> selector;
  private final int compresssion;

  public DruidTDigestCombiningBufferAggregator(
      ObjectColumnSelector<DruidTDigest> selector,
      int compresssion
  )
  {
    this.selector = selector;
    this.compresssion = compresssion;
  }

  @Override
  public void init(ByteBuffer buf, int position) {
    DruidTDigest digest = new DruidTDigest(compresssion);

    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    digest.asBytes(mutationBuffer);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    DruidTDigest digest = DruidTDigest.fromBytes(mutationBuffer);
    DruidTDigest added = selector.get();
    digest.add(added);

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
    throw new UnsupportedOperationException("DruidTDigestCombiningBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("DruidTDigestCombiningBufferAggregator does not support getLong()");
  }

  @Override
  public void close() {

  }
}
