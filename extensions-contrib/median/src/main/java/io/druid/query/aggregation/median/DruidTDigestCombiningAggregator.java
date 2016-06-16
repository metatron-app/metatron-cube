package io.druid.query.aggregation.median;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class DruidTDigestCombiningAggregator implements Aggregator
{
  private final String name;
  private final ObjectColumnSelector<DruidTDigest> selector;
  private final int compression;

  private DruidTDigest digest;

  public DruidTDigestCombiningAggregator(
      String name,
      ObjectColumnSelector<DruidTDigest> selector,
      int compression
  )
  {
    this.name = name;
    this.selector = selector;
    this.compression = compression;

    this.digest = new DruidTDigest(compression);
  }

  @Override
  public void aggregate() {
    DruidTDigest target = selector.get();
    if (target == null) return;

    digest.add(target);
  }

  @Override
  public void reset() {
    this.digest = new DruidTDigest(compression);
  }

  @Override
  public Object get() {
    return digest;
  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException("DruidTDigestCombiningAggregator does not support getFloat()");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void close() {

  }

  @Override
  public long getLong() {
    throw new UnsupportedOperationException("DruidTDigestCombiningAggregator does not support getLong()");
  }
}
