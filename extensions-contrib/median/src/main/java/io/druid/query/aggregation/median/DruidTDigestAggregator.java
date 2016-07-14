package io.druid.query.aggregation.median;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class DruidTDigestAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Double.compare(((DruidTDigest) o).median(), ((DruidTDigest) o1).median());
    }
  };

  // can hold compression * 5 points typically
  public static final int DEFAULT_COMPRESSION = 10;

  private final String name;
  private final ObjectColumnSelector selector;
  private final double compression;
  private DruidTDigest digest;

  public DruidTDigestAggregator(
      String name,
      ObjectColumnSelector selector,
      int compression
  )
  {
    this.name = name;
    this.digest = new DruidTDigest(compression);
    this.selector = selector;
    this.compression = compression;
  }

  @Override
  public void aggregate()
  {
    digest.add(selector.get());
  }

  @Override
  public void reset()
  {
    this.digest = new DruidTDigest(compression);
  }

  @Override
  public Object get()
  {
    return digest;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("DruidTDigestAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("DruidTDigestAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("DruidTDigestAggregator does not support getDouble()");
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  public int getMaxStorageSize()
  {
    return digest.maxByteSize();
  }
}
