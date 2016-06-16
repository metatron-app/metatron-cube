package io.druid.query.aggregation.median;

import com.clearspring.analytics.stream.quantile.GroupTree;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.clearspring.analytics.util.Preconditions;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class DruidTDigest {
  private final TDigest digest;

  public DruidTDigest(double compression)
  {
    digest = new TDigest(compression);
  }

  private DruidTDigest(TDigest digest)
  {
    this.digest = digest;
  }

  public void add(double x)
  {
    digest.add(x);
  }

  public void add(double x, int w)
  {
    digest.add(x, w);
  }

  public void add(DruidTDigest other)
  {
    if (other.size() > 0)
    {
      digest.add(other.digest);
    }
  }

  public static DruidTDigest merge(double compression, Iterable<DruidTDigest> subData) {
    return new DruidTDigest(TDigest.merge(compression, Iterables.transform(subData, new Function<DruidTDigest, TDigest>() {
      public TDigest apply(DruidTDigest druidDigest)
      {
        return druidDigest.digest;
      }
    })));
  }

  public void compress()
  {
    digest.compress();
  }

  public int size()
  {
    return digest.size();
  }

  public double cdf(double x)
  {
    return digest.cdf(x);
  }

  public double quantile(double q)
  {
    return newQuantile(q);
  }

  public double[] quantiles(double[] qs)
  {
    return newQuantiles(qs);
  }

  private double newQuantile(double quantile)
  {
    return newQuantiles(new double[] {quantile})[0];
  }

  private double[] newQuantiles(double[] quantiles)
  {
    double[] args = new double[quantiles.length];

    int index = 0;
    for (double quantile: quantiles) {
      Preconditions.checkArgument(1 >= quantile);
      Preconditions.checkArgument(0 <= quantile);

      args[index++] = digest.size() * quantile;
    }

    return NthValues(args, true);
  }

  public double median() {
    return NthValues(new double[] {(digest.size() + 1) / 2.0}, false)[0];
  }

  // get N th element from the given values
  // (actually, N can be double number)
  private double[] NthValues(double[] targetPositions, boolean contains)
  {
    GroupTree values = (GroupTree) digest.centroids();
    Preconditions.checkArgument(values.size() > 0);

    Iterator<TDigest.Group> groupIterator = values.iterator();

    double currentPosition = 0;
    double prevMean = 0;
    double[] quantileValues = new double[targetPositions.length];
    int quantileIndex = 0;

    while (groupIterator.hasNext()) {
      TDigest.Group center = groupIterator.next();
      currentPosition += center.count();
      while (currentPosition >= targetPositions[quantileIndex]) {
        if (contains && center.count() == 1) {
          quantileValues[quantileIndex] = center.mean();
        } else {
          quantileValues[quantileIndex] =
              center.mean() - (center.mean() - prevMean) * (currentPosition - targetPositions[quantileIndex]) / center.count();
        }
        if (++quantileIndex == targetPositions.length)
          return quantileValues;
      }
      prevMean = center.mean();
    }

    //should not reach hear
    for (;quantileIndex < targetPositions.length; quantileIndex++) {
      quantileValues[quantileIndex] = 1;
    }
    return quantileValues;
  }

  public double compression()
  {
    return digest.compression();
  }

  public int byteSize()
  {
    return digest.byteSize();
  }

  public int maxByteSize()
  {
    return 4 + 8 + 4 + (int)(compression() * 100 * 12);
  }

  public static int maxStorageSize(int compression)
  {
    return 4 + 8 + 4 + compression * 100 * 12;
  }

  public int smallByteSize()
  {
    return digest.smallByteSize();
  }

  public void asBytes(ByteBuffer buf)
  {
    if (size() == 0)
    {
      buf.putInt(TDigest.VERBOSE_ENCODING);
      buf.putDouble(compression());
    } else {
      digest.asBytes(buf);
    }
  }

  @JsonValue
  public byte[] toBytes()
  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(digest.byteSize());
    asBytes(byteBuffer);
    byteBuffer.position(0);
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);

    return bytes;
  }

  public void asSmallBytes(ByteBuffer buf)
  {
    digest.asSmallBytes(buf);
  }

  public static DruidTDigest fromBytes(ByteBuffer buf) {
    TDigest digest = TDigest.fromBytes(buf);
    return new DruidTDigest(digest);
  }
}
