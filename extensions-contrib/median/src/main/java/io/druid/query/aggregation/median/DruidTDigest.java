/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

  public synchronized void add(Object o) {
    if (o instanceof Number) {
      add(((Number)o).doubleValue());
    } else if (o instanceof DruidTDigest) {
      add((DruidTDigest)o);
    }
  }

  public synchronized void add(double x)
  {
    digest.add(x);
  }

  public synchronized void add(double x, int w)
  {
    digest.add(x, w);
  }

  public synchronized void add(DruidTDigest other)
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
      buf.putInt(0);
    } else {
      digest.asBytes(buf);
    }
  }

  @JsonValue
  public byte[] toBytes()
  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(digest.byteSize());
    asSmallBytes(byteBuffer);
    byteBuffer.position(0);
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);

    return bytes;
  }

  // asSmallBytes() and fromBytes() are modified to store the first mean as double (not float)
  // to avoid lose of precision
  public void asSmallBytes(ByteBuffer buf)
  {
    buf.putInt(TDigest.SMALL_ENCODING);
    buf.putDouble(compression());
    buf.putInt(digest.centroidCount());
    if (size() > 0)
    {
      double x = 0;
      boolean first = true;
      for (TDigest.Group group : digest.centroids()) {
        double delta = group.mean() - x;
        x = group.mean();
        if (first) {
          buf.putDouble(delta);
          first = false;
        } else {
          buf.putFloat((float) delta);
        }
      }

      for (TDigest.Group group : digest.centroids()) {
        int n = group.count();
        TDigest.encode(buf, n);
      }
    }
  }

  public static DruidTDigest fromBytes(ByteBuffer buf) {
    int encoding = buf.getInt();
    double compression = buf.getDouble();
    TDigest digest = new TDigest(compression);
    int num = buf.getInt();
    if (num > 0) {
      if (encoding == TDigest.VERBOSE_ENCODING) {
        double[] means = new double[num];
        for (int i = 0; i < num; i++) {
          means[i] = buf.getDouble();
        }
        for (int i = 0; i < num; i++) {
          digest.add(means[i], buf.getInt());
        }
      } else if (encoding == TDigest.SMALL_ENCODING) {
        double[] means = new double[num];
        double x = buf.getDouble();
        means[0] = x;
        for (int i = 1; i < num; i++) {
          double delta = buf.getFloat();
          x += delta;
          means[i] = x;
        }

        for (int i = 0; i < num; i++) {
          int z = TDigest.decode(buf);
          digest.add(means[i], z);
        }
      } else {
        throw new IllegalStateException("Invalid format for serialized histogram");
      }
    }

    return new DruidTDigest(digest);
  }
}
