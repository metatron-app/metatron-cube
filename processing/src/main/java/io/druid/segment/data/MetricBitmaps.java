/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueType;
import io.druid.segment.column.MetricBitmap;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public abstract class MetricBitmaps<T extends Comparable> implements MetricBitmap<T>
{
  public static ObjectStrategy<MetricBitmaps> getStrategy(final BitmapSerdeFactory serdeFactory, final ValueType type)
  {
    final ObjectStrategy<ImmutableBitmap> strategy = serdeFactory.getObjectStrategy();
    return new ObjectStrategy.NotComparable<MetricBitmaps>()
    {
      @Override
      public Class<? extends MetricBitmaps> getClazz()
      {
        return MetricBitmaps.class;
      }

      @Override
      public MetricBitmaps fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        BitmapFactory bitmapFactory = serdeFactory.getBitmapFactory();
        int size = buffer.getInt();
        switch (type) {
          case FLOAT:
            float[] floats = new float[size];
            for (int i = 0; i < floats.length; i++) {
              floats[i] = buffer.getFloat();
            }
            return new MetricBitmaps.FloatBitmaps(
                bitmapFactory, floats, loadBitmaps(buffer, strategy, size - 1)
            );
          case LONG:
            long[] longs = new long[size];
            for (int i = 0; i < longs.length; i++) {
              longs[i] = buffer.getLong();
            }
            return new MetricBitmaps.LongBitmaps(
                bitmapFactory, longs, loadBitmaps(buffer, strategy, size - 1)
            );
          case DOUBLE:
            double[] doubles = new double[size];
            for (int i = 0; i < doubles.length; i++) {
              doubles[i] = buffer.getDouble();
            }
            return new MetricBitmaps.DoubleBitmaps(
                bitmapFactory, doubles, loadBitmaps(buffer, strategy, size - 1)
            );
          default:
            throw new UnsupportedClassVersionError(type + " is not supported");
        }
      }

      private ImmutableBitmap[] loadBitmaps(
          ByteBuffer buffer,
          ObjectStrategy<ImmutableBitmap> strategy,
          int length
      )
      {
        ImmutableBitmap[] bitmaps = new ImmutableBitmap[length];
        for (int i = 0; i < bitmaps.length; i++) {
          bitmaps[i] = ByteBufferSerializer.read(buffer, strategy);
        }
        return bitmaps;
      }

      @Override
      public byte[] toBytes(MetricBitmaps val)
      {
        ByteArrayDataOutput bout = ByteStreams.newDataOutput();
        bout.writeInt(val.breaks.length);
        switch (type) {
          case FLOAT:
            for (Comparable aBreak : val.breaks) {
              bout.writeFloat((Float) aBreak);
            }
            break;
          case DOUBLE:
            for (Comparable aBreak : val.breaks) {
              bout.writeDouble((Double) aBreak);
            }
            break;
          case LONG:
            for (Comparable aBreak : val.breaks) {
              bout.writeLong((Long) aBreak);
            }
            break;
          default:
            throw new UnsupportedClassVersionError(type + " is not supported");
        }
        for (ImmutableBitmap bin : val.bins) {
          byte[] bytes = strategy.toBytes(bin);
          bout.writeInt(bytes.length);
          bout.write(bytes);
        }
        return bout.toByteArray();
      }

      @Override
      public int compare(MetricBitmaps o1, MetricBitmaps o2)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private final BitmapFactory factory;

  private final T[] breaks;   // in-ex in-ex... in-in (includes max)
  private final ImmutableBitmap[] bins;

  public MetricBitmaps(BitmapFactory factory, T[] breaks, ImmutableBitmap[] bins)
  {
    this.breaks = breaks;
    this.bins = bins;
    this.factory = factory;
  }

  @Override
  public abstract ValueType type();

  @Override
  public BitmapFactory getFactory()
  {
    return factory;
  }

  @Override
  public ImmutableBitmap filterFor(Range<T> range)
  {
    if (range.isEmpty()) {
      return factory.makeEmptyImmutableBitmap();
    }
    int from = 0;
    int to = bins.length - 1;
    if (range.hasLowerBound()) {
      T lower = range.lowerEndpoint();
      int index = binarySearch(lower);
      if (index == bins.length) {
        return range.lowerBoundType() == BoundType.CLOSED ? bins[index - 1] : factory.makeEmptyImmutableBitmap();
      }
      from = index < 0 ? -index - 2 : index;
    }
    if (range.hasUpperBound()) {
      T upper = range.upperEndpoint();
      int index = binarySearch(upper);
      if (index == -1 || (index == 0 && range.upperBoundType() == BoundType.OPEN)) {
        return factory.makeEmptyImmutableBitmap();
      }
      to = Math.min(to, index < 0 ? -index - 2 : range.upperBoundType() == BoundType.OPEN ? index - 1 : index);
    }
    if (from < 0) {
      return factory.makeEmptyImmutableBitmap();
    }
    if (from == to) {
      return bins[from];
    }
    return factory.union(Arrays.asList(bins).subList(from, to + 1));
  }

  @Override
  public int size()
  {
    return bins.length;
  }

  protected abstract int binarySearch(T lower);

  @VisibleForTesting
  public int[] getSizes()
  {
    int[] sizes = new int[bins.length];
    for (int i = 0; i < sizes.length; i++) {
      sizes[i] = bins[i].size();
    }
    return sizes;
  }

  public T getMin()
  {
    return breaks[0];
  }

  public T getMax()
  {
    return breaks[bins.length];
  }

  @Override
  public int rows()
  {
    int size = 0;
    for (ImmutableBitmap bin : bins) {
      size += bin.size();
    }
    return size;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(breaks) + ":" + Arrays.toString(getSizes());
  }

  public static class FloatBitmaps extends MetricBitmaps<Float>
  {
    private final float[] breaks;   // in-ex in-ex... in-in (includes max)

    public FloatBitmaps(BitmapFactory factory, float[] breaks, ImmutableBitmap[] bins)
    {
      super(factory, Floats.asList(breaks).toArray(new Float[breaks.length]), bins);
      this.breaks = breaks;
    }

    @Override
    public ValueType type()
    {
      return ValueType.FLOAT;
    }

    @Override
    protected final int binarySearch(Float lower)
    {
      return Arrays.binarySearch(breaks, lower);
    }

    public float[] breaks()
    {
      return breaks;
    }
  }

  public static class DoubleBitmaps extends MetricBitmaps<Double>
  {
    private final double[] breaks;   // in-ex in-ex... in-in (includes max)

    public DoubleBitmaps(BitmapFactory factory, double[] breaks, ImmutableBitmap[] bins)
    {
      super(factory, Doubles.asList(breaks).toArray(new Double[breaks.length]), bins);
      this.breaks = breaks;
    }

    @Override
    public ValueType type()
    {
      return ValueType.DOUBLE;
    }

    @Override
    protected final int binarySearch(Double lower)
    {
      return Arrays.binarySearch(breaks, lower);
    }

    public double[] breaks()
    {
      return breaks;
    }
  }

  public static class LongBitmaps extends MetricBitmaps<Long>
  {
    private final long[] breaks;   // in-ex in-ex... in-in (includes max)

    public LongBitmaps(BitmapFactory factory, long[] breaks, ImmutableBitmap[] bins)
    {
      super(factory, Longs.asList(breaks).toArray(new Long[breaks.length]), bins);
      this.breaks = breaks;
    }

    @Override
    public ValueType type()
    {
      return ValueType.LONG;
    }

    @Override
    protected final int binarySearch(Long lower)
    {
      return Arrays.binarySearch(breaks, lower);
    }

    public long[] breaks()
    {
      return breaks;
    }
  }
}
