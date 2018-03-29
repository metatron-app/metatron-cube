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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.Ranges;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.ColumnPartProviders;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.MetricBitmap;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

/**
 */
public abstract class MetricBitmaps<T extends Comparable> implements MetricBitmap<T>
{
  public static class SerDe implements ColumnPartSerde
  {
    private final ValueType valueType;
    private final BitmapSerdeFactory serdeFactory;
    private final MetricBitmaps bitmaps;

    private transient byte[] bitmapPayload;

    @JsonCreator
    public SerDe()
    {
      valueType = null;
      serdeFactory = null;
      bitmaps = null;
    }

    public SerDe(ValueType valueType, BitmapSerdeFactory serdeFactory, MetricBitmaps bitmaps)
    {
      this.valueType = Preconditions.checkNotNull(valueType);
      this.serdeFactory = Preconditions.checkNotNull(serdeFactory);
      this.bitmaps = Preconditions.checkNotNull(bitmaps);
    }

    @Override
    public Serializer getSerializer()
    {
      return new Serializer.Abstract()
      {
        @Override
        public long getSerializedSize() throws IOException
        {
          bitmapPayload = MetricBitmaps.getStrategy(serdeFactory, valueType).toBytes(bitmaps);
          return Ints.BYTES + bitmapPayload.length;
        }

        @Override
        public void writeToChannel(WritableByteChannel channel) throws IOException
        {
          channel.write(ByteBuffer.wrap(Ints.toByteArray(bitmapPayload.length)));
          channel.write(ByteBuffer.wrap(bitmapPayload));
          bitmapPayload = null;
        }
      };
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(
            ByteBuffer buffer,
            ColumnBuilder builder,
            BitmapSerdeFactory serdeFactory
        ) throws IOException
        {
          builder.setMetricBitmap(
              ColumnPartProviders.ofType(
                  builder.getNumRows(),
                  ByteBufferSerializer.prepareForRead(buffer),
                  MetricBitmaps.getStrategy(serdeFactory, ValueDesc.assertPrimitive(builder.getType()).type())
              )
          );
        }
      };
    }
  }

  public static ObjectStrategy<MetricBitmap> getStrategy(final BitmapSerdeFactory serdeFactory, final ValueType type)
  {
    final ObjectStrategy<ImmutableBitmap> strategy = serdeFactory.getObjectStrategy();
    return new ObjectStrategy.NotComparable<MetricBitmap>()
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
                bitmapFactory,
                floats,
                loadBitmaps(buffer, strategy, size - 1),
                ByteBufferSerializer.read(buffer, strategy)
            );
          case LONG:
            long[] longs = new long[size];
            for (int i = 0; i < longs.length; i++) {
              longs[i] = buffer.getLong();
            }
            return new MetricBitmaps.LongBitmaps(
                bitmapFactory,
                longs,
                loadBitmaps(buffer, strategy, size - 1),
                ByteBufferSerializer.read(buffer, strategy)
            );
          case DOUBLE:
            double[] doubles = new double[size];
            for (int i = 0; i < doubles.length; i++) {
              doubles[i] = buffer.getDouble();
            }
            return new MetricBitmaps.DoubleBitmaps(
                bitmapFactory,
                doubles,
                loadBitmaps(buffer, strategy, size - 1),
                ByteBufferSerializer.read(buffer, strategy)
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
      public byte[] toBytes(MetricBitmap bitmap)
      {
        MetricBitmaps val = (MetricBitmaps) bitmap;
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
        byte[] bytes = strategy.toBytes(val.zeros);
        bout.writeInt(bytes.length);
        bout.write(bytes);
        return bout.toByteArray();
      }
    };
  }

  private final BitmapFactory factory;

  private final T[] breaks;   // in-ex in-ex... in-in (includes max)
  private final ImmutableBitmap[] bins;
  private final ImmutableBitmap zeros;

  public MetricBitmaps(BitmapFactory factory, T[] breaks, ImmutableBitmap[] bins, ImmutableBitmap zeros)
  {
    this.breaks = breaks;
    this.bins = bins;
    this.factory = factory;
    this.zeros = zeros;
  }

  @Override
  public abstract ValueDesc type();

  @Override
  public ImmutableBitmap filterFor(Range<T> range)
  {
    if (range.isEmpty()) {
      return factory.makeEmptyImmutableBitmap();
    }
    if (isPointZero(range)) {
      return zeros;
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
  public int numBins()
  {
    return bins.length;
  }

  protected abstract boolean isPointZero(Range<T> range);

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
  public int zeroRows()
  {
    return zeros.size();
  }

  @Override
  public void close()
  {
  }

  @Override
  public String toString()
  {
    return Arrays.toString(breaks) + ":" + Arrays.toString(getSizes());
  }

  public static class FloatBitmaps extends MetricBitmaps<Float>
  {
    private final float[] breaks;   // in-ex in-ex... in-in (includes max)

    public FloatBitmaps(BitmapFactory factory, float[] breaks, ImmutableBitmap[] bins, ImmutableBitmap zeros)
    {
      super(factory, Floats.asList(breaks).toArray(new Float[breaks.length]), bins, zeros);
      this.breaks = breaks;
    }

    @Override
    public ValueDesc type()
    {
      return ValueDesc.FLOAT;
    }

    @Override
    protected boolean isPointZero(Range<Float> range)
    {
      return Ranges.isPoint(range) && range.lowerEndpoint() == 0f;
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

    public DoubleBitmaps(BitmapFactory factory, double[] breaks, ImmutableBitmap[] bins, ImmutableBitmap zeros)
    {
      super(factory, Doubles.asList(breaks).toArray(new Double[breaks.length]), bins, zeros);
      this.breaks = breaks;
    }

    @Override
    public ValueDesc type()
    {
      return ValueDesc.DOUBLE;
    }

    @Override
    protected boolean isPointZero(Range<Double> range)
    {
      return Ranges.isPoint(range) && range.lowerEndpoint() == 0d;
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

    public LongBitmaps(BitmapFactory factory, long[] breaks, ImmutableBitmap[] bins, ImmutableBitmap zeros)
    {
      super(factory, Longs.asList(breaks).toArray(new Long[breaks.length]), bins, zeros);
      this.breaks = breaks;
    }

    @Override
    public ValueDesc type()
    {
      return ValueDesc.LONG;
    }

    @Override
    protected boolean isPointZero(Range<Long> range)
    {
      return Ranges.isPoint(range) && range.lowerEndpoint() == 0l;
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
