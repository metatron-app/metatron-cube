/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.Ranges;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.filter.DimFilters;
import io.druid.segment.ColumnPartProviders;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.serde.ColumnPartSerde;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 */
public class BitSlicedBitmaps
{
  public static class SerDe implements ColumnPartSerde
  {
    private final ValueType valueType;
    private final BitmapSerdeFactory serdeFactory;
    private final BitSlicedBitmap bitmaps;
    private byte[] bitmapPayload;

    @JsonCreator
    public SerDe()
    {
      valueType = null;
      serdeFactory = null;
      bitmaps = null;
    }

    public SerDe(ValueType valueType, BitmapSerdeFactory serdeFactory, BitSlicedBitmap bitmaps)
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
          bitmapPayload = BitSlicedBitmaps.getStrategy(serdeFactory, valueType).toBytes(bitmaps);
          return Integer.BYTES + bitmapPayload.length;
        }

        @Override
        public void writeToChannel(WritableByteChannel channel) throws IOException
        {
          channel.write(ByteBuffer.wrap(Ints.toByteArray(bitmapPayload.length)));
          channel.write(ByteBuffer.wrap(bitmapPayload));
        }
      };
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory)
        {
          builder.setBitSlicedBitmap(
              ColumnPartProviders.ofBitSlicedBitmap(
                  builder.getNumRows(),
                  ByteBufferSerializer.prepareForRead(buffer),
                  BitSlicedBitmaps.getStrategy(serdeFactory, builder.getType().type())
              )
          );
        }
      };
    }
  }

  public static ObjectStrategy<BitSlicedBitmap> getStrategy(final BitmapSerdeFactory serdeFactory, final ValueType type)
  {
    final ObjectStrategy<ImmutableBitmap> strategy = serdeFactory.getObjectStrategy();
    return new ObjectStrategy<BitSlicedBitmap>()
    {
      @Override
      public Class<? extends BitSlicedBitmap> getClazz()
      {
        return BitSlicedBitmap.class;
      }

      @Override
      public BitSlicedBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        BitmapFactory bitmapFactory = serdeFactory.getBitmapFactory();
        int rowCount = buffer.getInt();
        ImmutableBitmap[] bitmaps = loadBitmaps(buffer, strategy, buffer.get());
        switch (type) {
          case FLOAT:
            return new FloatType(bitmapFactory, bitmaps, rowCount);
          case LONG:
            return new LongType(bitmapFactory, bitmaps, rowCount);
          case DOUBLE:
            return new DoubleType(bitmapFactory, bitmaps, rowCount);
          default:
            throw new UnsupportedOperationException(type + " is not supported");
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
      public byte[] toBytes(BitSlicedBitmap val)
      {
        ByteArrayDataOutput bout = ByteStreams.newDataOutput();
        bout.writeInt(val.rowCount);
        bout.write(val.bitmaps.length);
        for (ImmutableBitmap bin : val.bitmaps) {
          byte[] bytes = strategy.toBytes(bin);
          bout.writeInt(bytes.length);
          bout.write(bytes);
        }
        return bout.toByteArray();
      }
    };
  }

  public static class FloatType extends BitSlicedBitmap<Float>
  {
    FloatType(BitmapFactory factory, ImmutableBitmap[] bitmaps, int rowCount)
    {
      super(ValueDesc.FLOAT, factory, bitmaps, rowCount);
    }

    @Override
    public final ImmutableBitmap filterFor(Range<Float> query, FilterContext context, String attachment)
    {
      if (query.isEmpty() ||
          query.hasLowerBound() && query.lowerEndpoint().isNaN() ||
          query.hasLowerBound() && query.lowerEndpoint().isNaN()) {
        return factory.makeEmptyImmutableBitmap();
      }
      final ImmutableBitmap baseBitmap = context == null ? null : context.getBaseBitmap();
      if (Ranges.isPoint(query)) {
        return _eq(BitSlicer.normalize(query.lowerEndpoint()), baseBitmap);
      }
      final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      if (query.hasLowerBound()) {
        final int normalized = BitSlicer.normalize(query.lowerEndpoint());
        bitmaps.add(_gt(normalized, query.lowerBoundType() == BoundType.CLOSED, baseBitmap));
      }
      if (query.hasUpperBound()) {
        final int normalized = BitSlicer.normalize(query.upperEndpoint());
        bitmaps.add(_lt(normalized, query.upperBoundType() == BoundType.CLOSED, baseBitmap));
      }
      return DimFilters.intersection(factory, bitmaps);
    }
  }

  public static class DoubleType extends BitSlicedBitmap<Double>
  {
    DoubleType(BitmapFactory factory, ImmutableBitmap[] bitmaps, int rowCount)
    {
      super(ValueDesc.DOUBLE, factory, bitmaps, rowCount);
    }

    @Override
    public final ImmutableBitmap filterFor(Range<Double> query, FilterContext context, String attachment)
    {
      if (query.isEmpty() ||
          query.hasLowerBound() && query.lowerEndpoint().isNaN() ||
          query.hasLowerBound() && query.lowerEndpoint().isNaN()) {
        return factory.makeEmptyImmutableBitmap();
      }
      final ImmutableBitmap baseBitmap = context == null ? null : context.getBaseBitmap();
      if (Ranges.isPoint(query)) {
        return _eq(BitSlicer.normalize(query.lowerEndpoint()), baseBitmap);
      }
      final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      if (query.hasLowerBound()) {
        final long normalized = BitSlicer.normalize(query.lowerEndpoint());
        bitmaps.add(_gt(normalized, query.lowerBoundType() == BoundType.CLOSED, baseBitmap));
      }
      if (query.hasUpperBound()) {
        final long normalized = BitSlicer.normalize(query.upperEndpoint());
        bitmaps.add(_lt(normalized, query.upperBoundType() == BoundType.CLOSED, baseBitmap));
      }
      return DimFilters.intersection(factory, bitmaps);
    }
  }

  public static class LongType extends BitSlicedBitmap<Long>
  {
    LongType(BitmapFactory factory, ImmutableBitmap[] bitmaps, int rowCount)
    {
      super(ValueDesc.LONG, factory, bitmaps, rowCount);
    }

    @Override
    public final ImmutableBitmap filterFor(Range<Long> query, FilterContext context, String attachment)
    {
      if (query.isEmpty()) {
        return factory.makeEmptyImmutableBitmap();
      }
      final ImmutableBitmap baseBitmap = context == null ? null : context.getBaseBitmap();
      if (Ranges.isPoint(query)) {
        return _eq(BitSlicer.normalize(query.lowerEndpoint()), baseBitmap);
      }
      final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      if (query.hasLowerBound()) {
        final long normalized = BitSlicer.normalize(query.lowerEndpoint());
        bitmaps.add(_gt(normalized, query.lowerBoundType() == BoundType.CLOSED, baseBitmap));
      }
      if (query.hasUpperBound()) {
        final long normalized = BitSlicer.normalize(query.upperEndpoint());
        bitmaps.add(_lt(normalized, query.upperBoundType() == BoundType.CLOSED, baseBitmap));
      }
      return DimFilters.intersection(factory, bitmaps);
    }
  }

  public static String toString(ImmutableBitmap bitmap)
  {
    StringBuilder b = new StringBuilder();
    IntIterator it = bitmap.iterator();
    while (it.hasNext()) {
      if (b.length() > 0) {
        b.append(',');
      }
      b.append(it.next());
    }
    return b.toString();
  }

  public static String toString(int v, int length)
  {
    String x = Integer.toBinaryString(v);
    StringBuilder b = new StringBuilder();
    for (int i = length - x.length(); i > 0; i--) {
      b.append('0');
    }
    return b.append(x).toString();
  }
}
