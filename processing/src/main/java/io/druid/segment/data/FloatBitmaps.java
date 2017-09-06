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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueType;
import io.druid.segment.column.MetricBitmap;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class FloatBitmaps implements MetricBitmap<Float>
{
  public static ObjectStrategy<FloatBitmaps> getStrategy(final BitmapSerdeFactory serdeFactory)
  {
    final ObjectStrategy<ImmutableBitmap> strategy = serdeFactory.getObjectStrategy();
    return new ObjectStrategy.NotComparable<FloatBitmaps>()
    {
      @Override
      public Class<? extends FloatBitmaps> getClazz()
      {
        return FloatBitmaps.class;
      }

      @Override
      public FloatBitmaps fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        float[] breaks = new float[buffer.getInt()];
        for (int i = 0; i < breaks.length; i++) {
          breaks[i] = buffer.getFloat();
        }
        ImmutableBitmap[] bitmaps = new ImmutableBitmap[breaks.length - 1];
        for (int i = 0; i < bitmaps.length; i++) {
          bitmaps[i] = ByteBufferSerializer.read(buffer, strategy);
        }
        return new FloatBitmaps(serdeFactory.getBitmapFactory(), breaks, bitmaps);
      }

      @Override
      public byte[] toBytes(FloatBitmaps val)
      {
        ByteArrayDataOutput bout = ByteStreams.newDataOutput();
        bout.writeInt(val.breaks.length);
        for (float aBreak : val.breaks) {
          bout.writeFloat(aBreak);
        }
        for (ImmutableBitmap bin : val.bins) {
          byte[] bytes = strategy.toBytes(bin);
          bout.writeInt(bytes.length);
          bout.write(bytes);
        }
        return bout.toByteArray();
      }

      @Override
      public int compare(FloatBitmaps o1, FloatBitmaps o2)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private final BitmapFactory factory;
  private final float[] breaks;   // in-ex in-ex... in-in (includes max)
  private final ImmutableBitmap[] bins;

  public FloatBitmaps(BitmapFactory factory, float[] breaks, ImmutableBitmap[] bins)
  {
    this.breaks = breaks;
    this.bins = bins;
    this.factory = factory;
  }

  @Override
  public ValueType type()
  {
    return ValueType.FLOAT;
  }

  @Override
  public BitmapFactory getFactory()
  {
    return factory;
  }

  @Override
  public ImmutableBitmap filterFor(Range<Float> range)
  {
    if (range.isEmpty()) {
      return factory.makeEmptyImmutableBitmap();
    }
    int from = 0;
    int to = bins.length - 1;
    if (range.hasLowerBound()) {
      float lower = range.lowerEndpoint();
      int index = Arrays.binarySearch(breaks, lower);
      if (index == bins.length) {
        return range.lowerBoundType() == BoundType.CLOSED ? bins[index - 1] : factory.makeEmptyImmutableBitmap();
      }
      from = index < 0 ? -index - 2 : index;
    }
    if (range.hasUpperBound()) {
      float upper = range.upperEndpoint();
      int index = Arrays.binarySearch(breaks, upper);
      if (index == -1 || (index == 0 && range.upperBoundType() == BoundType.OPEN)) {
        return factory.makeEmptyImmutableBitmap();
      }
      to = Math.min(to, index < 0 ? -index - 2 : range.upperBoundType() == BoundType.OPEN ? index - 1 : index);
    }
    if (from == to) {
      return bins[from];
    }
    return factory.union(Arrays.asList(bins).subList(from, to + 1));
  }

  public float[] getBreaks()
  {
    return breaks;
  }

  public ImmutableBitmap[] getBins()
  {
    return bins;
  }

  public int[] getSizes()
  {
    int[] sizes = new int[bins.length];
    for (int i = 0; i < sizes.length; i++) {
      sizes[i] = bins[i].size();
    }
    return sizes;
  }

  public float getMin()
  {
    return breaks[0];
  }

  public float getMax()
  {
    return breaks[bins.length];
  }

  @Override
  public int size()
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
}
