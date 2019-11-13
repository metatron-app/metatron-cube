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

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.data.ValueType;

/**
 */
public abstract class BitSlicer<T extends Comparable>
{
  protected final ValueType valueType;
  protected final BitmapFactory factory;
  protected final MutableBitmap[] bitmaps;
  protected final MutableBitmap nans;

  protected int rowCount;

  BitSlicer(ValueType valueType, BitmapFactory factory)
  {
    this.factory = factory;
    this.valueType = valueType;
    this.bitmaps = new MutableBitmap[valueType.lengthOfBinary()];
    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = factory.makeEmptyMutableBitmap();
    }
    nans = factory.makeEmptyMutableBitmap();
  }

  public abstract void add(T value);

  public abstract BitSlicedBitmap<T> build();

  public abstract String toBitmapString(T value);

  protected String toDumpString()
  {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < rowCount; i++) {
      for (MutableBitmap bitmap : bitmaps) {
        b.append(bitmap.get(i) ? '1' : '0');
      }
      b.append('\n');
    }
    return b.toString();
  }

  protected ImmutableBitmap[] toFinalized()
  {
    boolean hasNan = !nans.isEmpty();
    ImmutableBitmap[] immutable = new ImmutableBitmap[hasNan ? bitmaps.length + 1: bitmaps.length];

    int i = 0;
    for (; i < bitmaps.length; i++) {
      immutable[i] = factory.makeImmutableBitmap(bitmaps[i]);
    }
    if (hasNan) {
      immutable[i] = factory.makeImmutableBitmap(nans);
    }
    return immutable;
  }

  static abstract class Base32<T extends Comparable> extends BitSlicer<T>
  {
    public Base32(ValueType valueType, BitmapFactory factory)
    {
      super(valueType, factory);
    }

    @Override
    public void add(T value)
    {
      if (value == null) {
        rowCount++;
      } else if (isNan(value)) {
        nans.add(rowCount++);
      } else {
        int val = normalize(value);
        for (MutableBitmap bitmap : bitmaps) {
          if ((val & Integer.MIN_VALUE) != 0) {
            bitmap.add(rowCount);
          }
          val <<= 1;
        }
        rowCount++;
      }
    }

    @Override
    public String toBitmapString(T value)
    {
      String x = Integer.toBinaryString(normalize(value));
      StringBuilder b = new StringBuilder();
      for (int i = 32 - x.length(); i > 0; i--) {
        b.append('0');
      }
      return b.append(x).toString();
    }

    protected abstract boolean isNan(T value);

    protected abstract int normalize(T value);
  }

  static abstract class Base64<T extends Comparable> extends BitSlicer<T>
  {
    public Base64(ValueType valueType, BitmapFactory factory)
    {
      super(valueType, factory);
    }

    @Override
    public void add(T value)
    {
      if (value == null) {
        rowCount++;
      } else if (isNan(value)) {
        nans.add(rowCount++);
      } else {
        long val = normalize(value);
        for (MutableBitmap bitmap : bitmaps) {
          if ((val & Long.MIN_VALUE) != 0) {
            bitmap.add(rowCount);
          }
          val <<= 1;
        }
        rowCount++;
      }
    }

    @Override
    public String toBitmapString(T value)
    {
      String x = Long.toBinaryString(normalize(value));
      StringBuilder b = new StringBuilder();
      for (int i = 64 - x.length(); i > 0; i--) {
        b.append('0');
      }
      return b.append(x).toString();
    }

    protected abstract boolean isNan(T value);

    protected abstract long normalize(T value);
  }

  public static class FloatType extends BitSlicer.Base32<Float>
  {
    public FloatType(BitmapFactory factory)
    {
      super(ValueType.FLOAT, factory);
    }

    @Override
    protected boolean isNan(Float value)
    {
      return value.isNaN();
    }

    @Override
    protected final int normalize(Float v)
    {
      return BitSlicer.normalize(v);
    }

    @Override
    public BitSlicedBitmap<Float> build()
    {
      return new BitSlicedBitmaps.FloatType(factory, toFinalized(), rowCount);
    }
  }

  public static class LongType extends BitSlicer.Base64<Long>
  {
    public LongType(BitmapFactory factory)
    {
      super(ValueType.LONG, factory);
    }

    @Override
    protected boolean isNan(Long value)
    {
      return false;
    }

    @Override
    protected final long normalize(Long v)
    {
      return BitSlicer.normalize(v);
    }

    @Override
    public BitSlicedBitmap<Long> build()
    {
      return new BitSlicedBitmaps.LongType(factory, toFinalized(), rowCount);
    }
  }

  public static class DoubleType extends BitSlicer.Base64<Double>
  {
    public DoubleType(BitmapFactory factory)
    {
      super(ValueType.DOUBLE, factory);
    }

    @Override
    protected boolean isNan(Double value)
    {
      return value.isNaN();
    }

    @Override
    protected final long normalize(Double v)
    {
      return BitSlicer.normalize(v);
    }

    @Override
    public BitSlicedBitmap<Double> build()
    {
      return new BitSlicedBitmaps.DoubleType(factory, toFinalized(), rowCount);
    }
  }

  static long normalize(long l)
  {
    return l ^ Long.MIN_VALUE;
  }

  static int normalize(float f)
  {
    if (Float.isNaN(f)) {
      return 0x00;
    }
    int i = Float.floatToIntBits(f);
    if (f >= 0) {
      i |= Integer.MIN_VALUE; // cannot flip (-0f)
    } else {
      i &= Integer.MAX_VALUE;
      i ^= Integer.MAX_VALUE;
    }
    return i;
  }

  static long normalize(double d)
  {
    if (Double.isNaN(d)) {
      return 0x00;
    }
    long l = Double.doubleToLongBits(d);
    if (d >= 0) {
      l |= Long.MIN_VALUE; // cannot flip (-0d)
    } else {
      l &= Long.MAX_VALUE;
      l ^= Long.MAX_VALUE;
    }
    return l;
  }
}
