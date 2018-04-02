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

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.BitSetBitmapFactory;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedBitSetBitmap;
import com.metamx.collections.bitmap.WrappedConciseBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.column.SecondaryIndex;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.BitSet;

/**
 */
public abstract class BitSlicedBitmap<T extends Comparable> implements SecondaryIndex<Range<T>>
{
  protected final ValueDesc type;
  protected final BitmapFactory factory;
  protected final ImmutableBitmap[] bitmaps;
  protected final int rowCount;

  public BitSlicedBitmap(ValueDesc type, BitmapFactory factory, ImmutableBitmap[] bitmaps, int rowCount)
  {
    Preconditions.checkArgument(type.isNumeric(), "only primitive numeric types allowed");
    if (type.isFloat()) {
      Preconditions.checkArgument(bitmaps.length == 32);
    } else if (type.isLong() || type.isDouble()) {
      Preconditions.checkArgument(bitmaps.length == 64);
    } else {
      throw new IllegalArgumentException("Not supported type " + type);
    }
    this.type = type;
    this.factory = factory;
    this.bitmaps = bitmaps;
    this.rowCount = rowCount;
  }

  @Override
  public ValueDesc type()
  {
    return type;
  }

  @Override
  public int rows()
  {
    return rowCount;
  }

  // todo
  public int zeroRows()
  {
    if (bitmaps.length == 32) {
      return _eq(Integer.MIN_VALUE).size();
    } else if (bitmaps.length == 64) {
      return _eq(Long.MIN_VALUE).size();
    }
    return -1;
  }

  @Override
  public void close() throws IOException
  {
  }

  protected final ImmutableBitmap _gt(int x, boolean eq)
  {
    final MutableBitmap runner = makeRunner(factory);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    for (ImmutableBitmap bitmap : bitmaps) {
      final boolean a = (x & Integer.MIN_VALUE) != 0;
      IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        int index = iterator.next();
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (!a) {
          result.add(index);
        }
        runner.remove(index);
      }
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _gt(long x, boolean eq)
  {
    final MutableBitmap runner = makeRunner(factory);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    for (ImmutableBitmap bitmap : bitmaps) {
      final boolean a = (x & Long.MIN_VALUE) != 0;
      IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        int index = iterator.next();
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (!a) {
          result.add(index);
        }
        runner.remove(index);
      }
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _lt(int x, boolean eq)
  {
    final MutableBitmap runner = makeRunner(factory);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    for (ImmutableBitmap bitmap : bitmaps) {
      final boolean a = (x & Integer.MIN_VALUE) != 0;
      IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        int index = iterator.next();
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (a) {
          result.add(index);
        }
        runner.remove(index);
      }
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _lt(long x, boolean eq)
  {
    final MutableBitmap runner = makeRunner(factory);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    for (ImmutableBitmap bitmap : bitmaps) {
      final boolean a = (x & Long.MIN_VALUE) != 0;
      IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        int index = iterator.next();
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (a) {
          result.add(index);
        }
        runner.remove(index);
      }
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _eq(int x)
  {
    final MutableBitmap runner = makeRunner(factory);

    for (ImmutableBitmap bitmap : bitmaps) {
      final boolean a = (x & Integer.MIN_VALUE) != 0;
      IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        int index = iterator.next();
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        runner.remove(index);
      }
      x <<= 1;
    }
    return factory.makeImmutableBitmap(runner);
  }

  protected final ImmutableBitmap _eq(long x)
  {
    final MutableBitmap runner = makeRunner(factory);

    for (ImmutableBitmap bitmap : bitmaps) {
      final boolean a = (x & Long.MIN_VALUE) != 0;
      IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        int index = iterator.next();
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        runner.remove(index);
      }
      x <<= 1;
    }
    return factory.makeImmutableBitmap(runner);
  }

  private MutableBitmap makeRunner(BitmapFactory factory)
  {
    if (factory instanceof ConciseBitmapFactory) {
      ConciseSet conciseSet = new ConciseSet();
      conciseSet.fill(0, rowCount);
      return new WrappedConciseBitmap(conciseSet);
    } else if (factory instanceof BitSetBitmapFactory) {
      BitSet bitset = new BitSet();
      bitset.flip(0, rowCount);
      return new WrappedBitSetBitmap(bitset);
    }
    MutableBitmap mutable = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < rowCount; i++) {
      mutable.add(i);
    }
    return mutable;
  }
}
