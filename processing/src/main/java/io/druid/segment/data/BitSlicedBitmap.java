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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedBitSetBitmap;
import com.metamx.collections.bitmap.WrappedConciseBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.bitmap.BitSetBitmapFactory;
import io.druid.segment.column.SecondaryIndex;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 */
public abstract class BitSlicedBitmap<T extends Comparable> implements SecondaryIndex<Range<T>>
{
  protected final ValueDesc type;
  protected final BitmapFactory factory;
  protected final ImmutableBitmap[] bitmaps;
  protected final ImmutableBitmap nans;
  protected final int rowCount;

  public BitSlicedBitmap(ValueDesc type, BitmapFactory factory, ImmutableBitmap[] bitmaps, int rowCount)
  {
    Preconditions.checkArgument(type.isPrimitiveNumeric(), "only primitive numeric types allowed");
    if (type.isFloat()) {
      if (bitmaps.length == Float.SIZE) {
        this.bitmaps = bitmaps;
        this.nans = factory.makeEmptyImmutableBitmap();
      } else if (bitmaps.length == Float.SIZE + 1) {
        this.bitmaps = Arrays.copyOf(bitmaps, Float.SIZE);
        this.nans = bitmaps[Float.SIZE];
      } else {
        throw new IllegalArgumentException("Invalid number of bitmaps " + bitmaps.length);
      }
    } else if (type.isLong() || type.isDouble()) {
      if (bitmaps.length == Double.SIZE) {
        this.bitmaps = bitmaps;
        this.nans = factory.makeEmptyImmutableBitmap();
      } else if (bitmaps.length == Double.SIZE + 1) {
        this.bitmaps = Arrays.copyOf(bitmaps, Double.SIZE);
        this.nans = bitmaps[Double.SIZE];
      } else {
        throw new IllegalArgumentException("Invalid number of bitmaps " + bitmaps.length);
      }
    } else {
      throw new IllegalArgumentException("Not supported type " + type);
    }
    this.type = type;
    this.factory = factory;
    this.rowCount = rowCount;
  }

  @Override
  public ValueDesc type()
  {
    return type;
  }

  @Override
  public boolean isExact()
  {
    return true;
  }

  @Override
  public int rows()
  {
    return rowCount;
  }

  // todo
  public int zeroRows()
  {
    if (bitmaps.length == Float.SIZE) {
      return _eq(Integer.MIN_VALUE, null).size();
    } else if (bitmaps.length == Double.SIZE) {
      return _eq(Long.MIN_VALUE, null).size();
    }
    return -1;
  }

  @Override
  public void close()
  {
    Arrays.fill(bitmaps, null);
  }

  protected final ImmutableBitmap _gt(int x, boolean eq, ImmutableBitmap baseBitmap)
  {
    final MutableBitmap runner = makeRunner(factory, baseBitmap);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    final List<Integer> removing = Lists.newArrayList();
    for (final ImmutableBitmap bitmap : bitmaps) {
      final int size = bitmap.size();
      final boolean a = (x & Integer.MIN_VALUE) != 0;
      if (!a && size == 0 || a && size == rowCount) {
        x <<= 1;
        continue;
      }
      final IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (index >= rowCount) {
          break;  // fucking concise..
        }
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (b) {
          result.add(index);
        }
        removing.add(index);
      }
      for (int remove : removing) {
        runner.remove(remove);
      }
      removing.clear();
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _gt(long x, boolean eq, ImmutableBitmap baseBitmap)
  {
    final MutableBitmap runner = makeRunner(factory, baseBitmap);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    final List<Integer> removing = Lists.newArrayList();
    for (final ImmutableBitmap bitmap : bitmaps) {
      final int size = bitmap.size();
      final boolean a = (x & Long.MIN_VALUE) != 0;
      if (!a && size == 0 || a && size == rowCount) {
        x <<= 1;
        continue;
      }
      final IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (index >= rowCount) {
          break;  // fucking concise..
        }
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (b) {
          result.add(index);
        }
        removing.add(index);
      }
      for (int remove : removing) {
        runner.remove(remove);
      }
      removing.clear();
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _lt(int x, boolean eq, ImmutableBitmap baseBitmap)
  {
    final MutableBitmap runner = makeRunner(factory, baseBitmap);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    final List<Integer> removing = Lists.newArrayList();
    for (final ImmutableBitmap bitmap : bitmaps) {
      final int size = bitmap.size();
      final boolean a = (x & Integer.MIN_VALUE) != 0;
      if (!a && size == 0 || a && size == rowCount) {
        x <<= 1;
        continue;
      }
      final IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (index >= rowCount) {
          break;  // fucking concise..
        }
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (a) {
          result.add(index);
        }
        removing.add(index);
      }
      for (int remove : removing) {
        runner.remove(remove);
      }
      removing.clear();
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _lt(long x, boolean eq, ImmutableBitmap baseBitmap)
  {
    final MutableBitmap runner = makeRunner(factory, baseBitmap);
    final MutableBitmap result = factory.makeEmptyMutableBitmap();

    final List<Integer> removing = Lists.newArrayList();
    for (final ImmutableBitmap bitmap : bitmaps) {
      final int size = bitmap.size();
      final boolean a = (x & Long.MIN_VALUE) != 0;
      if (!a && size == 0 || a && size == rowCount) {
        x <<= 1;
        continue;
      }
      final IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (index >= rowCount) {
          break;  // fucking concise..
        }
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        if (a) {
          result.add(index);
        }
        removing.add(index);
      }
      for (int remove : removing) {
        runner.remove(remove);
      }
      removing.clear();
      x <<= 1;
    }
    if (eq) {
      result.or(runner);
    }
    return factory.makeImmutableBitmap(result);
  }

  protected final ImmutableBitmap _eq(int x, ImmutableBitmap baseBitmap)
  {
    final MutableBitmap runner = makeRunner(factory, baseBitmap);

    final List<Integer> removing = Lists.newArrayList();
    for (final ImmutableBitmap bitmap : bitmaps) {
      final int size = bitmap.size();
      final boolean a = (x & Integer.MIN_VALUE) != 0;
      if (!a && size == 0 || a && size == rowCount) {
        x <<= 1;
        continue;
      }
      final IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (index >= rowCount) {
          break;  // fucking concise..
        }
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        removing.add(index);
      }
      for (int remove : removing) {
        runner.remove(remove);
      }
      removing.clear();
      x <<= 1;
    }
    return factory.makeImmutableBitmap(runner);
  }

  protected final ImmutableBitmap _eq(long x, ImmutableBitmap baseBitmap)
  {
    final MutableBitmap runner = makeRunner(factory, baseBitmap);

    final List<Integer> removing = Lists.newArrayList();
    for (final ImmutableBitmap bitmap : bitmaps) {
      final int size = bitmap.size();
      final boolean a = (x & Long.MIN_VALUE) != 0;
      if (!a && size == 0 || a && size == rowCount) {
        x <<= 1;
        continue;
      }
      final IntIterator iterator = runner.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (index >= rowCount) {
          break;  // fucking concise..
        }
        final boolean b = bitmap.get(index);
        if (a == b) {
          continue;
        }
        removing.add(index);
      }
      for (int remove : removing) {
        runner.remove(remove);
      }
      removing.clear();
      x <<= 1;
    }
    return factory.makeImmutableBitmap(runner);
  }

  private MutableBitmap makeRunner(BitmapFactory factory, ImmutableBitmap baseBitmap)
  {
    final boolean noNan = nans.isEmpty();
    if (noNan && baseBitmap == null) {
      if (factory instanceof ConciseBitmapFactory) {
        ConciseSet conciseSet = new ConciseSet();
        conciseSet.fill(0, rowCount);
        return new WrappedConciseBitmap(conciseSet);
      } else if (factory instanceof BitSetBitmapFactory) {
        BitSet bitset = new BitSet();
        bitset.flip(0, rowCount);
        return new WrappedBitSetBitmap(bitset);
      }
    }
    final MutableBitmap mutable = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < rowCount; i++) {
      if (baseBitmap != null && !baseBitmap.get(i)) {
        continue;
      }
      if (noNan || !nans.get(i)) {
        mutable.add(i);
      }
    }
    return mutable;
  }
}
