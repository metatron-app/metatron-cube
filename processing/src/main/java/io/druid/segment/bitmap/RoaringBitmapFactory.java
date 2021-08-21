/*
 * Copyright 2011 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.bitmap;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.data.VLongUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.logger.Logger;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringArray;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;

// default implementation of union/intersection makes a lot of copy, which is not necessary for our use case
// simply using bitset and return back to roaring bitmap out-performs in the most of real use-cases (it's worse in single threaded micro test)
public final class RoaringBitmapFactory extends com.metamx.collections.bitmap.RoaringBitmapFactory
{
  private static final Logger LOG = new Logger(RoaringBitmapFactory.class);

  public RoaringBitmapFactory()
  {
    super();
  }

  public RoaringBitmapFactory(boolean compressRunOnSerialization)
  {
    super(compressRunOnSerialization);
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return new WrappedImmutableRoaringBitmap(((WrappedImmutableRoaringBitmap) super.makeEmptyImmutableBitmap()).getBitmap())
    {
      @Override
      public boolean get(int value)
      {
        return false;
      }
    };
  }

  private static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
  private static final short SERIAL_COOKIE = 12347;

  private static final short SMALL_COOKIE = 12345;
  private static final short RANGE_COOKIE = 12344;

  private static final int CARDINALITY_THRESHOLD = 12;
  private static final int EXPECTED_MAX_LENGTH = 48;

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    return new WrappedImmutableRoaringBitmap(((WrappedImmutableRoaringBitmap) super.makeImmutableBitmap(mutableBitmap)).getBitmap())
    {
      @Override
      public byte[] toBytes()
      {
        final ImmutableRoaringBitmap bitmap = getBitmap();
        final int cardinality = bitmap.getCardinality();
        if (cardinality > 1) {
          final int from = bitmap.getIntIterator().next();
          final int to = bitmap.getReverseIntIterator().next();
          if (to - from == cardinality - 1) {
            final BytesOutputStream out = new BytesOutputStream(Integer.BYTES * 3);
            out.writeInt(Integer.reverseBytes(RANGE_COOKIE));
            out.writeUnsignedVarInt(from);
            out.writeUnsignedVarInt(to - from);
            return out.toByteArray();
          }
        }
        if (cardinality < CARDINALITY_THRESHOLD) {
          final BytesOutputStream out = new BytesOutputStream(EXPECTED_MAX_LENGTH);
          out.writeInt(Integer.reverseBytes(SMALL_COOKIE | cardinality << 16));
          final IntIterator iterator = bitmap.getIntIterator();
          int prev = 0;
          while (iterator.hasNext()) {
            final int value = iterator.next();
            out.writeUnsignedVarInt(value - prev);    // write delta
            prev = value;
          }
          return out.toByteArray();
        }
        return super.toBytes();
      }
    };
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> bitmaps)
  {
    final Iterator<ImmutableBitmap> iterator = Iterables.filter(bitmaps, b -> !b.isEmpty()).iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = iterator.next();
    if (!iterator.hasNext()) {
      return first;
    }
    final BitSet bitSet = toBitset(first);
    while (iterator.hasNext()) {
      union(bitSet, iterator.next());
    }
    return from(bitSet);
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> bitmaps)
  {
    final Iterator<ImmutableBitmap> iterator = bitmaps.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = iterator.next();
    if (!iterator.hasNext()) {
      return first;
    }
    final BitSet bitSet = toBitset(first);
    while (iterator.hasNext() && !bitSet.isEmpty()) {
      intersect(bitSet, iterator.next());
    }
    return from(bitSet);
  }

  // basically for bound filter.. a contains b
  public ImmutableBitmap difference(ImmutableBitmap a, ImmutableBitmap b, int length)
  {
    return from(difference(toBitset(a), b));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b)
  {
    return super.complement(unwrapLazy(b));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
    if (length == 0) {
      return makeEmptyImmutableBitmap();
    }
    if (b.isEmpty()) {
      return from(length, new IntIterable.Range(0, length - 1));
    }
    return super.complement(unwrapLazy(b), length);
  }

  // seemed not effective for small number of large bitmaps
  public ImmutableBitmap _intersection(Iterable<ImmutableBitmap> bitmaps)
  {
    final Iterator<ImmutableBitmap> iterator = bitmaps.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = iterator.next();
    if (!iterator.hasNext()) {
      return first;
    }
    final BitSet bitSet = toBitset(first);
    while (iterator.hasNext() && !bitSet.isEmpty()) {
      final ImmutableBitmap bitmap = iterator.next();
      if (bitmap.isEmpty()) {
        return makeEmptyImmutableBitmap();
      }
      int last = -1;
      int next = bitSet.nextSetBit(0);
      final IntIterator values = bitmap.iterator();
      while (values.hasNext() && next >= 0) {
        final int x = values.next();
        if (x > next) {
          bitSet.clear(next, x);
          next = bitSet.nextSetBit(x + 1);
        } else if (x == next) {
          next = bitSet.nextSetBit(x + 1);
        }
        last = x;
      }
      if (bitSet.get(last)) {
        last++;
      }
      if (last < bitSet.size()) {
        bitSet.clear(last, bitSet.size());
      }
    }
    return finalize(bitSet);
  }

  private ImmutableBitmap finalize(final BitSet bitSet)
  {
    if (bitSet == null || bitSet.isEmpty()) {
      return makeEmptyImmutableBitmap();
    }
    return toBitmap(bitSet);
  }

  public static BitSet toBitset(final ImmutableBitmap bitmap)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      return ((LazyImmutableBitmap) bitmap).toBitSet();
    }
    return union(new BitSet(), bitmap.iterator());
  }

  public static ImmutableBitmap toBitmap(final BitSet bitSet)
  {
    return copyToBitmap(WrappedBitSetBitmap.iterator(bitSet));
  }

  private static BitSet union(final BitSet bitSet, final ImmutableBitmap bitmap)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      ((LazyImmutableBitmap) bitmap).or(bitSet);
    } else {
      union(bitSet, bitmap.iterator());
    }
    return bitSet;
  }

  private static BitSet union(final BitSet bitSet, final IntIterator iterator)
  {
    while (iterator.hasNext()) {
      bitSet.set(iterator.next());
    }
    return bitSet;
  }

  private static BitSet intersect(final BitSet bitSet, final ImmutableBitmap bitmap)
  {
    if (bitmap.isEmpty()) {
      bitSet.clear();
    } else if (bitmap instanceof LazyImmutableBitmap) {
      ((LazyImmutableBitmap) bitmap).and(bitSet);
    } else {
      intersect(bitSet, bitmap.iterator());
    }
    return bitSet;
  }

  private static BitSet intersect(final BitSet bitSet, final IntIterator iterator)
  {
    int prev = 0;
    while (iterator.hasNext()) {
      final int x = iterator.next();
      if (bitSet.get(x)) {
        bitSet.clear(prev, x);
        prev = x + 1;
      }
    }
    bitSet.clear(prev, bitSet.length());
    return bitSet;
  }

  private static BitSet difference(final BitSet bitSet, final ImmutableBitmap bitmap)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      ((LazyImmutableBitmap) bitmap).andNot(bitSet);
    } else {
      difference(bitSet, bitmap.iterator());
    }
    return bitSet;
  }

  private static void difference(final BitSet bitSet, final IntIterator iterator)
  {
    while (iterator.hasNext()) {
      bitSet.clear(iterator.next());
    }
  }

  // should return -1 instead of NoSuchElementException
  private static ImmutableBitmap copyToBitmap(final IntIterator iterator)
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    final MutableRoaringArray roaringArray = mutable.getMappeableRoaringArray();

    short current_hb = 0;
    int cardinality = 0;
    final BitSet values = new BitSet(0xFFFF);
    for (int x = iterator.next(); x >= 0; x = iterator.next()) {
      final short hb = RoaringUtils.highbits(x);
      if (hb != current_hb && !values.isEmpty()) {
        RoaringUtils.addContainer(roaringArray, current_hb, values, cardinality);
        values.clear();
        cardinality = 0;
      }
      current_hb = hb;
      values.set(x & 0xFFFF);
      cardinality++;
    }
    if (!values.isEmpty()) {
      RoaringUtils.addContainer(roaringArray, current_hb, values, cardinality);
    }
    return new WrappedImmutableRoaringBitmap(mutable);
  }

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer bbf)
  {
    final int position = bbf.position();
    final int magic = bbf.order(ByteOrder.LITTLE_ENDIAN).getInt(position);
    final int cookie = magic & 0xFFFF;
    if (cookie == RANGE_COOKIE) {
      bbf.position(position + 4);   // skip
      final int from = VLongUtils.readUnsignedVarInt(bbf);
      final int to = from + VLongUtils.readUnsignedVarInt(bbf);
      return from(to - from + 1, new IntIterable.Range(from, to));
    } else if (cookie == SMALL_COOKIE) {
      bbf.position(position + 4);   // skip
      final int size = magic >>> 16;
      if (size == 0) {
        return makeEmptyImmutableBitmap();
      }
      if (size == 1) {
        final int ix = VLongUtils.readUnsignedVarInt(bbf);
        return from(1, new IntIterable.Range(ix, ix));
      }
      final int[] indices = new int[size];
      for (int i = 0; i < indices.length; i++) {
        indices[i] = (i == 0 ? 0 : indices[i - 1]) + VLongUtils.readUnsignedVarInt(bbf);
      }
      return from(size, new IntIterable.FromArray(indices));
    }
    return new WrappedImmutableRoaringBitmap(new ImmutableRoaringBitmap(bbf));
  }

  private static ImmutableBitmap unwrapLazy(ImmutableBitmap bitmap)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      return ((LazyImmutableBitmap) bitmap).materializer.get();
    } else {
      return bitmap;
    }
  }

  public static LazyImmutableBitmap from(final int cardinality, final IntIterable iterable)
  {
    return new LazyImmutableBitmap(cardinality)
    {
      @Override
      public IntIterator iterator()
      {
        return iterable.iterator();
      }

      @Override
      public BitSet toBitSet()
      {
        final BitSet bitSet;
        if (iterable instanceof IntIterable.MaxAware) {
          bitSet = new BitSet(((IntIterable.MaxAware) iterable).max());
        } else {
          bitSet = new BitSet();
        }
        if (iterable instanceof IntIterable.Range) {
          ((IntIterable.Range) iterable).or(bitSet);
        } else {
          RoaringBitmapFactory.union(bitSet, iterator());
        }
        return bitSet;
      }

      @Override
      public boolean get(int index)
      {
        if (iterable instanceof IntIterable.Range) {
          return ((IntIterable.Range) iterable).get(index);
        } else if (iterable instanceof IntIterable.FromArray) {
          return ((IntIterable.FromArray) iterable).get(index);
        }
        return super.get(index);
      }

      @Override
      public void or(BitSet target)
      {
        if (iterable instanceof IntIterable.Range) {
          ((IntIterable.Range) iterable).or(target);
        } else {
          super.or(target);
        }
      }

      @Override
      public void and(BitSet target)
      {
        if (iterable instanceof IntIterable.Range) {
          ((IntIterable.Range) iterable).and(target);
        } else {
          super.and(target);
        }
      }

      @Override
      public void andNot(BitSet target)
      {
        if (iterable instanceof IntIterable.Range) {
          ((IntIterable.Range) iterable).andNot(target);
        } else {
          super.andNot(target);
        }
      }
    };
  }

  public static LazyImmutableBitmap from(final BitSet bitSet)
  {
    return new LazyImmutableBitmap(bitSet.cardinality())
    {
      @Override
      public IntIterator iterator()
      {
        return WrappedBitSetBitmap.iterator(bitSet);
      }

      @Override
      public BitSet toBitSet()
      {
        return (BitSet) bitSet.clone();
      }

      @Override
      public boolean get(int value)
      {
        return bitSet.get(value);
      }

      @Override
      public void or(BitSet target)
      {
        target.or(bitSet);
      }

      @Override
      public void and(BitSet target)
      {
        target.and(bitSet);
      }

      @Override
      public void andNot(BitSet target)
      {
        target.andNot(bitSet);
      }
    };
  }

  private static abstract class LazyImmutableBitmap implements ImmutableBitmap
  {
    private final int cardinality;
    private final Supplier<ImmutableBitmap> materializer;

    public LazyImmutableBitmap(int cardinality)
    {
      this.cardinality = cardinality;
      this.materializer = Suppliers.memoize(() -> copyToBitmap(iterator()));
    }

    @Override
    public int size()
    {
      return cardinality;
    }

    @Override
    public byte[] toBytes()
    {
      return materializer.get().toBytes();
    }

    @Override
    public int compareTo(ImmutableBitmap other)
    {
      return materializer.get().compareTo(other);
    }

    @Override
    public boolean isEmpty()
    {
      return cardinality == 0;
    }

    @Override
    public boolean get(int value)
    {
      return materializer.get().get(value);
    }

    @Override
    public ImmutableBitmap union(ImmutableBitmap otherBitmap)
    {
      return from(RoaringBitmapFactory.union(toBitSet(), otherBitmap));
    }

    @Override
    public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
    {
      return from(RoaringBitmapFactory.intersect(toBitSet(), otherBitmap));
    }

    @Override
    public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
    {
      return from(RoaringBitmapFactory.difference(toBitSet(), otherBitmap));
    }

    public BitSet toBitSet()
    {
      return RoaringBitmapFactory.union(new BitSet(), iterator());
    }

    public void or(BitSet target)
    {
      RoaringBitmapFactory.union(target, iterator());
    }

    public void and(BitSet target)
    {
      RoaringBitmapFactory.intersect(target, iterator());
    }

    public void andNot(BitSet target)
    {
      RoaringBitmapFactory.difference(target, iterator());
    }
  }
}
