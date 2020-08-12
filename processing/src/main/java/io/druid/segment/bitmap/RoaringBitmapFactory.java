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
    final Iterator<ImmutableBitmap> iterator = bitmaps.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = iterator.next();
    if (!iterator.hasNext()) {
      return first;
    }
    final BitSet bitSet = copyTo(first, new BitSet());
    while (iterator.hasNext()) {
      copyTo(iterator.next(), bitSet);
    }
    if (bitSet.isEmpty()) {
      return makeEmptyImmutableBitmap();
    }
    return from(bitSet);
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b)
  {
    return super.intersection(Iterables.transform(b, bitmap -> unwrapLazy(bitmap)));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b)
  {
    return super.complement(unwrapLazy(b));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
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
    final BitSet bitSet = copyTo(first, new BitSet());
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

  private static BitSet copyTo(final ImmutableBitmap bitmap, final BitSet bitSet)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      ((LazyImmutableBitmap) bitmap).union(bitSet);
    } else {
      final IntIterator iterator = bitmap.iterator();
      while (iterator.hasNext()) {
        bitSet.set(iterator.next());
      }
    }
    return bitSet;
  }

  private ImmutableBitmap finalize(final BitSet bitSet)
  {
    if (bitSet == null || bitSet.isEmpty()) {
      return makeEmptyImmutableBitmap();
    }
    return copyToBitmap(WrappedBitSetBitmap.iterator(bitSet));
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
    final ByteBuffer buffer = bbf.order(ByteOrder.LITTLE_ENDIAN);
    final int cookie = buffer.getInt(buffer.position()) & 0xFFFF;
    if (cookie == RANGE_COOKIE) {
      final ByteBuffer bigEndian = buffer.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
      bigEndian.getInt();   // skip
      final int from = VLongUtils.readUnsignedVarInt(bigEndian);
      final int to = from + VLongUtils.readUnsignedVarInt(bigEndian);
      return from(to - from + 1, new IntIterators.Range(from, to));
    } else if (cookie == SMALL_COOKIE) {
      final ByteBuffer readOnly = buffer.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN); // order is not propagated
      final int size = readOnly.getInt() >>> 16;
      if (size == 0) {
        return makeEmptyImmutableBitmap();
      }
      final ByteBuffer bigEndian = readOnly.order(ByteOrder.BIG_ENDIAN);
      final int[] indices = new int[size];
      for (int i = 0; i < indices.length; i++) {
        indices[i] = (i == 0 ? 0 : indices[i - 1]) + VLongUtils.readUnsignedVarInt(bigEndian);
      }
      return from(size, new IntIterators.FromArray(indices));
    }
    return super.mapImmutableBitmap(bbf);
  }

  private static ImmutableBitmap unwrapLazy(ImmutableBitmap bitmap)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      return ((LazyImmutableBitmap) bitmap).materializer.get();
    } else {
      return bitmap;
    }
  }

  public static LazyImmutableBitmap from(final int cardinality, final IntIterator iterator)
  {
    return new LazyImmutableBitmap(cardinality)
    {
      @Override
      public IntIterator iterator()
      {
        return iterator.clone();
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
      public void union(BitSet target)
      {
        target.or(bitSet);
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
      return materializer.get().union(otherBitmap);
    }

    @Override
    public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
    {
      return materializer.get().intersection(otherBitmap);
    }

    @Override
    public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
    {
      return materializer.get().difference(otherBitmap);
    }

    public void union(BitSet target)
    {
      final IntIterator iterator = iterator();
      if (iterator instanceof IntIterators.Range) {
        ((IntIterators.Range) iterator).union(target);
      } else {
        while (iterator.hasNext()) {
          target.set(iterator.next());
        }
      }
    }
  }
}
