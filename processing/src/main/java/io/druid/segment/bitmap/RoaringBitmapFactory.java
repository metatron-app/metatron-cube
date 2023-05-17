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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.data.VLongUtils;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryException;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringArray;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;

// default implementation of union/intersection makes a lot of copy, which is not necessary for our use case
// simply using bitset and return it as roaring bitmap out-performs in the most real use-cases (it's worse in single threaded micro test)
public final class RoaringBitmapFactory implements BitmapFactory
{
  private static final Logger LOG = new Logger(RoaringBitmapFactory.class);

  static final boolean DEFAULT_COMPRESS_RUN_ON_SERIALIZATION = false;

  private static final WrappedImmutableRoaringBitmap EMPTY_IMMUTABLE_BITMAP;

  static {
    try {
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      roaringBitmap.serialize(new DataOutputStream(out));

      ImmutableRoaringBitmap empty = new ImmutableRoaringBitmap(ByteBuffer.wrap(out.toByteArray()));
      EMPTY_IMMUTABLE_BITMAP = new WrappedImmutableRoaringBitmap(empty)
      {
        @Override
        public int size() {return 0;}
        @Override
        public boolean get(int value) {return false;}
      };
    }
    catch (Exception e) {
      throw QueryException.wrapIfNeeded(e);
    }
  }

  private final boolean compressRunOnSerialization;

  public RoaringBitmapFactory()
  {
    this(DEFAULT_COMPRESS_RUN_ON_SERIALIZATION);
  }

  public RoaringBitmapFactory(boolean compressRunOnSerialization)
  {
    this.compressRunOnSerialization = compressRunOnSerialization;
  }

  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return new WrappedRoaringBitmap(compressRunOnSerialization);
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return EMPTY_IMMUTABLE_BITMAP;
  }

  // first set bit
  public static int firstOf(ImmutableBitmap b)
  {
    Preconditions.checkArgument(!b.isEmpty());
    if (b instanceof WrappedImmutableRoaringBitmap) {
      return unwrap(b).first();
    } else if (b instanceof LazyImmutableBitmap) {
      return ((LazyImmutableBitmap) b).first();
    } else if (b instanceof WrappedBitSetBitmap) {
      return ((WrappedBitSetBitmap) b).bitset().nextSetBit(0);
    }
    return -1;
  }

  // last set bit
  public static int lastOf(ImmutableBitmap b)
  {
    Preconditions.checkArgument(!b.isEmpty());
    if (b instanceof WrappedImmutableRoaringBitmap) {
      return unwrap(b).last();
    } else if (b instanceof LazyImmutableBitmap) {
      return ((LazyImmutableBitmap) b).last();
    } else if (b instanceof WrappedBitSetBitmap) {
      return ((WrappedBitSetBitmap) b).bitset().length() - 1;
    }
    return -1;
  }

  private static ImmutableRoaringBitmap unwrap(ImmutableBitmap b)
  {
    return ((WrappedImmutableRoaringBitmap) b).getBitmap();
  }

  private static Iterable<ImmutableRoaringBitmap> unwrap(final Iterable<ImmutableBitmap> b)
  {
    return () -> Iterators.transform(b.iterator(), RoaringBitmapFactory::unwrap);
  }

  private static WrappedImmutableRoaringBitmap _makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    if (!(mutableBitmap instanceof WrappedRoaringBitmap)) {
      throw new IllegalStateException(String.format("Cannot convert [%s]", mutableBitmap.getClass()));
    }
    try {
      return ((WrappedRoaringBitmap) mutableBitmap).toImmutableBitmap();
    }
    catch (Exception e) {
      throw QueryException.wrapIfNeeded(e);
    }
  }

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    return new WrappedImmutableRoaringBitmap(_makeImmutableBitmap(mutableBitmap).getBitmap());
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> bitmaps)
  {
    // do it with single iteration
    final BitSet bitSet = new BitSet(0);
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    for (ImmutableBitmap bitmap : bitmaps) {
      if (bitmap instanceof LazyImmutableBitmap) {
        ((LazyImmutableBitmap) bitmap).or(bitSet);
      } else {
        RoaringUtils.lazyor(answer, unwrap(bitmap));
      }
    }
    if (answer.isEmpty()) {
      return bitSet.isEmpty() ? EMPTY_IMMUTABLE_BITMAP : from(bitSet);
    }
    ImmutableBitmap immutable = new WrappedImmutableRoaringBitmap(RoaringUtils.repair(answer));
    if (!bitSet.isEmpty()) {
      immutable = from(union(bitSet, immutable));
    }
    return immutable;
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> bitmaps)
  {
    if (!(Iterables.any(bitmaps, b -> b instanceof LazyImmutableBitmap))) {
      return new WrappedImmutableRoaringBitmap(BufferFastAggregation.and(unwrap(bitmaps).iterator()));
    }
    Iterator<ImmutableBitmap> it = bitmaps.iterator();
    if (!it.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = it.next();
    if (!it.hasNext()) {
      return first;
    }
    final BitSet bitSet = toBitset(first);
    while (it.hasNext()) {
      intersect(bitSet, it.next());
    }
    return from(bitSet);
  }

  // basically for bound filter.. a contains b
  public ImmutableBitmap difference(ImmutableBitmap a, ImmutableBitmap b, int length)
  {
    if (a instanceof LazyImmutableBitmap || b instanceof LazyImmutableBitmap) {
      return from(difference(toBitset(a), b));
    }
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.andNot(unwrap(a), unwrap(b), 0L, length));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b)
  {
    throw new UOE("complement without length");
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
    if (length == 0) {
      return makeEmptyImmutableBitmap();
    }
    if (b.isEmpty()) {
      return from(0, length);
    }
    if (b instanceof LazyImmutableBitmap) {
      BitSet bitset = ((LazyImmutableBitmap)b).toBitSet();
      bitset.flip(0, length);
      return from(bitset);
    }
    return _complement(b, length);
  }

  private ImmutableBitmap _complement(ImmutableBitmap b, int length)
  {
    return new WrappedImmutableRoaringBitmap(
        ImmutableRoaringBitmap.flip(((WrappedImmutableRoaringBitmap) b).getBitmap(), 0L, length)
    );
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
    return _union(new BitSet(), bitmap.iterator());
  }

  public static ImmutableBitmap toBitmap(final BitSet bitSet)
  {
    return copyToBitmap(BitSets.iterator(bitSet));
  }

  private static BitSet union(final BitSet bitSet, final ImmutableBitmap bitmap)
  {
    if (bitmap instanceof LazyImmutableBitmap) {
      ((LazyImmutableBitmap) bitmap).or(bitSet);
    } else {
      _union(bitSet, bitmap.iterator());
    }
    return bitSet;
  }

  private static BitSet _union(final BitSet bitSet, final IntIterator iterator)
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
      _intersect(bitSet, bitmap.iterator());
    }
    return bitSet;
  }

  private static BitSet _intersect(final BitSet bitSet, final IntIterator iterator)
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
      _difference(bitSet, bitmap.iterator());
    }
    return bitSet;
  }

  private static void _difference(final BitSet bitSet, final IntIterator iterator)
  {
    while (iterator.hasNext()) {
      bitSet.clear(iterator.next());
    }
  }

  // should return -1 instead of NoSuchElementException
  public static WrappedImmutableRoaringBitmap copyToBitmap(final IntIterator iterator)
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    final MutableRoaringArray roaringArray = mutable.getMappeableRoaringArray();

    char current_hb = 0;
    int cardinality = 0;
    final BitSet values = new BitSet(0xFFFF);
    for (int x = iterator.next(); x >= 0; x = iterator.next()) {
      final char hb = RoaringUtils.highbits(x);
      if (hb != current_hb && !values.isEmpty()) {
        RoaringUtils.addContainer(roaringArray, current_hb, values, cardinality);
        values.clear();
        cardinality = 0;
      }
      current_hb = hb;
      values.set(RoaringUtils.lowbits(x));
      cardinality++;
    }
    if (!values.isEmpty()) {
      RoaringUtils.addContainer(roaringArray, current_hb, values, cardinality);
    }
    return new WrappedImmutableRoaringBitmap(mutable);
  }

  private static final short SERIAL_COOKIE = 12347;
  private static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;

  public static final short SMALL_COOKIE = 12345;
  public static final short RANGE_COOKIE = 12344;

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer bbf)
  {
    return mapImmutableBitmap(bbf, bbf.position(), bbf.remaining());
  }

  public ImmutableBitmap mapImmutableBitmap(ByteBuffer bbf, int offset, int length)
  {
    final int magic = VLongUtils.readInt(bbf, offset, ByteOrder.LITTLE_ENDIAN);
    final int cookie = magic & 0xFFFF;
    if (cookie == RANGE_COOKIE) {
      offset += Integer.BYTES;
      final int from = VLongUtils.readUnsignedVarInt(bbf, offset);
      offset += VLongUtils.sizeOfUnsignedVarInt(from);
      final int to = from + VLongUtils.readUnsignedVarInt(bbf, offset); // inclusive
      return from(from, to + 1);
    } else if (cookie == SMALL_COOKIE) {
      offset += Integer.BYTES;
      final int size = magic >>> 16;
      if (size == 0) {
        return makeEmptyImmutableBitmap();
      }
      if (size == 1) {
        return of(VLongUtils.readUnsignedVarInt(bbf, offset));
      }
      final int[] indices = new int[size];
      for (int i = 0; i < indices.length; i++) {
        int x = VLongUtils.readUnsignedVarInt(bbf, offset);
        indices[i] = (i == 0 ? 0 : indices[i - 1]) + x;
        offset += VLongUtils.sizeOfUnsignedVarInt(x);
      }
      return from(indices);
    }
    final int limit = bbf.limit();
    bbf.limit(bbf.position() + length);
    try {
      return new WrappedImmutableRoaringBitmap(new ImmutableRoaringBitmap(bbf));
    }
    finally {
      bbf.limit(limit);
    }
  }

  // inclusive ~ exclusive
  public static LazyImmutableBitmap from(int from, int to)
  {
    Preconditions.checkArgument(from <= to, "invalid range %d ~ %d", from, to);
    if (from == to) {
      return from(0, IntIterable.EMPTY);
    }
    return from(to - from, new IntIterable.Range(from, to));
  }

  public static LazyImmutableBitmap of(int index)
  {
    return index < 0 ? from(0, IntIterable.EMPTY) : from(new int[]{index});
  }

  public static LazyImmutableBitmap from(int[] indices)
  {
    return indices.length == 0 ? from(0, IntIterable.EMPTY) : from(indices.length, new IntIterable.FromArray(indices));
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
        if (iterable instanceof IntIterable.MinMaxAware) {
          bitSet = new BitSet(((IntIterable.MinMaxAware) iterable).max());
        } else {
          bitSet = new BitSet();
        }
        if (iterable instanceof IntIterable.Range) {
          ((IntIterable.Range) iterable).or(bitSet);
        } else {
          RoaringBitmapFactory._union(bitSet, iterator());
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
      public int first()
      {
        if (iterable instanceof IntIterable.MinMaxAware) {
          return ((IntIterable.MinMaxAware) iterable).min();
        }
        return -1;
      }

      @Override
      public int last()
      {
        if (iterable instanceof IntIterable.MinMaxAware) {
          return ((IntIterable.MinMaxAware) iterable).max();
        }
        return -1;
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
        return BitSets.iterator(bitSet);
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
      public int first()
      {
        return bitSet.nextSetBit(0);
      }

      @Override
      public int last()
      {
        return bitSet.length() - 1;
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

  public static abstract class LazyImmutableBitmap implements ImmutableBitmap
  {
    private final int cardinality;
    private final Supplier<WrappedImmutableRoaringBitmap> materializer;

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

    public abstract int first();

    public abstract int last();

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
      return RoaringBitmapFactory._union(new BitSet(), iterator());
    }

    public void or(BitSet target)
    {
      RoaringBitmapFactory._union(target, iterator());
    }

    public void and(BitSet target)
    {
      RoaringBitmapFactory._intersect(target, iterator());
    }

    public void andNot(BitSet target)
    {
      RoaringBitmapFactory._difference(target, iterator());
    }
  }
}
