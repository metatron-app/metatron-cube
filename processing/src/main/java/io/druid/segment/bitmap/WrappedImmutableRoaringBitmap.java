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

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.QueryException;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringUtils;

import java.nio.ByteBuffer;

public class WrappedImmutableRoaringBitmap implements ExtendedBitmap
{
  /**
   * Underlying bitmap.
   */
  private final ImmutableRoaringBitmap bitmap;

  protected WrappedImmutableRoaringBitmap(ByteBuffer byteBuffer)
  {
    this.bitmap = new ImmutableRoaringBitmap(byteBuffer.asReadOnlyBuffer());
  }

  /**
   * Wrap an ImmutableRoaringBitmap
   *
   * @param immutableRoaringBitmap bitmap to be wrapped
   */
  public WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap immutableRoaringBitmap)
  {
    this.bitmap = immutableRoaringBitmap;
  }

  public ImmutableRoaringBitmap getBitmap()
  {
    return bitmap;
  }

  private static final int CARDINALITY_THRESHOLD = 12;
  private static final int EXPECTED_MAX_LENGTH = 48;

  @Override
  public byte[] toBytes()
  {
    final int cardinality = bitmap.getCardinality();
    if (cardinality > 1) {
      final int from = bitmap.getIntIterator().next();
      final int to = bitmap.getReverseIntIterator().next();
      if (to - from == cardinality - 1) {
        final BytesOutputStream out = new BytesOutputStream(Integer.BYTES * 3);
        out.writeInt(Integer.reverseBytes(RoaringBitmapFactory.RANGE_COOKIE));
        out.writeUnsignedVarInt(from);
        out.writeUnsignedVarInt(to - from);
        return out.toByteArray();
      }
    }
    if (cardinality < CARDINALITY_THRESHOLD) {
      final BytesOutputStream out = new BytesOutputStream(EXPECTED_MAX_LENGTH);
      out.writeInt(Integer.reverseBytes(RoaringBitmapFactory.SMALL_COOKIE | cardinality << 16));
      final IntIterator iterator = bitmap.getIntIterator();
      int prev = 0;
      while (iterator.hasNext()) {
        final int value = iterator.next();
        out.writeUnsignedVarInt(value - prev);    // write delta
        prev = value;
      }
      return out.toByteArray();
    }
    try {
      final BytesOutputStream out = new BytesOutputStream();
      bitmap.serialize(out);
      return out.toByteArray();
    }
    catch (Exception e) {
      throw QueryException.wrapIfNeeded(e);
    }
  }

  @Override
  public int compareTo(ImmutableBitmap other)
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return String.format("%s[%d]", getClass().getSimpleName(), bitmap.getCardinality());
  }

  @Override
  public IntIterator iterator()
  {
    return FromBatchIterator.of(bitmap);
  }

  @Override
  public int first()
  {
    return bitmap.first();
  }

  @Override
  public int last()
  {
    return bitmap.last();
  }

  @Override
  public IntIterator iterator(int offset)
  {
    return FromBatchIterator.of(bitmap, offset);
  }

  @Override
  public IntIterator iterator(int[] range)
  {
    return IntIterators.filter(FromBatchIterator.of(bitmap, range[0]), x -> x <= range[1]);
  }

  @Override
  public int cardinality(int[] range)
  {
    return RoaringUtils.cardinality(bitmap, range);
  }

  @Override
  public int size()
  {
    return bitmap.getCardinality();
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.isEmpty();
  }

  @Override
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableRoaringBitmap other = (WrappedImmutableRoaringBitmap) otherBitmap;
    ImmutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.or(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableRoaringBitmap other = (WrappedImmutableRoaringBitmap) otherBitmap;
    ImmutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.and(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableRoaringBitmap other = (WrappedImmutableRoaringBitmap) otherBitmap;
    ImmutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.andNot(bitmap, unwrappedOtherBitmap));
  }
}
