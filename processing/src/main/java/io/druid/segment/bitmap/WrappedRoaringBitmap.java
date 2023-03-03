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
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.query.QueryException;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class WrappedRoaringBitmap implements MutableBitmap
{
  /**
   * Underlying bitmap.
   */
  private final MutableRoaringBitmap bitmap;

  // attempt to compress long runs prior to serialization (requires RoaringBitmap version 0.5 or better)
  // this may improve compression greatly in some cases at the expense of slower serialization
  // in the worst case.
  private final boolean compressRunOnSerialization;

  /**
   * Creates a new WrappedRoaringBitmap wrapping an empty MutableRoaringBitmap
   */
  public WrappedRoaringBitmap()
  {
    this(RoaringBitmapFactory.DEFAULT_COMPRESS_RUN_ON_SERIALIZATION);
  }

  /**
   * Creates a new WrappedRoaringBitmap wrapping an empty MutableRoaringBitmap
   *
   * @param compressRunOnSerialization indicates whether to call {@link RoaringBitmap#runOptimize()} before serializing
   */
  public WrappedRoaringBitmap(boolean compressRunOnSerialization)
  {
    this.bitmap = new MutableRoaringBitmap();
    this.compressRunOnSerialization = compressRunOnSerialization;
  }

  WrappedImmutableRoaringBitmap toImmutableBitmap()
  {
    MutableRoaringBitmap mrb = bitmap.clone();
    if (compressRunOnSerialization) {
      mrb.runOptimize();
    }
    return new WrappedImmutableRoaringBitmap(mrb);
  }

  @Override
  public byte[] toBytes()
  {
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      if (compressRunOnSerialization) {
        bitmap.runOptimize();
      }
      bitmap.serialize(new DataOutputStream(out));
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
  public void clear()
  {
    bitmap.clear();
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.or(unwrappedOtherBitmap);
  }

  @Override
  public void and(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.and(unwrappedOtherBitmap);
  }


  @Override
  public void andNot(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.andNot(unwrappedOtherBitmap);
  }


  @Override
  public void xor(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.xor(unwrappedOtherBitmap);
  }

  @Override
  public int getSizeInBytes()
  {
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    return bitmap.serializedSizeInBytes();
  }

  @Override
  public void add(int entry)
  {
    bitmap.add(entry);
  }

  @Override
  public int size()
  {
    return bitmap.getCardinality();
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }

  @Override
  public void serialize(final ByteBuffer buffer)
  {
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    try {
      bitmap.serialize(
          new DataOutputStream(
              new OutputStream()
              {
                @Override
                public void write(int b)
                {
                  buffer.put((byte) b);
                }

                @Override
                public void write(byte[] b)
                {
                  buffer.put(b);
                }

                @Override
                public void write(byte[] b, int off, int l)
                {
                  buffer.put(b, off, l);
                }
              }
          )
      );
    }
    catch (Exception e) {
      throw QueryException.wrapIfNeeded(e);
    }
  }

  @Override
  public String toString()
  {
    return String.format("%s[%d]", getClass().getSimpleName(), bitmap.getCardinality());
  }

  @Override
  public void remove(int entry)
  {
    bitmap.remove(entry);
  }

  @Override
  public IntIterator iterator()
  {
    return FromBatchIterator.of(bitmap);
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.isEmpty();
  }

  @Override
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.or(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.and(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.andNot(bitmap, unwrappedOtherBitmap));
  }
}