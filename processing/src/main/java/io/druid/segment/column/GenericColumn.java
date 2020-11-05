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

package io.druid.segment.column;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.collections.ResourceHolder;
import io.druid.data.ValueDesc;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.serde.ComplexMetricSerde;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Objects;
import java.util.function.DoublePredicate;
import java.util.function.LongPredicate;

/**
 */
public interface GenericColumn extends ComplexColumn
{
  int DEFAULT_PREFETCH = 32;

  default String getString(int rowNum) { throw new UnsupportedOperationException();}

  default Float getFloat(int rowNum) { throw new UnsupportedOperationException();}

  default Long getLong(int rowNum) { throw new UnsupportedOperationException();}

  default Double getDouble(int rowNum) { throw new UnsupportedOperationException();}

  default Boolean getBoolean(int rowNum) { throw new UnsupportedOperationException();}

  default ImmutableBitmap getNulls() { return null;}

  @Override
  default void close() throws IOException {}

  abstract class LongType implements GenericColumn
  {
    @Override
    public final ValueDesc getType()
    {
      return ValueDesc.LONG;
    }

    @Override
    public final Boolean getBoolean(int rowNum)
    {
      final Long value = getValue(rowNum);
      return value == null ? null : value != 0;
    }

    @Override
    public final String getString(int rowNum)
    {
      return Objects.toString(getValue(rowNum), null);
    }

    @Override
    public final Double getDouble(int rowNum)
    {
      final Long value = getValue(rowNum);
      return value == null ? null : value.doubleValue();
    }

    @Override
    public final Float getFloat(int rowNum)
    {
      final Long value = getValue(rowNum);
      return value == null ? null : value.floatValue();
    }

    @Override
    public final Long getLong(int rowNum)
    {
      return getValue(rowNum);
    }

    @Override
    public abstract Long getValue(int rowNum);

    public abstract void scan(IntIterator include, LongScanner scanner);

    public ImmutableBitmap collect(
        final BitmapFactory factory,
        final IntIterator iterator,
        final LongPredicate predicate
    )
    {
      final ImmutableBitmap nulls = getNulls();
      final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
      if (nulls.isEmpty()) {
        scan(iterator, (x, f) -> { if (predicate.test(f.get(x))) { bitmap.add(x); } });
      } else {
        scan(iterator, (x, f) -> { if (!nulls.get(x) && predicate.test(f.get(x))) { bitmap.add(x); } });
      }
      return factory.makeImmutableBitmap(bitmap);
    }
  }

  abstract class FloatType implements GenericColumn
  {
    @Override
    public final ValueDesc getType()
    {
      return ValueDesc.FLOAT;
    }

    @Override
    public final Boolean getBoolean(int rowNum)
    {
      final Float value = getValue(rowNum);
      return value == null ? null : value != 0;
    }

    @Override
    public final String getString(int rowNum)
    {
      return Objects.toString(getValue(rowNum), null);
    }

    @Override
    public final Double getDouble(int rowNum)
    {
      final Float value = getValue(rowNum);
      return value == null ? null : value.doubleValue();
    }

    @Override
    public final Float getFloat(int rowNum)
    {
      return getValue(rowNum);
    }

    @Override
    public final Long getLong(int rowNum)
    {
      final Float value = getValue(rowNum);
      return value == null ? null : value.longValue();
    }

    @Override
    public abstract Float getValue(int rowNum);

    public abstract void scan(IntIterator iterator, FloatScanner scanner);

    public ImmutableBitmap collect(
        final BitmapFactory factory,
        final IntIterator iterator,
        final FloatPredicate predicate
    )
    {
      final ImmutableBitmap nulls = getNulls();
      final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
      if (nulls.isEmpty()) {
        scan(iterator, (x, f) -> { if (predicate.test(f.get(x))) { bitmap.add(x); } });
      } else {
        scan(iterator, (x, f) -> { if (!nulls.get(x) && predicate.test(f.get(x))) { bitmap.add(x); } });
      }
      return factory.makeImmutableBitmap(bitmap);
    }
  }

  abstract class DoubleType implements GenericColumn
  {
    @Override
    public final ValueDesc getType()
    {
      return ValueDesc.DOUBLE;
    }

    @Override
    public final Boolean getBoolean(int rowNum)
    {
      final Double value = getValue(rowNum);
      return value == null ? null : value != 0;
    }

    @Override
    public final String getString(int rowNum)
    {
      return Objects.toString(getValue(rowNum), null);
    }

    @Override
    public final Float getFloat(int rowNum)
    {
      final Double value = getValue(rowNum);
      return value == null ? null : value.floatValue();
    }

    @Override
    public final Double getDouble(int rowNum)
    {
      return getValue(rowNum);
    }

    @Override
    public final Long getLong(int rowNum)
    {
      final Double value = getValue(rowNum);
      return value == null ? null : value.longValue();
    }

    @Override
    public abstract Double getValue(int rowNum);

    public abstract void scan(IntIterator iterator, DoubleScanner scanner);

    public ImmutableBitmap collect(
        final BitmapFactory factory,
        final IntIterator iterator,
        final DoublePredicate predicate
    )
    {
      final ImmutableBitmap nulls = getNulls();
      final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
      if (nulls.isEmpty()) {
        scan(iterator, (x, f) -> { if (predicate.test(f.get(x))) { bitmap.add(x); } });
      } else {
        scan(iterator, (x, f) -> { if (!nulls.get(x) && predicate.test(f.get(x))) { bitmap.add(x); } });
      }
      return factory.makeImmutableBitmap(bitmap);
    }
  }

  abstract class Compressed extends ComplexColumn.Compressed implements GenericColumn
  {
    protected Compressed(
        ComplexMetricSerde serde,
        int[] mapping,
        ShortBuffer offsets,
        GenericIndexed<ResourceHolder<ByteBuffer>> indexed,
        CompressionStrategy compression
    )
    {
      super(serde, mapping, offsets, indexed, compression);
    }
  }
}
