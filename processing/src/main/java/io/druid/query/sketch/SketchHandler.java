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

package io.druid.query.sketch;

import com.metamx.collections.bitmap.ImmutableBitmap;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.segment.column.BitmapIndex;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public interface SketchHandler<U>
{
  SketchOp op();

  TypedSketch<U> calculate(
      int sketchParam,
      BitmapIndex bitmapIndex,
      ExtractionFn function
  );

  TypedSketch<U> calculate(
      int sketchParam,
      BitmapIndex bitmapIndex,
      ExtractionFn function,
      ImmutableBitmap filter,
      BitmapIndexSelector selector
  );

  boolean supports(ValueDesc type);

  TypedSketch<U> newUnion(int sketchParam, ValueDesc type, Comparator comparator);

  void updateWithValue(TypedSketch<U> union, Object value);

  void updateWithSketch(TypedSketch<U> union, Object sketch);

  void reset(TypedSketch<U> union);

  TypedSketch toSketch(TypedSketch<U> input);

  public static class Synchronized<X> implements SketchHandler<X>
  {
    private final SketchHandler<X> handler;

    public Synchronized(SketchHandler<X> handler) {this.handler = handler;}

    @Override
    public SketchOp op()
    {
      return handler.op();
    }

    @Override
    public synchronized TypedSketch<X> calculate(int sketchParam, BitmapIndex bitmapIndex, ExtractionFn function)
    {
      return handler.calculate(sketchParam, bitmapIndex, function);
    }

    @Override
    public synchronized TypedSketch<X> calculate(
        int sketchParam,
        BitmapIndex bitmapIndex,
        ExtractionFn function,
        ImmutableBitmap filter,
        BitmapIndexSelector selector
    )
    {
      return handler.calculate(sketchParam, bitmapIndex, function, filter, selector);
    }

    @Override
    public synchronized boolean supports(ValueDesc type)
    {
      return handler.supports(type);
    }

    @Override
    public synchronized TypedSketch<X> newUnion(int sketchParam, ValueDesc type, Comparator comparator)
    {
      return handler.newUnion(sketchParam, type, comparator);
    }

    @Override
    public synchronized void updateWithValue(TypedSketch<X> union, Object value)
    {
      handler.updateWithValue(union, value);
    }

    @Override
    public synchronized void updateWithSketch(TypedSketch<X> union, Object sketch)
    {
      handler.updateWithSketch(union, sketch);
    }

    @Override
    public synchronized void reset(TypedSketch<X> union)
    {
      handler.reset(union);
    }

    @Override
    public synchronized TypedSketch toSketch(TypedSketch<X> input)
    {
      return handler.toSketch(input);
    }
  }

  public static class Theta implements SketchHandler<Union>
  {
    @Override
    public SketchOp op()
    {
      return SketchOp.THETA;
    }

    @Override
    public TypedSketch<Union> calculate(int sketchParam, BitmapIndex bitmapIndex, ExtractionFn function)
    {
      final TypedSketch<Union> union = newUnion(sketchParam, ValueDesc.STRING, null);
      final Union sketch = union.value();
      final int cardinality = bitmapIndex.getCardinality();
      if (function == null) {
        for (int i = 0; i < cardinality; ++i) {
          sketch.update(bitmapIndex.getValueAsRaw(i));
        }
      } else {
        for (int i = 0; i < cardinality; i++) {
          sketch.update(function.apply(bitmapIndex.getValue(i)));
        }
      }
      return union;
    }

    @Override
    public TypedSketch<Union> calculate(
        int sketchParam,
        BitmapIndex bitmapIndex,
        ExtractionFn function,
        ImmutableBitmap filter,
        BitmapIndexSelector selector
    )
    {
      final TypedSketch<Union> union = newUnion(sketchParam, ValueDesc.STRING, null);
      final int cardinality = bitmapIndex.getCardinality();
      if (function == null) {
        for (int i = 0; i < cardinality; ++i) {
          List<ImmutableBitmap> intersecting = Arrays.asList(bitmapIndex.getBitmap(i), filter);
          ImmutableBitmap bitmap = DimFilters.intersection(selector.getBitmapFactory(), intersecting);
          if (!bitmap.isEmpty()) {
            union.value().update(bitmapIndex.getValueAsRaw(i));
          }
        }
      } else {
        for (int i = 0; i < cardinality; ++i) {
          List<ImmutableBitmap> intersecting = Arrays.asList(bitmapIndex.getBitmap(i), filter);
          ImmutableBitmap bitmap = DimFilters.intersection(selector.getBitmapFactory(), intersecting);
          if (!bitmap.isEmpty()) {
            union.value().update(function.apply(bitmapIndex.getValue(i)));
          }
        }
      }
      return union;
    }

    @Override
    public boolean supports(ValueDesc type)
    {
      return type.isPrimitive();
    }

    @Override
    public TypedSketch<Union> newUnion(int sketchParam, ValueDesc type, Comparator comparator)
    {
      return TypedSketch.of(type, (Union) SetOperation.builder().setNominalEntries(sketchParam).build(Family.UNION));
    }

    @Override
    public void updateWithValue(TypedSketch<Union> union, Object value)
    {
      switch (union.type().type()) {
        case STRING:
          union.value().update((String) value);
          break;
        case FLOAT:
          union.value().update(((Number) value).floatValue());
          break;
        case DOUBLE:
          union.value().update(((Number) value).doubleValue());
          break;
        case LONG:
          union.value().update(((Number) value).longValue());
          break;
        default:
          throw new IllegalArgumentException("not supported type " + union.type());
      }
    }

    @Override
    public void updateWithSketch(TypedSketch<Union> union, Object sketch)
    {
      union.value().update((Sketch) sketch);
    }

    @Override
    public void reset(TypedSketch<Union> union)
    {
      union.value().reset();
    }

    @Override
    public TypedSketch<Sketch> toSketch(TypedSketch<Union> input)
    {
      return input == null ? null : TypedSketch.of(input.type(), input.value().getResult());
    }
  }

  public abstract static class CardinalitySensitive<U> implements SketchHandler<U>
  {
    @Override
    public boolean supports(ValueDesc type)
    {
//      return type != ValueType.COMPLEX || Comparable.class.isAssignableFrom(type.classOfObject());
      return type.isPrimitive();
    }

    @Override
    public final TypedSketch<U> calculate(int sketchParam, BitmapIndex bitmapIndex, ExtractionFn function)
    {
      final ExtractionFn extraction = function == null ? IdentityExtractionFn.nullToEmpty() : function;
      final TypedSketch<U> union = newUnion(sketchParam, ValueDesc.STRING, null);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        final String value = extraction.apply(bitmapIndex.getValue(i));
        update(union, value, bitmapIndex.getBitmap(i).size());
      }
      return union;
    }

    @Override
    public final TypedSketch<U> calculate(
        int sketchParam,
        BitmapIndex bitmapIndex,
        ExtractionFn function,
        ImmutableBitmap filter,
        BitmapIndexSelector selector
    )
    {
      final ExtractionFn extraction = function == null ? IdentityExtractionFn.nullToEmpty() : function;
      final TypedSketch<U> union = newUnion(sketchParam, ValueDesc.STRING, null);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        final List<ImmutableBitmap> intersecting = Arrays.asList(bitmapIndex.getBitmap(i), filter);
        final ImmutableBitmap bitmap = DimFilters.intersection(selector.getBitmapFactory(), intersecting);
        if (!bitmap.isEmpty()) {
          final String value = extraction.apply(bitmapIndex.getValue(i));
          update(union, value, bitmap.size());
        }
      }
      return union;
    }

    protected abstract void update(TypedSketch<U> instance, Object value, int count);
  }

  public static class Quantile extends CardinalitySensitive<ItemsUnion>
  {
    @Override
    public SketchOp op()
    {
      return SketchOp.QUANTILE;
    }

    @Override
    public boolean supports(ValueDesc type)
    {
      return type.isPrimitive() || type.isStruct();
    }

    @Override
    public TypedSketch<ItemsUnion> newUnion(int sketchParam, ValueDesc type, Comparator comparator)
    {
      comparator = comparator == null ? type.comparator() : comparator;
      return TypedSketch.of(type, ItemsUnion.getInstance(sketchParam, comparator));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final void update(TypedSketch<ItemsUnion> sketch, Object value, int count)
    {
      for (int i = 0; i < count; i++) {
        sketch.value().update(value);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithValue(TypedSketch<ItemsUnion> union, Object value)
    {
      union.value().update(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithSketch(TypedSketch<ItemsUnion> union, Object sketch)
    {
      union.value().update((ItemsSketch) sketch);
    }

    @Override
    public void reset(TypedSketch<ItemsUnion> union)
    {
      union.value().reset();
    }

    @Override
    public TypedSketch<ItemsSketch> toSketch(TypedSketch<ItemsUnion> input)
    {
      return input == null ? null : TypedSketch.of(input.type(), input.value().getResult());
    }
  }

  public static class Frequency extends CardinalitySensitive<com.yahoo.sketches.frequencies.ItemsSketch>
  {
    @Override
    @SuppressWarnings("unchecked")
    protected final void update(TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> instance, Object value, int count)
    {
      instance.value().update(value, count);
    }

    @Override
    public SketchOp op()
    {
      return SketchOp.FREQUENCY;
    }

    @Override
    public TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> newUnion(
        int sketchParam,
        ValueDesc type,
        Comparator comparator
    )
    {
      return TypedSketch.of(type, new com.yahoo.sketches.frequencies.ItemsSketch(sketchParam));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithValue(TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> union, Object value)
    {
      union.value().update(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithSketch(TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> union, Object sketch)
    {
      union.value().merge((com.yahoo.sketches.frequencies.ItemsSketch) sketch);
    }

    @Override
    public void reset(TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> union)
    {
      union.value().reset();
    }

    @Override
    public TypedSketch toSketch(TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> input)
    {
      return input;
    }
  }

  public static class Sampling extends CardinalitySensitive<ReservoirItemsUnion>
  {
    @Override
    @SuppressWarnings("unchecked")
    protected final void update(TypedSketch<ReservoirItemsUnion> sketch, Object value, int count)
    {
      for (int i = 0; i < count; i++) {
        sketch.value().update(value);
      }
    }

    @Override
    public SketchOp op()
    {
      return SketchOp.SAMPLING;
    }

    @Override
    public TypedSketch<ReservoirItemsUnion> newUnion(int sketchParam, ValueDesc type, Comparator comparator)
    {
      return TypedSketch.of(type, ReservoirItemsUnion.newInstance(sketchParam));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithValue(TypedSketch<ReservoirItemsUnion> union, Object value)
    {
      union.value().update(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithSketch(TypedSketch<ReservoirItemsUnion> union, Object sketch)
    {
      union.value().update((ReservoirItemsSketch) sketch);
    }

    @Override
    public void reset(TypedSketch<ReservoirItemsUnion> union)
    {
      throw new UnsupportedOperationException("reset"); // -_-+
    }

    @Override
    public TypedSketch toSketch(TypedSketch<ReservoirItemsUnion> union)
    {
      return union == null ? null : TypedSketch.of(union.type(), union.value().getResult());
    }
  }
}
