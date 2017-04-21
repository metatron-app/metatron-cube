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

package io.druid.query.sketch;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.BitmapIndex;

import java.util.Arrays;
import java.util.List;

/**
 */
public interface SketchHandler
{
  Object calculate(
      int sketchParam,
      BitmapIndex bitmapIndex,
      Function<String, String> function
  );

  Object calculate(
      int sketchParam,
      BitmapIndex bitmapIndex,
      Function<String, String> function,
      ImmutableBitmap filter,
      BitmapIndexSelector selector
  );

  Object newUnion(int sketchParam);

  void updateWithValue(Object union, String value);

  void updateWithSketch(Object union, Object sketch);

  Object toSketch(Object input);

  public static class Theta implements SketchHandler
  {
    @Override
    public CompactSketch calculate(int sketchParam, BitmapIndex bitmapIndex, Function<String, String> function)
    {
      final Union union = newUnion(sketchParam);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        union.update(function.apply(bitmapIndex.getValue(i)));
      }
      return union.getResult();
    }

    @Override
    public Object calculate(
        int sketchParam,
        BitmapIndex bitmapIndex,
        Function<String, String> function,
        ImmutableBitmap filter,
        BitmapIndexSelector selector
    )
    {
      final Union union = newUnion(sketchParam);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        List<ImmutableBitmap> intersecting = Arrays.asList(bitmapIndex.getBitmap(i), filter);
        ImmutableBitmap bitmap = selector.getBitmapFactory().intersection(intersecting);
        if (bitmap.size() > 0) {
          union.update(function.apply(bitmapIndex.getValue(i)));
        }
      }
      return union.getResult();
    }

    @Override
    public Union newUnion(int sketchParam)
    {
      return (Union) SetOperation.builder().build(sketchParam, Family.UNION);
    }

    @Override
    public void updateWithValue(Object union, String value)
    {
      ((Union) union).update(value);
    }

    @Override
    public void updateWithSketch(Object union, Object sketch)
    {
      ((Union) union).update((Sketch) sketch);
    }

    @Override
    public Object toSketch(Object input)
    {
      if (input instanceof Union) {
        return ((Union) input).getResult();
      }
      return input;
    }
  }

  public abstract static class CardinalitySensitive<T> implements SketchHandler
  {
    @Override
    public final T calculate(int sketchParam, BitmapIndex bitmapIndex, Function<String, String> function)
    {
      final T histogram = newInstance(sketchParam);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        final String value = function.apply(bitmapIndex.getValue(i));
        update(histogram, value, bitmapIndex.getBitmap(i).size());
      }
      return histogram;
    }

    @Override
    public final T calculate(
        int sketchParam,
        BitmapIndex bitmapIndex,
        Function<String, String> function,
        ImmutableBitmap filter,
        BitmapIndexSelector selector
    )
    {
      final T histogram = newInstance(sketchParam);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        final List<ImmutableBitmap> intersecting = Arrays.asList(bitmapIndex.getBitmap(i), filter);
        final ImmutableBitmap bitmap = selector.getBitmapFactory().intersection(intersecting);
        if (bitmap.size() > 0) {
          final String value = function.apply(bitmapIndex.getValue(i));
          update(histogram, value, bitmap.size());
        }
      }
      return histogram;
    }

    protected abstract T newInstance(int sketchParam);

    protected abstract void update(T instance, String value, int count);
  }

  public static class Quantile extends CardinalitySensitive<ItemsSketch<String>>
  {
    @Override
    protected final ItemsSketch<String> newInstance(int sketchParam)
    {
      return ItemsSketch.getInstance(sketchParam, Ordering.natural());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final void update(ItemsSketch<String> sketch, String value, int count)
    {
      for (int i = 0; i < count; i++) {
        sketch.update(value);
      }
    }

    @Override
    public ItemsUnion<String> newUnion(int sketchParam)
    {
      return ItemsUnion.<String>getInstance(sketchParam, Ordering.natural());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithValue(Object union, String value)
    {
      ((ItemsUnion) union).update(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithSketch(Object union, Object sketch)
    {
      ((ItemsUnion) union).update((ItemsSketch) sketch);
    }

    @Override
    public Object toSketch(Object input)
    {
      if (input instanceof ItemsUnion) {
        return ((ItemsUnion) input).getResult();
      }
      return input;
    }
  }

  public static class Frequency extends CardinalitySensitive<com.yahoo.sketches.frequencies.ItemsSketch<String>>
  {
    @Override
    protected final com.yahoo.sketches.frequencies.ItemsSketch<String> newInstance(int sketchParam)
    {
      return newUnion(sketchParam);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final void update(com.yahoo.sketches.frequencies.ItemsSketch<String> instance, String value, int count)
    {
      instance.update(value, count);
    }

    @Override
    public com.yahoo.sketches.frequencies.ItemsSketch<String> newUnion(int sketchParam)
    {
      return new com.yahoo.sketches.frequencies.ItemsSketch<>(sketchParam);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithValue(Object union, String value)
    {
      ((com.yahoo.sketches.frequencies.ItemsSketch) union).update(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithSketch(Object union, Object sketch)
    {
      ((com.yahoo.sketches.frequencies.ItemsSketch) union).merge((com.yahoo.sketches.frequencies.ItemsSketch) sketch);
    }

    @Override
    public Object toSketch(Object input)
    {
      return input;
    }
  }

  public static class Sampling extends CardinalitySensitive<ReservoirItemsSketch<String>>
  {
    @Override
    protected final ReservoirItemsSketch<String> newInstance(int sketchParam)
    {
      return ReservoirItemsSketch.getInstance(sketchParam);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final void update(ReservoirItemsSketch<String> sketch, String value, int count)
    {
      for (int i = 0; i < count; i++) {
        sketch.update(value);
      }
    }

    @Override
    public ReservoirItemsUnion<String> newUnion(int sketchParam)
    {
      return ReservoirItemsUnion.getInstance(sketchParam);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithValue(Object union, String value)
    {
      ((ReservoirItemsSketch) union).update(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void updateWithSketch(Object union, Object sketch)
    {
      ((ReservoirItemsUnion) union).update((ReservoirItemsSketch) sketch);
    }

    @Override
    public Object toSketch(Object input)
    {
      if (input instanceof ReservoirItemsUnion) {
        return ((ReservoirItemsUnion) input).getResult();
      }
      return input;
    }
  }
}
