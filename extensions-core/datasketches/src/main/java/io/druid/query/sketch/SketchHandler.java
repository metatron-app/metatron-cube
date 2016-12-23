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

import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.segment.column.BitmapIndex;

/**
 */
public interface SketchHandler
{
  Object calculate(int nomEntries, BitmapIndex bitmapIndex);

  Object newUnion(int nomEntries);

  void updateWithValue(Object union, String value);

  void updateWithSketch(Object union, Object sketch);

  Object toSketch(Object input);

  public static class Theta implements SketchHandler
  {
    @Override
    public CompactSketch calculate(int nomEntries, BitmapIndex bitmapIndex)
    {
      final Union union = newUnion(nomEntries);
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        union.update(Strings.nullToEmpty(bitmapIndex.getValue(i)));
      }
      return union.getResult();
    }

    @Override
    public Union newUnion(int nomEntries)
    {
      return (Union) SetOperation.builder().build(nomEntries, Family.UNION);
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

  public static class Quantile implements SketchHandler
  {
    @Override
    public Object calculate(int nomEntries, BitmapIndex bitmapIndex)
    {
      final ItemsSketch<String> histogram = ItemsSketch.getInstance(nomEntries, Ordering.natural());
      final int cardinality = bitmapIndex.getCardinality();
      for (int i = 0; i < cardinality; ++i) {
        final String value = Strings.nullToEmpty(bitmapIndex.getValue(i));
        for (int j = bitmapIndex.getBitmap(i).size(); j >= 0; j--) {
          histogram.update(value);
        }
      }
      return histogram;
    }

    @Override
    public ItemsUnion newUnion(int nomEntries)
    {
      return ItemsUnion.getInstance(nomEntries, Ordering.natural());
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
}
