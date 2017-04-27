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

package io.druid.segment.filter;

import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.IndexedInts;

/**
 */
public class SelectorFilter extends Filter.WithDictionary
{
  private final String dimension;
  private final String value;

  public SelectorFilter(
      String dimension,
      String value
  )
  {
    this.dimension = dimension;
    this.value = value;
  }

  @Override
  public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
  {
    MutableBitmap bitmap = selector.getBitmapFactory().makeEmptyMutableBitmap();
    BitmapIndex indexed = selector.getBitmapIndex(dimension);
    int index = indexed.getIndex(value);
    if (index >= 0) {
      bitmap.add(index);
    }
    return selector.getBitmapFactory().makeImmutableBitmap(bitmap);
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension, value);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, value);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    final ObjectColumnSelector selector = columnSelectorFactory.makeObjectColumnSelector(dimension);
    if (selector.classOfObject() == IndexedInts.WithLookup.class) {
      return new ValueMatcher()
      {
        private boolean init;
        private int finding = -1;

        @Override
        public boolean matches()
        {
          IndexedInts.WithLookup indexed = (IndexedInts.WithLookup) selector.get();
          final int size = indexed.size();
          if (size == 0) {
            return false;
          }
          if (!init) {
            finding = indexed.lookupId(value);
            init = true;
          }
          if (size == 1) {
            return finding == indexed.get(0);
          }
          for (Integer id : indexed) {
            if (finding == id) {
              return true;
            }
          }
          return false;
        }
      };
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return value.equals(selector.get());
      }
    };
  }

  @Override
  public String toString()
  {
    return "SelectorFilter{" +
           "dimension='" + dimension + '\'' +
           ", value='" + value + '\'' +
           '}';
  }
}
