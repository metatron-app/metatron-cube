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
import io.druid.common.guava.IntPredicate;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.IndexedID;
import org.python.google.common.base.Strings;

/**
 */
public class SelectorFilter implements Filter
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
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final boolean allowsNull = Strings.isNullOrEmpty(value);
    final ValueDesc valueType = factory.getColumnType(dimension);
    if (valueType == null) {
      return BooleanValueMatcher.of(allowsNull);
    }
    if (ValueDesc.isDimension(valueType)) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(dimension));
      @SuppressWarnings("unchecked")
      final int index = selector.lookupId(value);
      return Filters.toValueMatcher(
          selector, new IntPredicate()
          {
            @Override
            public boolean apply(int value)
            {
              return value == index;
            }
          },
          allowsNull
      );
    }
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
    if (ValueDesc.isIndexedId(selector.type())) {
      return new ValueMatcher()
      {
        private boolean init;
        private int finding = -1;

        @Override
        public boolean matches()
        {
          IndexedID indexed = (IndexedID) selector.get();
          if (!init) {
            finding = indexed.lookupId(value);
            init = true;
          }
          return finding == indexed.get();
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
