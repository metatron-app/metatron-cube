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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.Scannable;
import io.druid.common.Scannable.BufferBacked;
import io.druid.common.utils.IOUtils;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Dictionary;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.MatcherContext;

import javax.annotation.Nullable;

import static io.druid.query.filter.DimFilterCacheKey.IS_NULL_CACHE_ID;

public class IsNullDimFilter extends DimFilter.SingleInput implements DimFilter.BestEffort
{
  private final String dimension;

  @JsonCreator
  public IsNullDimFilter(@JsonProperty("dimension") String dimension)
  {
    this.dimension = dimension;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new IsNullDimFilter(dimension);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(IS_NULL_CACHE_ID).append(dimension);
  }

  @Override
  public double cost(FilterContext context)
  {
    ColumnCapabilities capabilities = context.getCapabilities(dimension);
    return capabilities == null ? ZERO : capabilities.isDictionaryEncoded() ? PICK : FULLSCAN;
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        BitmapIndexSelector selector = context.indexSelector();
        Column column = selector.getColumn(dimension);
        if (column == null) {
          return BitmapHolder.exact(selector.createBoolean(true));
        }
        if (column.hasDictionaryEncodedColumn()) {
          return BitmapHolder.exact(fromDictionaryEncodedColumn(column, context));
        } else if (column.hasGenericColumn()) {
          return BitmapHolder.exact(fromGenericColumn(column, context));
        }
        return null;
      }

      @Override
      public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
      {
        ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
        return selector == null ? ValueMatcher.TRUE : () -> selector.get() == null;
      }
    };
  }

  @Nullable
  private ImmutableBitmap fromDictionaryEncodedColumn(Column column, FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    final DictionaryEncodedColumn encoded = column.getDictionaryEncoded();
    try {
      final Dictionary<String> dictionary = encoded.dictionary();
      final Boolean containsNull = dictionary.containsNull();
      if (containsNull == null) {
        return null;  // should not be happend
      }
      if (!containsNull) {
        return selector.createBoolean(false);
      }
      final ImmutableBitmap bitmap = column.getBitmapIndex().getBitmap(dictionary.indexOf(""));
      return bitmap.isEmpty() ? selector.createBoolean(false) : bitmap;
    }
    finally {
      IOUtils.closePropagate(encoded);
    }
  }

  @Nullable
  private ImmutableBitmap fromGenericColumn(Column column, FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    final GenericColumn generic = column.getGenericColumn();
    try {
      final ImmutableBitmap bitmap = generic.getNulls();
      if (bitmap != null) {
        return bitmap.isEmpty() ? selector.createBoolean(false) : bitmap;
      }
      final BitmapFactory factory = selector.getBitmapFactory();
      final MutableBitmap mutable = factory.makeEmptyMutableBitmap();
      if (generic instanceof BufferBacked) {
        ((BufferBacked) generic).scan(context.rowIterator(), (x, b, o, l) -> {if (l == 0) {mutable.add(x);}});
      } else if (generic instanceof Scannable) {
        ((Scannable<?>) generic).scan(context.rowIterator(), (ix, v) -> {if (v == null) {mutable.add(ix);}});
      } else {
        return null;
      }
      return factory.makeImmutableBitmap(mutable);
    }
    finally {
      IOUtils.closePropagate(generic);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof IsNullDimFilter && dimension.equals(((IsNullDimFilter)o).dimension);
  }

  @Override
  public int hashCode()
  {
    return dimension.hashCode();
  }

  @Override
  public String toString()
  {
    return dimension + "==NULL";
  }
}
