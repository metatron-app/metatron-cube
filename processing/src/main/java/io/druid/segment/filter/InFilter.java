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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.guava.IntPredicate;
import io.druid.data.ValueDesc;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.IndexedID;

import java.util.Set;

/**
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;

  public InFilter(String dimension, Set<String> values, ExtractionFn extractionFn)
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
  }

  @Override
  public boolean supportsBitmap(RowResolver resolver)
  {
    return ValueDesc.isDimension(resolver.resolveColumn(dimension));
  }

  @Override
  public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      MutableBitmap bitmap = selector.getBitmapFactory().makeEmptyMutableBitmap();
      BitmapIndex indexed = selector.getBitmapIndex(dimension);
      for (String value : values) {
        int index = indexed.getIndex(value);
        if (index >= 0) {
          bitmap.add(index);
        }
      }
      return selector.getBitmapFactory().makeImmutableBitmap(bitmap);
    }
    return null;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      return selector.getBitmapFactory().union(
          Iterables.transform(
              values, new Function<String, ImmutableBitmap>()
              {
                @Override
                public ImmutableBitmap apply(String value)
                {
                  return selector.getBitmapIndex(dimension, value);
                }
              }
          )
      );
    } else {
      return Filters.matchPredicate(
          dimension,
          selector,
          new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              // InDimFilter converts all null "values" to empty.
              return values.contains(Strings.nullToEmpty(extractionFn.apply(input)));
            }
          }
      );
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    ValueDesc type = factory.getColumnType(dimension);
    if (type == null) {
      return BooleanValueMatcher.of(toPredicate().apply(null));
    }
    if (ValueDesc.isDimension(type) && extractionFn == null) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(dimension));
      final Set<Integer> ids = Sets.newHashSet();
      boolean allowsNull = false;
      for (String value : values) {
        allowsNull |= Strings.isNullOrEmpty(value);
        int index = selector.lookupId(value);
        if (index >= 0) {
          ids.add(index);
        }
      }
      return Filters.toValueMatcher(
          selector, new IntPredicate()
          {
            @Override
            public boolean apply(int value) { return ids.contains(value); }
          },
          allowsNull
      );
    }
    ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
    if (ValueDesc.isIndexedId(type)) {
      if (extractionFn == null) {
        final ObjectColumnSelector<IndexedID> indexedSelector = selector;
        return new ValueMatcher()
        {
          private boolean ready;
          private final Set<Integer> find = Sets.newHashSet();
          @Override
          public boolean matches()
          {
            final IndexedID indexed = indexedSelector.get();
            if (!ready) {
              for (String value : values) {
                int id = indexed.lookupId(value);
                if (id >= 0) {
                  find.add(id);
                }
              }
              ready = true;
            }
            return find.contains(indexed.get());
          }
        };
      }
      selector = ColumnSelectors.asValued(selector);
    }
    return Filters.toValueMatcher(selector, toPredicate());
  }

  private Predicate<String> toPredicate()
  {
    return new Predicate<String>()
    {
      @Override
      public boolean apply(String input)
      {
        if (extractionFn != null) {
          input = extractionFn.apply(input);
        }
        return values.contains(Strings.nullToEmpty(input));
      }
    };
  }

  @Override
  public String toString()
  {
    return "InFilter{" +
           "dimension='" + dimension + '\'' +
           ", values=" + values +
           ", extractionFn=" + extractionFn +
           '}';
  }
}
