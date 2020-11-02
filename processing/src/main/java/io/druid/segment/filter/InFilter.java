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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.ExprEval;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedID;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collection;

/**
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final Collection<String> values;
  private final ExtractionFn extractionFn;

  public InFilter(String dimension, Collection<String> values, ExtractionFn extractionFn)
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    if (extractionFn != null && Filters.isColumnWithoutBitmap(selector, dimension)) {
      return null;  // extractionFn requires bitmap index
    }
    if (extractionFn == null) {
      return DimFilters.union(
          selector.getBitmapFactory(),
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
          new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              // InDimFilter converts all null "values" to empty.
              return values.contains(Strings.nullToEmpty(extractionFn.apply(input)));
            }
          },
          context
      );
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final boolean allowsNull = values.contains("");
    final ValueDesc type = factory.resolve(dimension);
    if (type == null) {
      // should handle extract fn
      return BooleanValueMatcher.of(toPredicate(allowsNull, null).apply(null));
    }
    if (ValueDesc.isDimension(type) && extractionFn == null) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(dimension));
      final IntSet ids = new IntOpenHashSet();
      for (String value : values) {
        final int index = selector.lookupId(value);
        if (index >= 0) {
          ids.add(index);
        }
      }
      return Filters.toValueMatcher(selector, value -> ids.contains(value), allowsNull);
    }
    ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
    if (ValueDesc.isIndexedId(type)) {
      if (extractionFn == null) {
        final ObjectColumnSelector<IndexedID> indexedSelector = selector;
        return new ValueMatcher()
        {
          private boolean ready;
          private final IntSet find = new IntOpenHashSet();

          @Override
          public boolean matches()
          {
            final IndexedID indexed = indexedSelector.get();
            if (!ready) {
              for (String value : values) {
                final int id = indexed.lookupId(value);
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
    return Filters.toValueMatcher(selector, toPredicate(allowsNull, selector.type()));
  }

  @SuppressWarnings("unchecked")
  private Predicate toPredicate(final boolean containsNull, final ValueDesc valueType)
  {
    final Collection container;
    if (extractionFn == null && valueType != null && valueType.isPrimitiveNumeric()) {
      Preconditions.checkArgument(!containsNull, "cannot use null value for numeric type");
      container = Sets.newHashSet();
      for (String value : values) {
        container.add(Evals.castToValue(ExprEval.of(value), valueType));
      }
    } else {
      container = values;
    }
    Predicate predicate = new Predicate()
    {
      @Override
      public boolean apply(Object input)
      {
        return input == null ? containsNull : container.contains(input);
      }
    };
    if (extractionFn != null) {
      predicate = Predicates.compose(predicate, extractionFn);
    }
    return predicate;
  }

  @Override
  public String toString()
  {
    return "InFilter{" +
           "dimension='" + dimension + '\'' +
           ", values=" + values +
           (extractionFn == null ? "" : ", extractionFn=" + extractionFn) +
           '}';
  }
}
