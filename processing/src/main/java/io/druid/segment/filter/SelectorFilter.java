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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.guava.IntPredicate;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.ExprEval;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedID;
import io.druid.segment.lucene.Lucenes;

/**
 */
public class SelectorFilter implements Filter
{
  private final String dimension;
  private final String value;

  public SelectorFilter(String dimension, String value)
  {
    this.dimension = dimension;
    this.value = value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
  {
    final ColumnCapabilities capabilities = selector.getCapabilities(dimension);
    if (capabilities == null || capabilities.hasBitmapIndexes()) {
      return selector.getBitmapIndex(dimension, value);
    } else if (capabilities.hasLuceneIndex()) {
      return selector.getLuceneIndex(dimension).filterFor(Lucenes.point(dimension, value), baseBitmap);
    } else if (capabilities.hasMetricBitmap()) {
      return selector.getMetricBitmap(dimension).filterFor(Range.closed(value, value), baseBitmap);
    } else if (capabilities.hasBitSlicedBitmap()) {
      return selector.getBitSlicedBitmap(dimension).filterFor(Range.closed(value, value), baseBitmap);
    }
    return null;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final boolean allowsNull = Strings.isNullOrEmpty(value);
    final ValueDesc valueType = factory.resolve(dimension);
    if (valueType == null) {
      return BooleanValueMatcher.of(allowsNull);
    }
    if (ValueDesc.isDimension(valueType)) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(dimension));
      @SuppressWarnings("unchecked")
      final int index = selector.lookupId(value);
      if (index < 0) {
        return BooleanValueMatcher.of(false);
      }
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
    final Object casted = valueType.isPrimitiveNumeric() ? Evals.castToValue(ExprEval.of(value), valueType) : value;
    return Filters.toValueMatcher(
        selector, new Predicate()
        {
          @Override
          public boolean apply(Object input)
          {
            return input == null ? allowsNull : input.equals(casted);
          }
        }
    );
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
