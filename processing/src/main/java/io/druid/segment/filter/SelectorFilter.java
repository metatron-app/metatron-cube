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
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.IOUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.ExprEval;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.IndexedID;

import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Objects;

/**
 */
public class SelectorFilter implements Filter
{
  private final DimFilter source;
  private final String dimension;
  private final String value;

  public SelectorFilter(DimFilter source, String dimension, String value)
  {
    this.source = source;
    this.dimension = dimension;
    this.value = value;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    final Column column = selector.getColumn(dimension);
    if (column == null) {
      return BitmapHolder.exact(selector.createBoolean(Strings.isNullOrEmpty(value)));
    }
    if (column.getCapabilities().hasBitmapIndexes()) {
      final BitmapIndex bitmapIndex = column.getBitmapIndex();
      final int index = bitmapIndex.indexOf(value);
      if (context.isRoot(source)) {
        context.dictionaryRef(dimension, RoaringBitmapFactory.of(index));
      }
      return BitmapHolder.exact(bitmapIndex.getBitmap(index));
    }
    if (value == null && column.getGenericColumnType() != null) {
      final GenericColumn generic = column.getGenericColumn();
      try {
        ImmutableBitmap nulls = generic.getNulls();
        if (nulls != null) {
          return BitmapHolder.exact(nulls);
        }
      }
      finally {
        IOUtils.closePropagate(generic);
      }
    }
    final SecondaryIndex index = selector.getExternalIndex(dimension, null);
    try {
      return index == null ? null : index.eq(dimension, value, context);
    }
    finally {
      IOUtils.closePropagate(index);
    }
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    final boolean allowsNull = Strings.isNullOrEmpty(value);
    final ValueDesc valueType = factory.resolve(dimension);
    if (valueType == null) {
      return BooleanValueMatcher.of(allowsNull);
    }
    if (valueType.isDimension()) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(dimension));
      final int index = selector.lookupId(value);
      if (index < 0) {
        return BooleanValueMatcher.of(false);
      }
      return Filters.toValueMatcher(selector, value -> value == index, allowsNull);
    }
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
    if (valueType.isNumeric()) {
      if (allowsNull) {
        return () -> selector.get() == null;
      }
      final BigDecimal decimal = Rows.tryParseDecimal(value);
      if (decimal == null) {
        return ValueMatcher.FALSE;
      }
      final Predicate<Number> predicate = numericPredicate(valueType, decimal);
      if (predicate != null) {
        return () -> predicate.apply((Number) selector.get());
      }
      return ValueMatcher.FALSE;
    }
    if (selector.type().isBitSet()) {
      if (StringUtils.isNullOrEmpty(value)) {
        return () -> selector.get() == null;
      }
      final Integer ix = Rows.parseInt(value, null);
      if (ix != null) {
        return () -> {
          final BitSet bitSet = (BitSet) selector.get();
          return bitSet != null && bitSet.get(ix);
        };
      }
      return ValueMatcher.FALSE;
    }
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
    final Object casted = allowsNull ? null : Evals.castToValue(ExprEval.of(value), valueType);
    if (casted == null) {
      return Filters.toValueMatcher(selector, Predicates.isNull());
    }
    return Filters.toValueMatcher(selector, v -> casted.equals(v));
  }

  private static Predicate<Number> numericPredicate(final ValueDesc type, final BigDecimal decimal)
  {
    if (type.isDecimal()) {
      return v -> Objects.equals(decimal, v);
    } else if (type.isLong() && Rows.isExactLong(decimal)) {
      return v -> v == null ? decimal == null : decimal.longValue() == v.longValue();
    } else if (type.isFloat() && Rows.isExactFloat(decimal)) {
      return v -> v == null ? decimal == null : decimal.floatValue() == v.floatValue();
    } else if (Rows.isExactDouble(decimal)) {
      return v -> v == null ? decimal == null : decimal.doubleValue() == v.doubleValue();
    }
    return null;
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
