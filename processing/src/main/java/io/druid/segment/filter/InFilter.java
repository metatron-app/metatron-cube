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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Rows;
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
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedID;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final List<String> values;
  private final ExtractionFn extractionFn;

  public InFilter(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
  }

  @Override
  public BitmapHolder getBitmapIndex(final FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    if (extractionFn != null && Filters.isColumnWithoutBitmap(selector, dimension)) {
      return null;  // extractionFn requires bitmap index
    }
    if (extractionFn == null) {
      return unionBitmaps(dimension, values, selector);
    } else {
      return BitmapHolder.exact(Filters.matchPredicate(
          dimension,
          v -> values.contains(Strings.nullToEmpty(extractionFn.apply(v))),
          context
      ));
    }
  }

  // values reagarded to be sorted
  public static BitmapHolder unionBitmaps(String dimension, List<String> values, BitmapIndexSelector selector)
  {
    final Column column = selector.getColumn(dimension);
    if (column == null) {
      return BitmapHolder.exact(selector.createBoolean(!values.isEmpty() && "".equals(values.get(0))));
    }
    final BitmapIndex bitmap = column.getBitmapIndex();
    if (bitmap == null) {
      return null;
    }
    final Stream<ImmutableBitmap> stream = GuavaUtils.stateToInt(
        values.stream(), (v, s) -> bitmap.getIndex(v, s)).mapToObj(x -> bitmap.getBitmap(x)
    );
    return BitmapHolder.exact(DimFilters.union(selector.getBitmapFactory(), () -> stream.iterator()));
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
    if (type.isDimension() && extractionFn == null) {
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
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
    if (type.isPrimitiveNumeric() && extractionFn == null) {
      final List<BigDecimal> decimals = ImmutableList.copyOf(
          Iterables.filter(Iterables.transform(values, Rows::tryParseDecimal), Predicates.notNull())
      );
      final Predicate<Number> predicate = toNumericPredicate(allowsNull, type, decimals);
      if (predicate != null) {
        return () -> predicate.apply((Number) selector.get());
      }
      return allowsNull ? () -> selector.get() == null : ValueMatcher.FALSE;
    }
    if (selector.type().isBitSet()) {
      if (extractionFn == null) {
        List<Integer> ids = ImmutableList.copyOf(
            Iterables.filter(Iterables.transform(values, v -> Rows.parseInt(v, null)), Predicates.notNull())
        );
        if (ids.isEmpty()) {
          return () -> selector.get() == null;
        }
        final int[] ix = Ints.toArray(ids);
        return () -> {
          final BitSet bitSet = (BitSet) selector.get();
          if (bitSet == null) {
            return false;
          }
          for (int x : ix) {
            if (bitSet.get(x)) {
              return true;
            }
          }
          return false;
        };
      }
      return ValueMatcher.FALSE;
    }
    ObjectColumnSelector wrapped = selector;
    if (ValueDesc.isIndexedId(type)) {
      if (extractionFn == null) {
        return new ValueMatcher()
        {
          private boolean ready;
          private final IntSet find = new IntOpenHashSet();

          @Override
          public boolean matches()
          {
            final IndexedID indexed = (IndexedID) selector.get();
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
      } else {
        wrapped = ColumnSelectors.asValued(selector);
      }
    }
    return Filters.toValueMatcher(wrapped, toPredicate(allowsNull, wrapped.type()));
  }

  private static Predicate<Number> toNumericPredicate(
      final boolean containsNull,
      final ValueDesc valueType,
      final List<BigDecimal> decimals
  )
  {
    if (decimals.isEmpty()) {
      return null;
    } else if (valueType.isLong()) {
      final LongSet set = new LongOpenHashSet();
      for (BigDecimal decimal : decimals) {
        final long lv = decimal.longValue();
        if (decimal.compareTo(BigDecimal.valueOf(lv)) == 0) {
          set.add(lv);
        }
      }
      if (!set.isEmpty()) {
        return v -> v == null ? containsNull : set.contains(v.longValue());
      }
    } else if (valueType.isFloat()) {
      final FloatSet set = new FloatOpenHashSet();
      for (BigDecimal decimal : decimals) {
        final float fv = decimal.floatValue();
        if (decimal.compareTo(BigDecimal.valueOf(fv)) == 0) {
          set.add(fv);
        }
      }
      if (!set.isEmpty()) {
        return v -> v == null ? containsNull : set.contains(v.floatValue());
      }
    } else {
      final DoubleSet set = new DoubleOpenHashSet();
      for (BigDecimal decimal : decimals) {
        final double dv = decimal.doubleValue();
        if (decimal.compareTo(BigDecimal.valueOf(dv)) == 0) {
          set.add(dv);
        }
      }
      if (!set.isEmpty()) {
        return v -> v == null ? containsNull : set.contains(v.doubleValue());
      }
    }
    return null;
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
