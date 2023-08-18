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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.IndexedIDSupport;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.filter.BoundFilter;
import io.druid.segment.filter.FilterContext;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.ToIntFunction;

public class BoundDimFilter extends SingleInput implements RangeFilter, IndexedIDSupport, DimFilter.WithExtraction
{
  private final String dimension;
  private final String upper;
  private final String lower;
  private final boolean lowerStrict;
  private final boolean upperStrict;
  private final String comparatorType;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public BoundDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("lower") String lower,
      @JsonProperty("upper") String upper,
      @JsonProperty("lowerStrict") boolean lowerStrict,
      @JsonProperty("upperStrict") boolean upperStrict,
      @JsonProperty("comparatorType") String comparatorType,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "Must have a valid, non-null dimension or expression");
    this.upper = upper;
    this.lower = lower;
    this.lowerStrict = lowerStrict;
    this.upperStrict = upperStrict;
    this.comparatorType = comparatorType;
    this.extractionFn = extractionFn;

    if (Strings.isNullOrEmpty(lower) && Strings.isNullOrEmpty(upper) && (lowerStrict || upperStrict)) {
      throw new IllegalArgumentException("empty bound");
    }
    Preconditions.checkArgument(
        comparatorType == null || StringComparators.createGeneric(comparatorType, null) != null,
        "invalid comparator type %s", comparatorType
    );
    ValueType valueType = ValueType.of(comparatorType, ValueType.STRING);
    Preconditions.checkArgument(
        extractionFn == null || valueType == ValueType.STRING,
        "invalid combination of comparator %s and extract function", comparatorType
    );
  }

  public BoundDimFilter(
      String dimension,
      String lower,
      String upper,
      boolean lowerStrict,
      boolean upperStrict,
      boolean alphaNumeric,
      ExtractionFn extractionFn
  )
  {
    this(
        dimension,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        alphaNumeric ? StringComparators.ALPHANUMERIC_NAME : null,
        extractionFn
    );
  }

  // <= && <
  public static BoundDimFilter range(String dimension, Object lower, Object upper)
  {
    return new BoundDimFilter(dimension, String.valueOf(lower), String.valueOf(upper), false, true, null, null);
  }

  // <= && <=
  public static BoundDimFilter between(String dimension, Object lower, Object upper)
  {
    return new BoundDimFilter(dimension, String.valueOf(lower), String.valueOf(upper), true, true, null, null);
  }

  public static BoundDimFilter gt(String dimension, Object lower)
  {
    return new BoundDimFilter(dimension, String.valueOf(lower), null, true, false, null, null);
  }

  public static BoundDimFilter gte(String dimension, Object lower)
  {
    return new BoundDimFilter(dimension, String.valueOf(lower), null, false, false, null, null);
  }

  public static BoundDimFilter lt(String dimension, Object upper)
  {
    return new BoundDimFilter(dimension, null, String.valueOf(upper), false, true, null, null);
  }

  public static BoundDimFilter lte(String dimension, Object upper)
  {
    return new BoundDimFilter(dimension, null, String.valueOf(upper), false, false, null, null);
  }

  @Override
  public boolean possible(TypeResolver resolver)
  {
    if (extractionFn != null) {
      return false;
    }
    return !StringUtils.isNullOrEmpty(lower)
           || !StringUtils.isNullOrEmpty(upper)
           || typeOfBound(resolver).isString();
  }

  @Override
  public List<Range> toRanges(TypeResolver resolver)
  {
    return toRanges(typeOfBound(resolver), false);
  }

  // used in geo-server adapter
  private List<Range> toRanges(ValueDesc type, boolean withNot)
  {
    Preconditions.checkArgument(extractionFn == null, "extractionFn");
    if (StringUtils.isNullOrEmpty(lower) && StringUtils.isNullOrEmpty(upper)) {
      if (!type.isNumeric()) {
        return Arrays.<Range>asList(Range.closed("", ""));
      }
      throw new IllegalStateException("cannot handle null for numeric types");
    }
    final Comparable lower = hasLowerBound() ? (Comparable) type.cast(getLower()) : null;
    final Comparable upper = hasUpperBound() ? (Comparable) type.cast(getUpper()) : null;
    if (lower != null && upper != null) {
      if (withNot) {
        return Arrays.<Range>asList(
            lowerStrict ? Range.atMost(lower) : Range.lessThan(lower),      // "<=" : "<"
            upperStrict ? Range.atLeast(upper) : Range.greaterThan(upper)   // ">=" : ">"
        );
      }
      return Arrays.<Range>asList(
          lowerStrict && upperStrict ? Range.open(lower, upper) :
          lowerStrict ? Range.openClosed(lower, upper) :
          upperStrict ? Range.closedOpen(lower, upper) :
          Range.closed(lower, upper)
      );
    } else if (lower != null) {
      if (withNot) {
        return Arrays.<Range>asList(lowerStrict ? Range.atMost(lower) : Range.lessThan(lower));     // "<=" : "<"
      } else {
        return Arrays.<Range>asList(lowerStrict ? Range.greaterThan(lower) : Range.atLeast(lower)); // ">" : ">="
      }
    } else {
      if (withNot) {
        return Arrays.<Range>asList(upperStrict ? Range.atLeast(upper) : Range.greaterThan(upper)); // ">=" : ">"
      } else {
        return Arrays.<Range>asList(upperStrict ? Range.lessThan(upper) : Range.atMost(upper));     // "<" : "<="
      }
    }
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getUpper()
  {
    return upper;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getLower()
  {
    return lower;
  }

  @JsonProperty
  public boolean isLowerStrict()
  {
    return lowerStrict;
  }

  @JsonProperty
  public boolean isUpperStrict()
  {
    return upperStrict;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getComparatorType()
  {
    return comparatorType;
  }

  public boolean isLexicographic()
  {
    return comparatorType == null || StringComparators.isLexicographicString(comparatorType);
  }

  public Comparator getComparator()
  {
    return StringComparators.createGeneric(comparatorType, GuavaUtils.nullFirstNatural());
  }

  public boolean hasLowerBound()
  {
    return lower != null;
  }

  public boolean hasUpperBound()
  {
    return upper != null;
  }

  public boolean isEquals()
  {
    return !lowerStrict && !upperStrict && Objects.equals(lower, upper);
  }

  public boolean hasBoth()
  {
    return hasLowerBound() && hasUpperBound();
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.BOUND_CACHE_ID)
                  .append(lowerStrict, upperStrict)
                  .append(dimension).sp()
                  .append(lower).sp()
                  .append(upper).sp()
                  .append(comparatorType).sp()
                  .append(extractionFn);
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new BoundDimFilter(
        dimension,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        comparatorType,
        extractionFn
    );
  }

  public BoundDimFilter withComparatorType(String comparatorType)
  {
    if (Objects.equals(this.comparatorType, comparatorType)) {
      return this;
    }
    return new BoundDimFilter(
        dimension,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        comparatorType,
        extractionFn
    );
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new BoundFilter(this);
  }

  @Override
  public double cost(FilterContext context)
  {
    ColumnCapabilities capabilities = context.getCapabilities(dimension);
    if (capabilities == null) {
      return ZERO;
    }
    if (capabilities.isDictionaryEncoded()) {
      return extractionFn == null ? hasBoth() ? 0.01 : 0.004 : DIMENSIONSCAN;
    }
    return FULLSCAN;
  }

  @Override
  public String toString()
  {
    return "BoundDimFilter{" + toExpression() + "}";
  }

  private String toExpression()
  {
    StringBuilder builder = new StringBuilder();
    if (lower != null) {
      builder.append(lower).append(lowerStrict ? " < " : " <= ");
    }
    if (extractionFn != null) {
      builder.append(extractionFn.getClass().getSimpleName()).append('(');
    }
    builder.append(dimension);
    if (extractionFn != null) {
      builder.append(')');
    }
    if (upper != null) {
      builder.append(upperStrict ? " < " : " <= ").append(upper);
    }
    if (comparatorType != null) {
      builder.append('(').append(comparatorType).append(')');
    }
    return builder.toString();
  }

  private ValueDesc typeOfBound(TypeResolver resolver)
  {
    if (extractionFn == null) {
      ValueDesc resolved = resolver.resolve(dimension, ValueDesc.STRING).unwrapDimension();
      if (comparatorType == null || comparatorType.equals(StringComparators.NUMERIC_NAME)) {
        return resolved;
      }
      ValueDesc desc = comparatorType == null ? resolved : ValueDesc.of(comparatorType);
      if (desc != null && desc.isPrimitive()) {
        return desc;
      }
    }
    return ValueDesc.STRING;
  }

  @SuppressWarnings("unchecked")
  public Predicate toPredicate(TypeResolver resolver)
  {
    final ValueDesc type = typeOfBound(resolver);
    final String lowerValue = Strings.emptyToNull(getLower());
    final String upperValue = Strings.emptyToNull(getUpper());

    final boolean numeric = type.isNumeric() || StringComparators.NUMERIC_NAME.equals(comparatorType);
    final Object lower = numeric ? Rows.parseDecimal(lowerValue) : type.cast(lowerValue);
    final Object upper = numeric ? Rows.parseDecimal(upperValue) : type.cast(upperValue);

    final ToIntFunction lc = numeric ? lower((BigDecimal) lower, type, lowerStrict) : comparator(lower, comparatorType);
    final ToIntFunction rc = numeric ? upper((BigDecimal) upper, type, upperStrict) : comparator(upper, comparatorType);

    final Predicate predicate = toPredicate(lc, lowerStrict, rc, upperStrict);
    if (numeric) {
      return v -> v != null && predicate.apply(v);
    }
    final Function func = extractionFn != null ? extractionFn :
                          type.isString() ? GuavaUtils.NULLABLE_TO_STRING_FUNC : Functions.identity();

    // lower bound allows null && upper bound allows null
    final boolean allowNull = lower == null && !lowerStrict || upper == null && !upperStrict;
    final boolean allowOnlyNull = allowNull && lower == null && upper == null;

    return v -> {
      final Object value = func.apply(v);
      if (value == null) {
        return allowNull;
      } else if (allowOnlyNull) {
        return false;
      }
      return predicate.apply(value);
    };
  }

  private static ToIntFunction lower(final BigDecimal constant, final ValueDesc type, final boolean strict)
  {
    if (constant == null) {
      return null;
    } else if (type.isDecimal()) {
      return v -> constant.compareTo((BigDecimal) v);
    } else if (type.isString()) {
      return v -> constant.compareTo(Rows.parseDecimal(String.valueOf(v)));
    } else if (type.isLong()) {
      if (strict || constant.equals(BigDecimal.valueOf(constant.longValue()))) {
        final long lv = constant.longValue();
        return v -> Long.compare(lv, ((Number) v).longValue());
      }
    } else if (type.isFloat()) {
      final float fv = constant.floatValue();
      return v -> Float.compare(fv, ((Number) v).floatValue());
    }
    final double dv = constant.doubleValue();
    return v -> Double.compare(dv, ((Number) v).doubleValue());
  }

  private static ToIntFunction upper(final BigDecimal constant, final ValueDesc type, final boolean strict)
  {
    if (constant == null) {
      return null;
    } else if (type.isDecimal()) {
      return v -> constant.compareTo((BigDecimal) v);
    } else if (type.isString()) {
      return v -> constant.compareTo(Rows.parseDecimal(String.valueOf(v)));
    } else if (type.isLong()) {
      if (strict || constant.equals(BigDecimal.valueOf(constant.longValue()))) {
        final long lv = (long) Math.ceil(constant.doubleValue());
        return v -> Long.compare(lv, ((Number) v).longValue());
      }
    } else if (type.isFloat()) {
      final float fv = constant.floatValue();
      return v -> Float.compare(fv, ((Number) v).floatValue());
    }
    final double dv = constant.doubleValue();
    return v -> Double.compare(dv, ((Number) v).doubleValue());
  }

  @SuppressWarnings("unchecked")
  private static ToIntFunction comparator(final Object constant, final String comparatorType)
  {
    if (constant != null) {
      Comparator comparator = StringComparators.createGeneric(comparatorType, GuavaUtils.nullFirstNatural());
      return v -> comparator.compare(constant, v);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static Predicate toPredicate(
      final ToIntFunction lower,
      final boolean lowerStrict,
      final ToIntFunction upper,
      final boolean upperStrict
  )
  {
    if (lower != null && upper != null) {
      if (lowerStrict && upperStrict) {
        return v -> lower.applyAsInt(v) < 0 && upper.applyAsInt(v) > 0;
      } else if (lowerStrict) {
        return v -> lower.applyAsInt(v) < 0 && upper.applyAsInt(v) >= 0;
      } else if (upperStrict) {
        return v -> lower.applyAsInt(v) <= 0 && upper.applyAsInt(v) > 0;
      } else {
        return v -> lower.applyAsInt(v) <= 0 && upper.applyAsInt(v) >= 0;
      }
    } else if (lower != null) {
      if (lowerStrict) {
        return v -> lower.applyAsInt(v) < 0;
      } else {
        return v -> lower.applyAsInt(v) <= 0;
      }
    } else if (upper != null) {
      if (upperStrict) {
        return v -> upper.applyAsInt(v) > 0;
      } else {
        return v -> upper.applyAsInt(v) >= 0;
      }
    }
    return v -> false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BoundDimFilter that = (BoundDimFilter) o;

    if (lowerStrict != that.lowerStrict) {
      return false;
    }
    if (upperStrict != that.upperStrict) {
      return false;
    }
    if (!Objects.equals(comparatorType, that.comparatorType)) {
      return false;
    }
    if (!Objects.equals(dimension, that.dimension)) {
      return false;
    }
    if (!Objects.equals(upper, that.upper)) {
      return false;
    }
    if (!Objects.equals(lower, that.lower)) {
      return false;
    }
    return Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, lower, upper, lowerStrict, upperStrict, comparatorType, extractionFn);
  }
}
