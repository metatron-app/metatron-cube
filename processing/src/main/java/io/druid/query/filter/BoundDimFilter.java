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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.filter.BoundFilter;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class BoundDimFilter extends SingleInput implements RangeFilter
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

  public static BoundDimFilter between(String dimension, Object lower, Object upper)
  {
    return new BoundDimFilter(dimension, String.valueOf(lower), String.valueOf(upper), false, true, null, null);
  }

  public static BoundDimFilter betweenStrict(String dimension, Object lower, Object upper)
  {
    return new BoundDimFilter(dimension, String.valueOf(lower), String.valueOf(upper), false, false, null, null);
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
           || typeOfBound(resolver) == ValueType.STRING;
  }

  @Override
  public List<Range> toRanges(TypeResolver resolver)
  {
    return toRanges(typeOfBound(resolver), false);
  }

  // used in geo-server adapter
  private List<Range> toRanges(ValueType type, boolean withNot)
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

  public ValueType typeOfBound(TypeResolver resolver)
  {
    if (extractionFn == null) {
      ValueDesc resolved = resolver.resolve(dimension, ValueDesc.STRING);
      if (resolved.isStringOrDimension()) {
        resolved = ValueDesc.STRING;
      }
      if (comparatorType == null || comparatorType.equals(StringComparators.NUMERIC_NAME)) {
        return resolved.type();
      }
      ValueDesc desc = comparatorType == null ? resolved : ValueDesc.of(comparatorType);
      if (desc != null && desc.isPrimitive()) {
        return desc.type();
      }
    }
    return ValueType.STRING;
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

  public BoundDimFilter withType(ValueDesc type)
  {
    return new BoundDimFilter(
        dimension,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        type.typeName(),
        extractionFn
    );
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new BoundFilter(this);
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

  public BoundDimFilter withComparatorType(String comparatorType)
  {
    return new BoundDimFilter(
        dimension,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        Preconditions.checkNotNull(comparatorType),
        extractionFn
    );
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

    if (isLowerStrict() != that.isLowerStrict()) {
      return false;
    }
    if (isUpperStrict() != that.isUpperStrict()) {
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
    return getExtractionFn() != null
           ? getExtractionFn().equals(that.getExtractionFn())
           : that.getExtractionFn() == null;

  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(dimension, lower, upper);
    result = 31 * result + (isLowerStrict() ? 1 : 0);
    result = 31 * result + (isUpperStrict() ? 1 : 0);
    result = 31 * result + Objects.hashCode(comparatorType);
    result = 31 * result + (getExtractionFn() != null ? getExtractionFn().hashCode() : 0);
    return result;
  }
}
