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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import io.druid.common.utils.Ranges;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.ordering.Comparators;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.filter.BoundFilter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BoundDimFilter implements DimFilter.RangeFilter
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
        comparatorType == null || Comparators.createGeneric(comparatorType, null) != null,
        "invalid comparator type " + comparatorType
    );
    ValueType valueType = ValueType.of(comparatorType, ValueType.STRING);
    Preconditions.checkArgument(
        extractionFn == null || valueType == ValueType.STRING,
        "invalid combination of comparator " + comparatorType + " and extract function"
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
  public List<Range> toRanges(ValueType type, boolean withNot)
  {
    Preconditions.checkArgument(extractionFn == null, "extractionFn");
    if (StringUtils.isNullOrEmpty(lower) && StringUtils.isNullOrEmpty(upper)) {
      if (!type.isNumeric()) {
        return Arrays.<Range>asList(Range.closed("", ""));
      }
      throw new IllegalStateException("cannot handle null for numeric types");
    }
    final Comparable lower = hasLowerBound() ? type.cast(getLower()) : null;
    final Comparable upper = hasUpperBound() ? type.cast(getUpper()) : null;
    if (lower != null && upper != null) {
      if (withNot) {
        return Arrays.<Range>asList(
            Ranges.of(lower, lowerStrict ? "<=" : "<"),
            Ranges.of(upper, upperStrict ? ">=" : ">")
        );
      }
      return Arrays.<Range>asList(
          lowerStrict && upperStrict ? Range.open(lower, upper) :
          lowerStrict ? Range.openClosed(lower, upper) :
          upperStrict ? Range.closedOpen(lower, upper) :
          Range.closed(lower, upper)
      );
    }
    if (lower != null) {
      if (withNot) {
        return Arrays.<Range>asList(Ranges.of(lower, lowerStrict ? "<=" : "<"));
      }
      return Arrays.<Range>asList(Ranges.of(lower, lowerStrict ? ">" : ">="));
    }
    if (withNot) {
      return Arrays.<Range>asList(Ranges.of(upper, upperStrict ? ">=" : ">"));
    }
    return Arrays.<Range>asList(Ranges.of(upper, upperStrict ? "<" : "<="));
  }

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
      ValueDesc desc = comparatorType == null ? resolver.resolve(dimension) : ValueDesc.of(comparatorType);
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
    return Comparators.createGeneric(comparatorType, Ordering.natural().nullsFirst());
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
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8WithNullToEmpty(dimension);
    byte[] lowerBytes = StringUtils.toUtf8WithNullToEmpty(getLower());
    byte[] upperBytes = StringUtils.toUtf8WithNullToEmpty(getUpper());
    byte boundType = 0x1;
    if (this.getLower() == null) {
      boundType = 0x2;
    } else if (this.getUpper() == null) {
      boundType = 0x3;
    }
    byte[] comparatorBytes = StringUtils.toUtf8WithNullToEmpty(comparatorType);

    byte lowerStrictByte = !isLowerStrict() ? 0x0 : (byte) 1;
    byte upperStrictByte = !isUpperStrict() ? 0x0 : (byte) 1;

    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    ByteBuffer boundCacheBuffer = ByteBuffer.allocate(
        9
        + dimensionBytes.length
        + upperBytes.length
        + lowerBytes.length
        + comparatorBytes.length
        + extractionFnBytes.length
    );
    boundCacheBuffer.put(DimFilterCacheHelper.BOUND_CACHE_ID)
                    .put(boundType)
                    .put(upperStrictByte)
                    .put(lowerStrictByte)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(dimensionBytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(upperBytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(lowerBytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(comparatorBytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR)
                    .put(extractionFnBytes);
    return boundCacheBuffer.array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public BoundDimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new BoundDimFilter(
        replaced,
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
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
  }

  @Override
  public Filter toFilter()
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

  public static void main(String[] args) throws Exception
  {
    File file = new File("/Users/navis/Downloads/AL_00_D531_20180331.csv");
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "euc-kr"));
    String line = null;
    int i = 0;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
      if (i++ > 20) {
        break;
      }
    }
    reader.close();
  }
}
