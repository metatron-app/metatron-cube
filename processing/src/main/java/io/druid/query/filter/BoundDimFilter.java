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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.utils.StringUtils;
import io.druid.math.expr.Parser;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.filter.BoundFilter;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BoundDimFilter implements DimFilter
{
  private final String dimension;
  private final String expression;
  private final String upper;
  private final String lower;
  private final boolean lowerStrict;
  private final boolean upperStrict;
  private final String comparatorType;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public BoundDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("expression") String expression,
      @JsonProperty("lower") String lower,
      @JsonProperty("upper") String upper,
      @JsonProperty("lowerStrict") Boolean lowerStrict,
      @JsonProperty("upperStrict") Boolean upperStrict,
      @JsonProperty("alphaNumeric") Boolean alphaNumeric,
      @JsonProperty("comparatorType") String comparatorType,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(
        dimension == null ^ expression == null,
        "Must have a valid, non-null dimension or expression"
    );
    Preconditions.checkArgument(
        expression == null ||
        Parser.findRequiredBindings(expression).size() == 1, "expression should contain only one dimension"
    );
    Preconditions.checkState(lower != null || upper != null, "lower and upper can not be null at the same time");
    this.dimension = dimension;
    this.expression = expression;
    this.upper = upper;
    this.lower = lower;
    this.lowerStrict = (lowerStrict == null) ? false : lowerStrict;
    this.upperStrict = (upperStrict == null) ? false : upperStrict;
    this.comparatorType = comparatorType != null ? comparatorType :
                          alphaNumeric != null && alphaNumeric ? StringComparators.ALPHANUMERIC_NAME :
                          StringComparators.LEXICOGRAPHIC_NAME;
    this.extractionFn = extractionFn;
    Preconditions.checkArgument(
        StringComparators.tryMakeComparator(this.comparatorType) != null,
        "invalid comparator type" + this.comparatorType
    );
  }

  public BoundDimFilter(
      String dimension,
      String lower,
      String upper,
      Boolean lowerStrict,
      Boolean upperStrict,
      Boolean alphaNumeric,
      ExtractionFn extractionFn
  )
  {
    this(dimension, null, lower, upper, lowerStrict, upperStrict, alphaNumeric, null, extractionFn);
  }

  public BoundDimFilter(
      String dimension,
      String lower,
      String upper,
      Boolean lowerStrict,
      Boolean upperStrict,
      Boolean alphaNumeric,
      ExtractionFn extractionFn,
      String comparatorType
      )
  {
    this(dimension, null, lower, upper, lowerStrict, upperStrict, alphaNumeric, comparatorType, extractionFn);
  }

  public static BoundDimFilter between(String dimension, String lower, String upper)
  {
    return new BoundDimFilter(dimension, lower, upper, false, true, false, null);
  }

  public static BoundDimFilter gt(String dimension, String lower)
  {
    return new BoundDimFilter(dimension, lower, null, true, false, false, null);
  }

  public static BoundDimFilter gte(String dimension, String lower)
  {
    return new BoundDimFilter(dimension, lower, null, false, false, false, null);
  }

  public static BoundDimFilter lt(String dimension, String upper)
  {
    return new BoundDimFilter(dimension, null, upper, false, true, false, null);
  }

  public static BoundDimFilter lte(String dimension, String upper)
  {
    return new BoundDimFilter(dimension, null, upper, false, false, false, null);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public String getUpper()
  {
    return upper;
  }

  @JsonProperty
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
  public String getComparatorType()
  {
    return comparatorType;
  }

  public boolean isLexicographic()
  {
    return comparatorType.equalsIgnoreCase(StringComparators.LEXICOGRAPHIC_NAME);
  }

  public Comparator<String> getComparator()
  {
    return StringComparators.makeComparator(comparatorType);
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
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(this.getDimension());
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
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new BoundDimFilter(
        replaced,
        expression,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        null,
        comparatorType,
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
    builder.append(dimension != null ? dimension : expression);
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

  public BoundDimFilter withType(String comparatorType)
  {
    return new BoundDimFilter(
        dimension,
        expression,
        lower,
        upper,
        lowerStrict,
        upperStrict,
        null,
        comparatorType,
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
    if (!comparatorType.equals(that.comparatorType)) {
      return false;
    }
    if (!Objects.equals(dimension, that.dimension)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (getUpper() != null ? !getUpper().equals(that.getUpper()) : that.getUpper() != null) {
      return false;
    }
    if (getLower() != null ? !getLower().equals(that.getLower()) : that.getLower() != null) {
      return false;
    }
    return getExtractionFn() != null
           ? getExtractionFn().equals(that.getExtractionFn())
           : that.getExtractionFn() == null;

  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(dimension, expression);
    result = 31 * result + (getUpper() != null ? getUpper().hashCode() : 0);
    result = 31 * result + (getLower() != null ? getLower().hashCode() : 0);
    result = 31 * result + (isLowerStrict() ? 1 : 0);
    result = 31 * result + (isUpperStrict() ? 1 : 0);
    result = 31 * result + comparatorType.hashCode();
    result = 31 * result + (getExtractionFn() != null ? getExtractionFn().hashCode() : 0);
    return result;
  }
}
