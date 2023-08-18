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
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.FilterContext;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class RegexDimFilter extends SingleInput
{
  private final String dimension;
  private final String pattern;
  private final ExtractionFn extractionFn;
  private final boolean match;

  private final Supplier<Pattern> patternSupplier;

  @JsonCreator
  public RegexDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("pattern") String pattern,
      @JsonProperty("match") boolean match,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension must not be null");
    this.pattern = Preconditions.checkNotNull(pattern, "pattern must not be null");
    this.match = match;
    this.extractionFn = extractionFn;
    this.patternSupplier = Suppliers.memoize(() -> Pattern.compile(pattern));
  }

  public RegexDimFilter(String dimension, String pattern, ExtractionFn extractionFn)
  {
    this(dimension, pattern, false, extractionFn);
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new RegexDimFilter(dimension, pattern, match, extractionFn);
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
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
    return builder.append(DimFilterCacheKey.REGEX_CACHE_ID)
                  .append(dimension, pattern)
                  .append(match)
                  .append(extractionFn);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    final Matcher matcher = patternSupplier.get().matcher("");
    final Predicate<String> predicate = match ? v -> v != null && matcher.reset(v).matches()
                                              : v -> v != null && matcher.reset(v).find();
    return new DimensionPredicateFilter(dimension, predicate, null, extractionFn);
  }

  @Override
  public double cost(FilterContext context)
  {
    return context.scanningCost(dimension, extractionFn);
  }

  @Override
  public String toString()
  {
    return "RegexDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", pattern='" + pattern + '\'' +
           ", match='" + match + '\'' +
           (extractionFn == null ? "" : ", extractionFn='" + extractionFn + '\'') +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RegexDimFilter)) {
      return false;
    }

    RegexDimFilter that = (RegexDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!pattern.equals(that.pattern)) {
      return false;
    }
    if (match != that.match) {
      return false;
    }
    return Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, pattern, match, extractionFn);
  }
}
