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
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Ranges;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.filter.DimFilter.BooleanColumnSupport;
import io.druid.query.filter.DimFilter.IndexedIDSupport;
import io.druid.query.filter.DimFilter.Mergeable;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.SelectorFilter;
import io.netty.util.internal.StringUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 *
 */
public class SelectorDimFilter extends SingleInput
    implements RangeFilter, BooleanColumnSupport, Mergeable, IndexedIDSupport
{
  public static DimFilter or(String dimension, String... values)
  {
    return DimFilters.or(GuavaUtils.transform(Arrays.asList(values), input -> of(dimension, input)));
  }

  public static DimFilter of(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value, null);
  }

  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");

    this.dimension = dimension;
    this.value = Strings.nullToEmpty(value);
    this.extractionFn = extractionFn;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.SELECTOR_CACHE_ID)
                  .append(dimension).sp()
                  .append(value).sp()
                  .append(extractionFn);
  }

  @Override
  public DimFilter optimize()
  {
    List<String> rewritten = ExtractionFns.reverseMap(extractionFn, Arrays.asList(value));
    if (rewritten == null) {
      return this;
    }
    if (rewritten.isEmpty()) {
      return DimFilters.NONE;
    }
    if (rewritten.size() == 1) {
      return new SelectorDimFilter(dimension, rewritten.get(0), null);
    }
    Collections.sort(rewritten);
    return new InDimFilter(dimension, null, rewritten, null);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    if (extractionFn == null) {
      return new SelectorFilter(this, dimension, value);
    } else {
      final String valueOrNull = Strings.emptyToNull(value);
      return new DimensionPredicateFilter(dimension, v -> Objects.equals(valueOrNull, v), null, extractionFn);
    }
  }

  @Override
  public ImmutableBitmap toBooleanFilter(TypeResolver resolver, BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension, Rows.parseBoolean(value));
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
    return new SelectorDimFilter(dimension, value, extractionFn);
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public boolean possible(TypeResolver resolver)
  {
    return extractionFn == null;
  }

  @Override
  public List<Range> toRanges(TypeResolver resolver)
  {
    Preconditions.checkArgument(extractionFn == null, "extractionFn");
    ValueDesc resolved = resolver.resolve(dimension, ValueDesc.STRING).unwrapDimension();
    Comparable c = resolved.isString() ? value : (Comparable) resolved.cast(value);
    return Arrays.<Range>asList(Ranges.of(c, "=="));
  }

  @Override
  public String toString()
  {
    final String explain = StringUtil.isNullOrEmpty(value) ? "NULL" : String.format("'%s'", value);
    if (extractionFn != null) {
      return String.format("%s(%s)==%s", extractionFn, dimension, explain);
    } else {
      return String.format("%s==%s", dimension, explain);
    }
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

    SelectorDimFilter that = (SelectorDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!value.equals(that.value)) {
      return false;
    }
    return Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, extractionFn, value);
  }

  @Override
  public boolean supports(OP op, DimFilter other)
  {
    if (other instanceof SelectorDimFilter) {
      SelectorDimFilter select = (SelectorDimFilter) other;
      return dimension.equals(select.dimension) && Objects.equals(extractionFn, select.extractionFn);
    }
    return false;
  }

  @Override
  public DimFilter merge(OP op, DimFilter other)
  {
    if (other instanceof SelectorDimFilter) {
      SelectorDimFilter select = (SelectorDimFilter) other;
      int compare = GuavaUtils.nullFirstNatural().compare(value, select.value);
      if (compare == 0) {
        return this;
      }
      switch (op) {
        case AND:
          return DimFilters.NONE;
        case OR:
          List<String> values = compare < 0 ? Arrays.asList(value, select.value) : Arrays.asList(select.value, value);
          return new InDimFilter(dimension, values, extractionFn);
      }
    }
    throw new ISE("merge?? %s %s %s", this, op, other);
  }
}
