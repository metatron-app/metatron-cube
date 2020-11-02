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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.Ranges;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.BooleanColumnSupport;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.SelectorFilter;
import io.netty.util.internal.StringUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 */
public class SelectorDimFilter extends SingleInput implements RangeFilter, BooleanColumnSupport
{
  public static DimFilter or(final String dimension, String... values)
  {
    return DimFilters.or(
        Lists.newArrayList(Iterables.transform(Arrays.asList(values), new Function<String, DimFilter>()
        {
          @Override
          public DimFilter apply(String input)
          {
            return of(dimension, input);
          }
        })));
  }

  public static SelectorDimFilter of(String dimension, String value)
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
    return builder.append(DimFilterCacheHelper.SELECTOR_CACHE_ID)
                  .append(dimension).sp()
                  .append(value).sp()
                  .append(extractionFn);
  }

  @Override
  public DimFilter optimize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    return new InDimFilter(dimension, ImmutableList.of(value), extractionFn).optimize(segment, virtualColumns);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    if (extractionFn == null) {
      return new SelectorFilter(dimension, value);
    } else {
      final String valueOrNull = Strings.emptyToNull(value);
      final Predicate<String> predicate = new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return Objects.equals(valueOrNull, input);
        }
      };
      return new DimensionPredicateFilter(dimension, predicate, extractionFn);
    }
  }

  @Override
  public ImmutableBitmap toBooleanFilter(TypeResolver resolver, BitmapIndexSelector selector)
  {
    final Boolean bool = StringUtil.isNullOrEmpty(value) ? null : Rows.parseBoolean(value);
    return selector.getBitmapIndex(dimension, bool);
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
  @SuppressWarnings("unchecked")
  public List<Range> toRanges(TypeResolver resolver)
  {
    Preconditions.checkArgument(extractionFn == null, "extractionFn");
    ValueDesc resolved = resolver.resolve(dimension, ValueDesc.STRING);
    if (resolved.isStringOrDimension()) {
      resolved = ValueDesc.STRING;
    }
    return Arrays.<Range>asList(Ranges.of(resolved.type().cast(value), "=="));
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
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;
  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + value.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }
}
