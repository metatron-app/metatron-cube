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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.PrefixFilter;

import java.util.Objects;

/**
 */
@JsonTypeName("prefix")
public class PrefixDimFilter extends SingleInput
{
  public static PrefixDimFilter of(String dimension, String prefix)
  {
    return new PrefixDimFilter(dimension, prefix, null);
  }

  private final String dimension;
  private final String prefix;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public PrefixDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("prefix") String prefix,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension must not be null");
    this.prefix = Preconditions.checkNotNull(prefix, "prefix must not be null");
    this.extractionFn = extractionFn;
    Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix));
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getPrefix()
  {
    return prefix;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new PrefixDimFilter(dimension, prefix, extractionFn);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.PREFIX_CACHE_ID)
                  .append(dimension).sp()
                  .append(prefix).sp()
                  .append(extractionFn);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    if (extractionFn != null) {
      return new DimensionPredicateFilter(dimension, v -> v != null && v.startsWith(prefix), null, extractionFn);
    } else {
      return PrefixFilter.of(dimension, prefix);
    }
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return String.format("\"%s\"(%s).startsWith('%s')", dimension, extractionFn, prefix);
    } else {
      return String.format("\"%s\".startsWith('%s')", dimension, prefix);
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

    PrefixDimFilter that = (PrefixDimFilter) o;

    if (!Objects.equals(dimension, that.dimension)) {
      return false;
    }
    if (!Objects.equals(prefix, that.prefix)) {
      return false;
    }
    return Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, prefix, extractionFn);
  }
}
