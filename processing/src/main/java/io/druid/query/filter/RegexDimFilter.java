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
import io.druid.java.util.common.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.RegexFilter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 */
public class RegexDimFilter extends DimFilter.NotOptimizable
{
  private final String dimension;
  private final String pattern;
  private final ExtractionFn extractionFn;

  private final Pattern compiledPattern;

  @JsonCreator
  public RegexDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("pattern") String pattern,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension must not be null");
    this.pattern = Preconditions.checkNotNull(pattern, "pattern must not be null");
    this.extractionFn = extractionFn;
    this.compiledPattern = Pattern.compile(pattern);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
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
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] patternBytes = StringUtils.toUtf8(pattern);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + patternBytes.length + extractionFnBytes.length)
                     .put(DimFilterCacheHelper.REGEX_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(patternBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new RegexDimFilter(replaced, pattern, extractionFn);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new RegexFilter(dimension, compiledPattern, extractionFn);
  }

  @Override
  public String toString()
  {
    return "RegexDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", pattern='" + pattern + '\'' +
           ", extractionFn='" + extractionFn + '\'' +
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
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + pattern.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }
}
