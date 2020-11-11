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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;

import java.util.List;

/**
 * This class is deprecated, use SelectorDimFilter instead: {@link io.druid.query.filter.SelectorDimFilter}
 */
@Deprecated
public class ExtractionDimFilter extends SingleInput
{
  private final String dimension;
  private final String value;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public ExtractionDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      // for backwards compatibility
      @Deprecated @JsonProperty("dimExtractionFn") ExtractionFn dimExtractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(
        extractionFn != null || dimExtractionFn != null,
        "extraction function must not be null"
    );

    this.dimension = dimension;
    this.value = value;
    this.extractionFn = extractionFn != null ? extractionFn : dimExtractionFn;
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.EXTRACTION_CACHE_ID)
                  .append(dimension).sp()
                  .append(value).sp()
                  .append(extractionFn);
  }

  @Override
  public DimFilter optimize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    return new SelectorDimFilter(dimension, value, extractionFn).optimize(segment, virtualColumns);
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new SelectorDimFilter(dimension, value, extractionFn);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new SelectorDimFilter(dimension, value, extractionFn).toFilter(resolver);
  }

  @Override
  public String toString()
  {
    return String.format("%s(%s) = %s", extractionFn, dimension, value);
  }
}
