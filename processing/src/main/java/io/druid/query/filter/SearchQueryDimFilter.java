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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.filter.SearchQueryFilter;

import java.util.Map;
import java.util.Set;

/**
 */
public class SearchQueryDimFilter extends DimFilter.NotOptimizable
{
  private final String dimension;
  private final SearchQuerySpec query;
  private final ExtractionFn extractionFn;

  public SearchQueryDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("query") SearchQuerySpec query,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(query != null, "query must not be null");

    this.dimension = dimension;
    this.query = query;
    this.extractionFn = extractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public SearchQuerySpec getQuery()
  {
    return query;
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
    return builder.append(DimFilterCacheHelper.SEARCH_QUERY_TYPE_ID)
                  .append(dimension).sp()
                  .append(query).sp()
                  .append(extractionFn);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new SearchQueryDimFilter(replaced, query, extractionFn);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new SearchQueryFilter(dimension, query, extractionFn);
  }

  @Override
  public String toString()
  {
    return "SearchQueryDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", query=" + query +
           ", extractionFn='" + extractionFn + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SearchQueryDimFilter)) {
      return false;
    }

    SearchQueryDimFilter that = (SearchQueryDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!query.equals(that.query)) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + query.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }
}
