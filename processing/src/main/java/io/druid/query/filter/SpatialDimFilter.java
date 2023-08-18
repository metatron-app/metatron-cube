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
import com.metamx.collections.spatial.search.Bound;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.SpatialFilter;

import java.util.Map;
import java.util.Set;

/**
 */
public class SpatialDimFilter implements DimFilter
{
  private final String dimension;
  private final Bound bound;

  @JsonCreator
  public SpatialDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bound") Bound bound
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension must not be null");
    this.bound = Preconditions.checkNotNull(bound, "bound must not be null");
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.SPATIAL_CACHE_ID)
                  .append(dimension).sp()
                  .append(bound.getCacheKey());
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new SpatialDimFilter(replaced, bound);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public Bound getBound()
  {
    return bound;
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new SpatialFilter(dimension, bound);
  }

  @Override
  public double cost(FilterContext context)
  {
    return FULLSCAN * 4;
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

    SpatialDimFilter that = (SpatialDimFilter) o;

    if (bound != null ? !bound.equals(that.bound) : that.bound != null) {
      return false;
    }
    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (bound != null ? bound.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SpatialDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", bound=" + bound +
           '}';
  }
}
