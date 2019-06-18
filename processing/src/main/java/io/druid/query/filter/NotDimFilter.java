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
import io.druid.data.TypeResolver;
import io.druid.math.expr.Expression.NotExpression;
import io.druid.segment.filter.NotFilter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class NotDimFilter implements DimFilter, NotExpression
{
  final private DimFilter field;

  @JsonCreator
  public NotDimFilter(
      @JsonProperty("field") DimFilter field
  )
  {
    Preconditions.checkArgument(field != null, "NOT operator requires at least one field");
    this.field = field;
  }

  @JsonProperty("field")
  public DimFilter getField()
  {
    return field;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] subKey = field.getCacheKey();

    return ByteBuffer.allocate(1 + subKey.length).put(DimFilterCacheHelper.NOT_CACHE_ID).put(subKey).array();
  }

  @Override
  public DimFilter optimize()
  {
    return DimFilters.not(field.optimize());
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    DimFilter optimized = field.withRedirection(mapping);
    return field != optimized ? new NotDimFilter(optimized) : this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    field.addDependent(handler);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new NotFilter(field.toFilter(resolver));
  }

  @Override
  @SuppressWarnings("unchecked")
  public DimFilter getChild()
  {
    return field;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<DimFilter> getChildren()
  {
    return Arrays.asList(field);
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

    NotDimFilter that = (NotDimFilter) o;

    if (field != null ? !field.equals(that.field) : that.field != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return field != null ? field.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "!(" + field + ")";
  }

  public static DimFilter of(DimFilter filter) {
    return filter == null ? null : new NotDimFilter(filter);
  }
}
