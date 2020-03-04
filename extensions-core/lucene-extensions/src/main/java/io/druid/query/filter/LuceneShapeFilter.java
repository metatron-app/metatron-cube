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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.lucene.LuceneIndexingStrategy;
import io.druid.segment.lucene.ShapeFormat;
import io.druid.segment.lucene.SpatialOperations;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.shape")
public class LuceneShapeFilter extends DimFilter.LuceneFilter implements DimFilter.LogProvider
{
  private final String field;
  private final SpatialOperations operation;
  private final ShapeFormat shapeFormat;
  private final String shapeString;

  @JsonCreator
  public LuceneShapeFilter(
      @JsonProperty("field") String field,
      @JsonProperty("operation") SpatialOperations operation,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat,
      @JsonProperty("shapeString") String shapeString
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.operation = operation == null ? SpatialOperations.COVEREDBY : operation;
    this.shapeFormat = shapeFormat == null ? ShapeFormat.WKT : shapeFormat;
    this.shapeString = Preconditions.checkNotNull(shapeString, "shapeString can not be null");
  }

  @Override
  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public SpatialOperations getOperation()
  {
    return operation;
  }

  @JsonProperty
  public ShapeFormat getShapeFormat()
  {
    return shapeFormat;
  }

  @JsonProperty
  public String getShapeString()
  {
    return shapeString;
  }

  @Override
  protected DimFilter toOptimizedFilter(
      @NotNull String columnName, @Nullable String fieldName, @NotNull String descriptor
  )
  {
    String field = fieldName == null ? columnName : String.format("%s.%s", columnName, fieldName);
    if (descriptor.startsWith(LuceneIndexingStrategy.LATLON_POINT_DESC) && operation == SpatialOperations.COVEREDBY) {
      return new LuceneLatLonPolygonFilter(field, shapeFormat, shapeString);
    } else if (descriptor.startsWith(LuceneIndexingStrategy.SHAPE_DESC)) {
      return new LuceneSpatialFilter(field, operation, shapeFormat, shapeString);
    }
    return null;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_WITHIN_CACHE_ID)
                  .append(field)
                  .append(shapeFormat)
                  .append(shapeString);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneShapeFilter(replaced, operation, shapeFormat, shapeString);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    throw new UnsupportedOperationException("not supports filtering " + forLog());
  }

  @Override
  public DimFilter forLog()
  {
    return new LuceneShapeFilter(field, operation, shapeFormat, "<shape>");
  }

  @Override
  public String toString()
  {
    return "LuceneShapeFilter{" +
           "field='" + field + '\'' +
           ", operation=" + operation +
           ", shapeFormat=" + shapeFormat +
           ", shapeString=" + shapeString +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, operation, shapeFormat, shapeString);
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

    LuceneShapeFilter that = (LuceneShapeFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!operation.equals(that.operation)) {
      return false;
    }
    if (!shapeFormat.equals(that.shapeFormat)) {
      return false;
    }
    if (!shapeString.equals(that.shapeString)) {
      return false;
    }

    return true;
  }
}
