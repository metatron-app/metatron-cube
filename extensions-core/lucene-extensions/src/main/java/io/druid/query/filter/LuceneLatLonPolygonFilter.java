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
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.query.GeomUtils;
import io.druid.query.RowResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.ShapeFormat;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.latlon.polygon")
public class LuceneLatLonPolygonFilter extends DimFilter.LuceneFilter implements DimFilter.LogProvider
{
  private final String field;
  private final ShapeFormat shapeFormat;
  private final String shapeString;

  @JsonCreator
  public LuceneLatLonPolygonFilter(
      @JsonProperty("field") String field,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat,
      @JsonProperty("shapeString") String shapeString
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
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
  public String getShapeString()
  {
    return shapeString;
  }

  @JsonProperty
  public ShapeFormat getShapeFormat()
  {
    return shapeFormat;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_GEOJSON_CACHE_ID)
                  .append(field, shapeString);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneLatLonPolygonFilter(replaced, shapeFormat, shapeString);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter(final TypeResolver resolver)
  {
    final Polygon[] polygons;
    try {
      polygons = ShapeFormat.toLucenePolygons(JtsSpatialContext.GEO, shapeFormat, shapeString);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return new Filter()
    {

      @Override
      public ImmutableBitmap getBitmapIndex(FilterContext context)
      {
        // column-name.field-name or field-name (regarded same with column-name)
        String columnName = field;
        String fieldName = field;
        BitmapIndexSelector selector = context.indexSelector();
        LuceneIndex lucene = selector.getLuceneIndex(columnName);
        for (int index = field.indexOf('.'); lucene == null && index > 0; index = field.indexOf('.', index + 1)) {
          columnName = field.substring(0, index);
          fieldName = field.substring(index + 1);
          lucene = selector.getLuceneIndex(columnName);
        }
        Preconditions.checkNotNull(lucene, "no lucene index for [%s]", field);

        Query query = LatLonPoint.newPolygonQuery(fieldName, polygons);
        return lucene.filterFor(query, context);
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        throw new UnsupportedOperationException("value matcher");
      }

      @Override
      public String toString()
      {
        return LuceneLatLonPolygonFilter.this.toString();
      }
    };
  }

  public LuceneLatLonPolygonFilter withWKT(String shapeString)
  {
    return new LuceneLatLonPolygonFilter(field, ShapeFormat.WKT, shapeString);
  }

  @Override
  public DimFilter toExprFilter(RowResolver resolver, String columnName, String fieldName, String descriptor)
  {
    final String polygon = GeomUtils.fromString(shapeFormat, shapeString);
    final String point = toPointExpr(resolver, columnName, fieldName, descriptor);
    return new MathExprFilter(String.format("geom_contains_point(%s, %s)", polygon, point));
  }

  @Override
  public DimFilter forLog()
  {
    return new LuceneLatLonPolygonFilter(field, shapeFormat, StringUtils.forLog(shapeString));
  }

  @Override
  public String toString()
  {
    return "LuceneLatLonPolygonFilter{" +
           "field='" + field + '\'' +
           ", shapeFormat=" + shapeFormat +
           ", shapeString=" + StringUtils.forLog(shapeString) +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, shapeFormat, shapeString);
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

    LuceneLatLonPolygonFilter that = (LuceneLatLonPolygonFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (shapeFormat != that.shapeFormat) {
      return false;
    }
    if (!shapeString.equals(that.shapeString)) {
      return false;
    }
    return true;
  }
}
