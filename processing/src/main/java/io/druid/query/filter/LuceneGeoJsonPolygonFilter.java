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
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.query.RowResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;

import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("lucene.geojson")
public class LuceneGeoJsonPolygonFilter extends DimFilter.LuceneFilter implements DimFilter.LogProvider
{
  private final String geoJson;

  @JsonCreator
  public LuceneGeoJsonPolygonFilter(
      @JsonProperty("field") String field,
      @JsonProperty("polygon") String geoJson,
      @JsonProperty("scoreField") String scoreField
  )
  {
    super(field, scoreField);
    this.geoJson = Preconditions.checkNotNull(geoJson, "geoJson can not be null");
  }

  @JsonProperty
  public String getGeoJson()
  {
    return geoJson;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.LUCENE_GEOJSON_CACHE_ID)
                  .append(field, geoJson, scoreField);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneGeoJsonPolygonFilter(replaced, geoJson, scoreField);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
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

        try {
          Query query = LatLonPoint.newPolygonQuery(fieldName, Polygon.fromGeoJSON(geoJson));
          return lucene.filterFor(query, context, scoreField);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        throw new UnsupportedOperationException("value matcher");
      }

      @Override
      public String toString()
      {
        return LuceneGeoJsonPolygonFilter.this.toString();
      }
    };
  }

  @Override
  public DimFilter toExprFilter(RowResolver resolver, String columnName, String fieldName, String descriptor)
  {
    final String point = toPointExpr(resolver, columnName, fieldName, descriptor);
    return new MathExprFilter(String.format("geom_contains_point(geom_fromGeoJson('%s'), %s)", geoJson, point));
  }

  @Override
  public DimFilter forLog()
  {
    return new LuceneGeoJsonPolygonFilter(field, StringUtils.forLog(geoJson), scoreField);
  }

  @Override
  public String toString()
  {
    return "LuceneGeoJsonPolygonFilter{" +
           "field='" + field + '\'' +
           ", geoJson=" + StringUtils.forLog(geoJson) +
           (scoreField == null ? "" : ", scoreField='" + scoreField + '\'') +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, geoJson, scoreField);
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

    LuceneGeoJsonPolygonFilter that = (LuceneGeoJsonPolygonFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!geoJson.equals(that.geoJson)) {
      return false;
    }
    if (!Objects.equals(scoreField, that.scoreField)) {
      return false;
    }
    return true;
  }
}
