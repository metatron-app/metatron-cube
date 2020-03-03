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
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.geojson")
public class LuceneGeoJsonPolygonFilter extends DimFilter.LuceneFilter implements DimFilter.LogProvider
{
  private final String field;
  private final String geoJson;

  @JsonCreator
  public LuceneGeoJsonPolygonFilter(
      @JsonProperty("field") String field,
      @JsonProperty("polygon") String geoJson
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.geoJson = Preconditions.checkNotNull(geoJson, "geoJson can not be null");
  }

  @Override
  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public String getGeoJson()
  {
    return geoJson;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_GEOJSON_CACHE_ID)
                  .append(field, geoJson);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneGeoJsonPolygonFilter(replaced, geoJson);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
      {
        // column-name.field-name or field-name (regarded same with column-name)
        String columnName = field;
        String fieldName = field;
        LuceneIndex lucene = selector.getLuceneIndex(columnName);
        for (int index = field.indexOf('.'); lucene == null && index > 0; index = field.indexOf('.', index + 1)) {
          columnName = field.substring(0, index);
          fieldName = field.substring(index + 1);
          lucene = selector.getLuceneIndex(columnName);
        }
        Preconditions.checkNotNull(lucene, "no lucene index for [%s]", field);

        try {
          Query query = LatLonPoint.newPolygonQuery(fieldName, Polygon.fromGeoJSON(geoJson));
          return lucene.filterFor(query, baseBitmap);
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
  public DimFilter toExprFilter(String columnName, String fieldName, String descriptor)
  {
    final String latLon = toLatLonField(columnName, fieldName, descriptor);
    return new MathExprFilter(
        String.format("geom_contains(geom_fromGeoJson('%s'), geom_fromLatLon(%s))", geoJson, latLon)
    );
  }

  @Override
  public DimFilter forLog()
  {
    return new LuceneGeoJsonPolygonFilter(field, "<shape>");
  }

  @Override
  public String toString()
  {
    return "LuceneGeoJsonPolygonFilter{" +
           "field='" + field + '\'' +
           ", geoJson='" + geoJson + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, geoJson);
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
    return true;
  }
}
