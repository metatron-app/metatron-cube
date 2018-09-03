/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
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
import io.druid.common.utils.StringUtils;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.lucene.ShapeFormat;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.latlon.polygon")
public class LuceneLatLonPolygonFilter implements DimFilter.LuceneFilter
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
    Preconditions.checkArgument(field.contains("."), "should reference lat-lon point in struct field");
  }

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
  public byte[] getCacheKey()
  {
    byte[] fieldBytes = StringUtils.toUtf8(field);
    byte[] shapeStringBytes = StringUtils.toUtf8(shapeString);
    return ByteBuffer.allocate(3 + fieldBytes.length + shapeStringBytes.length)
                     .put(DimFilterCacheHelper.LUCENE_GEOJSON_CACHE_ID)
                     .put((byte) shapeFormat.ordinal())
                     .put(fieldBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(shapeStringBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter()
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
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(
          BitmapIndexSelector selector,
          EnumSet<BitmapType> using,
          ImmutableBitmap baseBitmap
      )
      {
        // column-name.field-name
        int index = field.indexOf(".");
        String columnName = field.substring(0, index);
        String fieldName = field.substring(index + 1);
        LuceneIndex lucene = Preconditions.checkNotNull(
            selector.getLuceneIndex(columnName),
            "no lucene index for " + columnName
        );
        Query query = LatLonPoint.newPolygonQuery(fieldName, polygons);
        return lucene.filterFor(query, baseBitmap);
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
  public String toString()
  {
    return "LuceneLatLonPolygonFilter{" +
           "field='" + field + '\'' +
           ", shapeFormat='" + shapeFormat + '\'' +
           ", shapeString='" + shapeString + '\'' +
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
