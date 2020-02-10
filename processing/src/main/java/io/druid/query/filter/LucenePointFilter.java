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
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.lucene.PointQueryType;
import org.apache.lucene.search.Query;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.point")
public class LucenePointFilter extends DimFilter.LuceneFilter
{
  public static LucenePointFilter bbox(String field, double[] latitudes, double[] longitudes)
  {
    return new LucenePointFilter(field, PointQueryType.BBOX, null, null, latitudes, longitudes, -1d);
  }

  public static LucenePointFilter distance(String field, double latitude, double longitude, double radiusMeters)
  {
    return new LucenePointFilter(field, PointQueryType.DISTANCE, latitude, longitude, null, null, radiusMeters);
  }

  public static LucenePointFilter polygon(String field, double[] latitudes, double[] longitudes)
  {
    return new LucenePointFilter(field, PointQueryType.POLYGON, null, null, latitudes, longitudes, -1d);
  }

  public static LuceneNearestFilter nearest(String field, double latitude, double longitude, int count)
  {
    return new LuceneNearestFilter(field, latitude, longitude, count);
  }

  private final String field;
  private final PointQueryType query;
  private final double[] latitudes;
  private final double[] longitudes;
  private final double radiusMeters;

  @JsonCreator
  public LucenePointFilter(
      @JsonProperty("field") String field,
      @JsonProperty("query") PointQueryType query,
      @JsonProperty("latitude") Double latitude,
      @JsonProperty("longitude") Double longitude,
      @JsonProperty("latitudes") double[] latitudes,
      @JsonProperty("longitudes") double[] longitudes,
      @JsonProperty("radiusMeters") double radiusMeters
  )
  {
    Preconditions.checkArgument(
        latitude == null ^ latitudes == null, "Must have a valid, non-null latitude or latitudes"
    );
    Preconditions.checkArgument(
        longitude == null ^ longitudes == null, "Must have a valid, non-null longitude or longitudes"
    );
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.latitudes = latitude != null ? new double[]{latitude} : latitudes;
    this.longitudes = longitude != null ? new double[]{longitude} : longitudes;
    Preconditions.checkArgument(getLatitudes().length == getLongitudes().length, "invalid coordinates");
    if (query != null) {
      this.query = query;
    } else if (getLatitudes().length == 1) {
      this.query = PointQueryType.DISTANCE;
    } else if (getLatitudes().length == 2) {
      this.query = PointQueryType.BBOX;
    } else {
      this.query = PointQueryType.POLYGON;
    }
    this.radiusMeters = this.query == PointQueryType.DISTANCE ? radiusMeters : 0;
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public PointQueryType getQuery()
  {
    return query;
  }

  @JsonProperty
  public double[] getLatitudes()
  {
    return latitudes;
  }

  @JsonProperty
  public double[] getLongitudes()
  {
    return longitudes;
  }

  @JsonProperty
  public double getRadiusMeters()
  {
    return radiusMeters;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_POINT_CACHE_ID)
                  .append(field)
                  .append(query)
                  .append(longitudes)
                  .append(latitudes)
                  .append(radiusMeters);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LucenePointFilter(replaced, query, null, null, latitudes, longitudes, radiusMeters);
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

        Query query = LucenePointFilter.this.query.toQuery(fieldName, latitudes, longitudes, radiusMeters);
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
        return LucenePointFilter.this.toString();
      }
    };
  }

  @Override
  public DimFilter toExpressionFilter()
  {
    int index = field.indexOf(".");
    String columnName = index < 0 ? field : field.substring(0, index);

    switch (query) {
      case DISTANCE:
        return new MathExprFilter(
            String.format(
                "shape_contains(shape_buffer(shape_fromLatLon(%f, %f), %f), shape_fromLatLon(\"%s\"))",
                latitudes[0], longitudes[0], radiusMeters, columnName
            )
        );
      case BBOX:
      case POLYGON:
        StringBuilder multiPoint = new StringBuilder();
        for (int i = 0; i < longitudes.length; i++) {
          if (multiPoint.length() > 0) {
            multiPoint.append(", ");
          }
          multiPoint.append('(').append(longitudes[i]).append(' ').append(latitudes[i]).append(')');
        }
        return new MathExprFilter(
            String.format(
                "shape_contains(shape_envelop('MULTIPOINT (%s)'), shape_fromLatLon(\"%s\"))", multiPoint, columnName
            )
        );
      default:
        return super.toExpressionFilter();
    }
  }

  @Override
  public String toString()
  {
    return "LucenePointFilter{" +
           "field='" + field + '\'' +
           ", query='" + query + '\'' +
           ", latitudes=" + Arrays.toString(latitudes) +
           ", longitudes=" + Arrays.toString(longitudes) +
           (query == PointQueryType.DISTANCE ? ", radiusMeters=" + radiusMeters : "") +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, query, latitudes, longitudes, radiusMeters);
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

    LucenePointFilter that = (LucenePointFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!query.equals(that.query)) {
      return false;
    }
    if (!Arrays.equals(latitudes, that.latitudes)) {
      return false;
    }
    if (!Arrays.equals(longitudes, that.longitudes)) {
      return false;
    }

    return radiusMeters == that.radiusMeters;
  }
}
