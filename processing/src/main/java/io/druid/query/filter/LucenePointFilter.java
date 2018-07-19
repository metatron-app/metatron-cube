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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.StringUtils;
import io.druid.query.QueryCacheHelper;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.lucene.PointQueryType;
import org.apache.lucene.search.Query;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class LucenePointFilter implements DimFilter.LuceneFilter
{
  public static LucenePointFilter bbox(String field, double[] latitudes, double[] longitudes)
  {
    return new LucenePointFilter(field, PointQueryType.BBOX, null, null, latitudes, longitudes, -1d);
  }

  public static LucenePointFilter distance(String field, double latitude, double longitude, double radiusMeters)
  {
    return new LucenePointFilter(field, PointQueryType.DISTANCE, latitude, longitude, null, null, radiusMeters);
  }

  private final String field;
  private final PointQueryType type;
  private final double[] latitudes;
  private final double[] longitudes;
  private final double radiusMeters;

  @JsonCreator
  public LucenePointFilter(
      @JsonProperty("field") String field,
      @JsonProperty("type") PointQueryType type,
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
    this.type = Preconditions.checkNotNull(type, "type can not be null");
    this.latitudes = latitude != null ? new double[]{latitude} : latitudes;
    this.longitudes = longitude != null ? new double[]{longitude} : longitudes;
    this.radiusMeters = type == PointQueryType.DISTANCE ? radiusMeters : 0;
    Preconditions.checkArgument(field.contains("."), "should reference lat-lon point in struct field");
    Preconditions.checkArgument(getLatitudes().length == getLongitudes().length, "invalid coordinates");
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public PointQueryType getType()
  {
    return type;
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
  public byte[] getCacheKey()
  {
    byte[] fieldBytes = StringUtils.toUtf8(field);
    byte[] longitudesBytes = QueryCacheHelper.toBytes(longitudes, false);
    byte[] latitudesBytes = QueryCacheHelper.toBytes(latitudes, false);
    return ByteBuffer.allocate(2 + fieldBytes.length + longitudesBytes.length + latitudesBytes.length + Doubles.BYTES)
                     .put(DimFilterCacheHelper.LUCENE_POINT_CACHE_ID)
                     .put(fieldBytes)
                     .put((byte) type.ordinal())
                     .put(longitudesBytes)
                     .put(latitudesBytes)
                     .putDouble(radiusMeters)
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
        int index = field.indexOf('.');
        String columnName = field.substring(0, index);
        String fieldName = field.substring(index + 1);
        LuceneIndex lucene = Preconditions.checkNotNull(
            selector.getLuceneIndex(columnName),
            "no lucene index for " + columnName
        );
        Query query = type.toQuery(fieldName, latitudes, longitudes, radiusMeters);
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
  public String toString()
  {
    return "LucenePointFilter{" +
           "field='" + field + '\'' +
           ", type='" + type + '\'' +
           ", latitudes=" + Arrays.toString(latitudes) +
           ", longitudes=" + Arrays.toString(longitudes) +
           (type == PointQueryType.DISTANCE ? ", radiusMeters=" + radiusMeters : "") +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, type, latitudes, longitudes, radiusMeters);
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
    if (!type.equals(that.type)) {
      return false;
    }
    if (Arrays.equals(latitudes, that.latitudes)) {
      return false;
    }
    if (Arrays.equals(longitudes, that.longitudes)) {
      return false;
    }

    return radiusMeters == that.radiusMeters;
  }
}
