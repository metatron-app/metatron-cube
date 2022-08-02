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
import io.druid.query.kmeans.SloppyMath;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.H3Index;
import io.druid.segment.H3IndexingSpec;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;

/**
 *
 */
@JsonTypeName("h3.point")
public class H3PointDistanceFilter implements DimFilter.BestEffort, H3Query
{
  private final String dimension;
  private final double latitude;
  private final double longitude;
  private final double radiusMeters;

  @JsonCreator
  public H3PointDistanceFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("latitude") double latitude,
      @JsonProperty("longitude") double longitude,
      @JsonProperty("radiusMeters") double radiusMeters
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension should not be null");
    this.latitude = latitude;
    this.longitude = longitude;
    this.radiusMeters = radiusMeters;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public double getLatitude()
  {
    return latitude;
  }

  @JsonProperty
  public double getLongitude()
  {
    return longitude;
  }

  @JsonProperty
  public double getRadiusMeters()
  {
    return radiusMeters;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.H3_DISTANCE_CACHE_ID)
                  .append(dimension)
                  .append(latitude)
                  .append(longitude)
                  .append(radiusMeters);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new H3PointDistanceFilter(dimension, latitude, longitude, radiusMeters);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
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
        BitmapIndexSelector selector = context.indexSelector();
        H3Index index = selector.getExternalIndex(dimension, H3Index.class);
        if (index != null) {
          return index.filterFor(H3PointDistanceFilter.this, context);
        }
        return null;
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        final ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
        if (selector == null) {
          return ValueMatcher.FALSE;
        }
        String descriptor = Preconditions.checkNotNull(factory.getDescriptor(dimension), "Missing descriptor")
                                         .get(H3IndexingSpec.INDEX_NAME);
        Matcher matcher = H3IndexingSpec.H3_PATTERN.matcher(descriptor);
        Preconditions.checkArgument(matcher.matches(), "Invalid descriptor %s", descriptor);

        final int resolution = Integer.valueOf(matcher.group(3));
        final int[] ixs = H3IndexingSpec.extractIx(factory.resolve(dimension), matcher.group(1), matcher.group(2));

        return () -> {
          final Object value = selector.get();
          if (value instanceof List) {
            double lat = ((Number) ((List) value).get(ixs[0])).doubleValue();
            double lon = ((Number) ((List) value).get(ixs[1])).doubleValue();
            return SloppyMath.haversinMeters(latitude, longitude, lat, lon) <= radiusMeters;
          } else if (value instanceof Object[]) {
            double lat = ((Number) ((Object[]) value)[ixs[0]]).doubleValue();
            double lon = ((Number) ((Object[]) value)[ixs[1]]).doubleValue();
            return SloppyMath.haversinMeters(latitude, longitude, lat, lon) <= radiusMeters;
          }
          return false;
        };
      }

      @Override
      public String toString()
      {
        return H3PointDistanceFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "H3PointDistanceFilter{" +
           "dimension=" + dimension +
           ", latitude=" + latitude +
           ", longitude=" + longitude +
           ", radiusMeters=" + radiusMeters +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, latitude, longitude, radiusMeters);
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

    H3PointDistanceFilter that = (H3PointDistanceFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (latitude != that.latitude) {
      return false;
    }
    if (longitude != that.longitude) {
      return false;
    }
    return radiusMeters == that.radiusMeters;
  }
}
