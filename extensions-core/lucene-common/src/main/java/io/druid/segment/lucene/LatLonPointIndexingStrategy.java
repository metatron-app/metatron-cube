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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonPoint;

import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("latlon")
public class LatLonPointIndexingStrategy implements LuceneIndexingStrategy
{
  private final String fieldName;
  private final String latitude;
  private final String longitude;
  private final String crs;

  @JsonCreator
  public LatLonPointIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("latitude") String latitude,
      @JsonProperty("longitude") String longitude,
      @JsonProperty("crs") String crs
  )
  {
    this.fieldName = fieldName;
    this.latitude = Preconditions.checkNotNull(latitude);
    this.longitude = Preconditions.checkNotNull(longitude);
    this.crs = crs;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getLatitude()
  {
    return latitude;
  }

  @JsonProperty
  public String getLongitude()
  {
    return longitude;
  }

  @JsonProperty
  public String getCrs()
  {
    return crs;
  }

  @Override
  public String getFieldDescriptor()
  {
    return String.format("%s(latitude=%s,longitude=%s)", LATLON_POINT_DESC, latitude, longitude);
  }

  @Override
  public LatLonPointIndexingStrategy withFieldName(String fieldName)
  {
    return new LatLonPointIndexingStrategy(fieldName, latitude, longitude, crs);
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    Preconditions.checkArgument(type.isStruct(), "only struct type can be used but %s", type);
    StructMetricSerde serde = (StructMetricSerde) Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));

    final int indexLat = serde.indexOf(latitude);
    Preconditions.checkArgument(indexLat >= 0, "invalid field name %s", latitude);
    Preconditions.checkArgument(
        serde.type(indexLat).isNumeric(), "invalid field type %s for %s", serde.type(indexLat), latitude
    );

    final int indexLon = serde.indexOf(longitude);
    Preconditions.checkArgument(indexLon >= 0, "invalid field name %s", longitude);
    Preconditions.checkArgument(
        serde.type(indexLon).isNumeric(), "invalid field type %s for %s", serde.type(indexLon), longitude
    );
    if (crs == null) {
      return new Function<Object, Field[]>()
      {
        @Override
        public Field[] apply(Object input)
        {
          double latitude;
          double longitude;
          if (input instanceof List) {
            List struct = (List) input;
            latitude = ((Number) struct.get(indexLat)).doubleValue();
            longitude = ((Number) struct.get(indexLon)).doubleValue();
          } else {
            Object[] struct = (Object[]) input;
            latitude = ((Number) struct[indexLat]).doubleValue();
            longitude = ((Number) struct[indexLon]).doubleValue();
          }
          return new Field[]{new LatLonPoint(fieldName, latitude, longitude)};
        }
      };
    }
    final double[] lonlat = new double[2];
    final Expr.NumericBinding binding = Parser.withMap(ImmutableMap.<String, Object>of("lonlat", lonlat));
    final Expr expr = Parser.parse(String.format("lonlat.to4326('%s',lonlat)", crs));
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        if (input instanceof List) {
          List struct = (List) input;
          lonlat[0] = ((Number) struct.get(indexLon)).doubleValue();
          lonlat[1] = ((Number) struct.get(indexLat)).doubleValue();
        } else {
          Object[] struct = (Object[]) input;
          lonlat[0] = ((Number) struct[indexLon]).doubleValue();
          lonlat[1] = ((Number) struct[indexLat]).doubleValue();
        }
        double[] converted = (double[]) expr.eval(binding).value();
        return new Field[]{new LatLonPoint(fieldName, converted[1], converted[0])};
      }
    };
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

    LatLonPointIndexingStrategy that = (LatLonPointIndexingStrategy) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!latitude.equals(that.latitude)) {
      return false;
    }
    if (!longitude.equals(that.longitude)) {
      return false;
    }
    if (!Objects.equals(crs, that.crs)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, latitude, longitude);
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
