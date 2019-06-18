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
import com.fasterxml.jackson.annotation.JsonProperty;
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

import java.util.Objects;

/**
 */
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
    this.fieldName = Preconditions.checkNotNull(fieldName);
    this.latitude = Preconditions.checkNotNull(latitude);
    this.longitude = Preconditions.checkNotNull(longitude);
    this.crs = crs;
  }

  @Override
  @JsonProperty
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
    return "point(latitude=" + latitude + ",longitude=" + longitude + ")";
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    Preconditions.checkArgument(type.isStruct(), "only struct type can be used");
    StructMetricSerde serde = (StructMetricSerde) Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));

    final int indexLat = serde.indexOf(latitude);
    Preconditions.checkArgument(indexLat >= 0, "invalid field name " + latitude);
    Preconditions.checkArgument(
        serde.type(indexLat).isNumeric(),
        "invalid field type " + serde.type(indexLat) + " for " + latitude
    );

    final int indexLon = serde.indexOf(longitude);
    Preconditions.checkArgument(indexLon >= 0, "invalid field name " + longitude);
    Preconditions.checkArgument(
        serde.type(indexLon).isNumeric(),
        "invalid field type " + serde.type(indexLon) + " for " + longitude
    );
    if (crs == null) {
      return new Function<Object, Field[]>()
      {
        @Override
        public Field[] apply(Object input)
        {
          Object[] struct = (Object[]) input;
          double latitude = ((Number) struct[indexLat]).doubleValue();
          double longitude = ((Number) struct[indexLon]).doubleValue();
          return new Field[]{new LatLonPoint(fieldName, latitude, longitude)};
        }
      };
    }
    final double[] lonlat = new double[2];
    final Expr.NumericBinding binding = Parser.withMap(ImmutableMap.<String, Object>of("lonlat", lonlat));
    final Expr expr = Parser.parse("lonlat.to4326('" + crs + "',lonlat)");
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        Object[] struct = (Object[]) input;
        lonlat[0] = ((Number) struct[indexLon]).doubleValue();
        lonlat[1] = ((Number) struct[indexLat]).doubleValue();
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

    if (!fieldName.equals(that.fieldName)) {
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
    int result = fieldName.hashCode();
    result = 31 * result + latitude.hashCode();
    result = 31 * result + longitude.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
