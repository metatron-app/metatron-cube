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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonPoint;

/**
 */
public class LatLonPointIndexingStrategy implements LuceneIndexingStrategy
{
  private final String fieldName;
  private final String latitude;
  private final String longitude;

  @JsonCreator
  public LatLonPointIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("latitude") String latitude,
      @JsonProperty("longitude") String longitude
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName);
    this.latitude = Preconditions.checkNotNull(latitude);
    this.longitude = Preconditions.checkNotNull(longitude);
  }

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
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        Object[] struct = (Object[]) input;
        return new Field[]{
            new LatLonPoint(
                fieldName,
                ((Number) struct[indexLat]).doubleValue(),
                ((Number) struct[indexLon]).doubleValue()
            )
        };
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
}
