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
import io.druid.common.utils.StringUtils;
import io.druid.data.ParsingFail;
import io.druid.data.ValueDesc;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonPoint;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import java.util.Objects;

/**
 */
@JsonTypeName("latlon.shape")
public class LatLonShapeIndexingStrategy implements LuceneIndexingStrategy
{
  private final String fieldName;
  private final ShapeFormat shapeFormat;

  @JsonCreator
  public LatLonShapeIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat
  )
  {
    this.fieldName = fieldName;
    this.shapeFormat = Preconditions.checkNotNull(shapeFormat, "shapeFormat cannot be null");
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public ShapeFormat getShapeFormat()
  {
    return shapeFormat;
  }

  @Override
  public String getFieldDescriptor()
  {
    return "point(format=" + shapeFormat + ")";
  }

  @Override
  public LuceneIndexingStrategy withFieldName(String fieldName)
  {
    return new LatLonShapeIndexingStrategy(fieldName, shapeFormat);
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    Preconditions.checkArgument(type.isString(), "only string type can be used");
    final ShapeReader reader = shapeFormat.newReader(JtsSpatialContext.GEO);
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        final String value = Objects.toString(input, null);
        if (StringUtils.isNullOrEmpty(value)) {
          return null;
        }
        Shape shape;
        try {
          shape = reader.read(value);
        }
        catch (Throwable t) {
          throw ParsingFail.propagate(value, t, "failed to read shape");
        }
        Preconditions.checkArgument(shape instanceof Point, "%s is not point", StringUtils.forLog(value));
        Point point = (Point) shape;
        return new Field[]{
            new LatLonPoint(fieldName, point.getY(), point.getX())
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

    LatLonShapeIndexingStrategy that = (LatLonShapeIndexingStrategy) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!shapeFormat.equals(that.shapeFormat)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, shapeFormat);
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
