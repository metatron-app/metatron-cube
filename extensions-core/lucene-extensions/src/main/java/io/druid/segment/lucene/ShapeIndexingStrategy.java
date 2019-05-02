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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.vividsolutions.jts.geom.Geometry;
import io.druid.common.utils.StringUtils;
import io.druid.data.ParsingFail;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.ShapeUtils;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;
import org.apache.lucene.document.Field;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import javax.annotation.Nullable;

/**
 */
@JsonTypeName("shape")
public class ShapeIndexingStrategy implements LuceneIndexingStrategy
{
  enum ShapeType
  {
    POINT {
      @Override
      boolean validate(Shape shape)
      {
        if (shape instanceof JtsGeometry) {
          Geometry geometry = ShapeUtils.toGeometry(shape);
          return geometry instanceof com.vividsolutions.jts.geom.Point ||
                 geometry instanceof com.vividsolutions.jts.geom.MultiPoint;
        }
        return false;
      }
    },
    LINE {
      @Override
      boolean validate(Shape shape)
      {
        Geometry geometry = ShapeUtils.toGeometry(shape);
        return geometry instanceof com.vividsolutions.jts.geom.LineString ||
               geometry instanceof com.vividsolutions.jts.geom.MultiLineString;
      }
    },
    POLYGON {
      @Override
      boolean validate(Shape shape)
      {
        Geometry geometry = ShapeUtils.toGeometry(shape);
        return geometry instanceof com.vividsolutions.jts.geom.Polygon ||
               geometry instanceof com.vividsolutions.jts.geom.MultiPolygon;
      }
    },
    ALL;

    @JsonValue
    public String getName()
    {
      return name().toLowerCase();
    }

    @Nullable
    @JsonCreator
    public static ShapeType fromString(@Nullable String name)
    {
      return Strings.isNullOrEmpty(name) ? null : valueOf(name.toUpperCase());
    }

    boolean validate(Shape shape)
    {
      return true;
    }
  }

//   1 : 5,009.4km x 4,992.6km
//   2 : 1,252.3km x 624.1km
//   3 : 156.5km x 156km
//   4 : 39.1km x 19.5km
//   5 : 4.9km x 4.9km
//   6 : 1.2km x 609.4m
//   7 : 152.9m x 152.4m
//   8 : 38.2m x 19m
//   9 : 4.8m x 4.8m
//  10 : 1.2m x 59.5cm
//  11 : 14.9cm x 14.9cm
//  12 : 3.7cm x 1.9cm

  private static final int DEFAULT_PRECISION = 10;

  private final String fieldName;
  private final ShapeFormat shapeFormat;
  private final ShapeType shapeType;
  private final int maxLevels;

  @JsonCreator
  public ShapeIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat,
      @JsonProperty("shapeType") ShapeType shapeType,
      @JsonProperty("maxLevels") int maxLevels
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName cannot be null");
    this.shapeFormat = Preconditions.checkNotNull(shapeFormat, "shapeFormat cannot be null");
    this.shapeType = shapeType;
    this.maxLevels = maxLevels <= 0 ? DEFAULT_PRECISION : maxLevels;
    Preconditions.checkArgument(
        maxLevels < GeohashUtils.MAX_PRECISION, "invalid max level " + maxLevels
    );
  }

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public ShapeFormat getShapeFormat()
  {
    return shapeFormat;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ShapeType getShapeType()
  {
    return shapeType;
  }

  @JsonProperty
  public int getMaxLevels()
  {
    return maxLevels;
  }

  @Override
  public String getFieldDescriptor()
  {
    return "shape(format=" + shapeFormat + ")";
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    // use CompositeSpatialStrategy ?
    final SpatialPrefixTree grid = new GeohashPrefixTree(JtsSpatialContext.GEO, maxLevels);
    final SpatialStrategy strategy = new RecursivePrefixTreeStrategy(grid, fieldName);
    final ShapeReader reader = shapeFormat.newReader(JtsSpatialContext.GEO);
    final ShapeType validator = shapeType == null ? ShapeType.ALL : shapeType;

    final int wktIndex;
    if (type.isStruct()) {
      StructMetricSerde serde = (StructMetricSerde) Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));
      wktIndex = serde.indexOf(fieldName);
      Preconditions.checkArgument(wktIndex >= 0, "invalid fieldName " + fieldName + " in " + type);
      Preconditions.checkArgument(serde.type(wktIndex) == ValueType.STRING, fieldName + " is not string");
    } else {
      wktIndex = -1;
    }
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        if (wktIndex >= 0) {
          input = ((Object[]) input)[wktIndex];
        }
        if (StringUtils.isNullOrEmpty(input)) {
          return null;
        }
        try {
          final Shape shape = reader.read(input);
          if (!validator.validate(shape)) {
            throw new IllegalStateException("invalid shape type " + shape);
          }
          return strategy.createIndexableFields(shape);
        }
        catch (Exception e) {
          throw ParsingFail.propagate(input, e);
        }
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

    ShapeIndexingStrategy that = (ShapeIndexingStrategy) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (shapeFormat != that.shapeFormat) {
      return false;
    }
    if (maxLevels != that.maxLevels) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName.hashCode();
    result = 31 * result + shapeFormat.hashCode();
    result = 31 * result + maxLevels;
    return result;
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
