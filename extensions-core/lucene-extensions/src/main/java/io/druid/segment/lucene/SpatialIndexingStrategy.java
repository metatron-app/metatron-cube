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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
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

import java.util.Objects;

/**
 */
@JsonTypeName("spatial")
public class SpatialIndexingStrategy implements LuceneIndexingStrategy
{
  private static final int DEFAULT_PRECISION = 18;

  private final String fieldName;
  private final ShapeFormat shapeFormat;
  private final int maxLevels;

  @JsonCreator
  public SpatialIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat,
      @JsonProperty("maxLevels") int maxLevels
  ) {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName cannot be null");
    this.shapeFormat = Preconditions.checkNotNull(shapeFormat, "shapeFormat cannot be null");
    this.maxLevels = maxLevels <= 0 ? DEFAULT_PRECISION : maxLevels;
    Preconditions.checkArgument(
        maxLevels < GeohashUtils.MAX_PRECISION, "invalid max level " + maxLevels
    );
  }

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
  public int getMaxLevels()
  {
    return maxLevels;
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    // use CompositeSpatialStrategy ?
    final SpatialPrefixTree grid = new GeohashPrefixTree(JtsSpatialContext.GEO, maxLevels);
    final SpatialStrategy strategy = new RecursivePrefixTreeStrategy(grid, fieldName);
    final ShapeReader reader = shapeFormat.newReader(JtsSpatialContext.GEO);
    final int wktIndex;
    if (type.isStruct()) {
      StructMetricSerde serde = (StructMetricSerde) Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));
      wktIndex = serde.indexOf(fieldName);
      Preconditions.checkArgument(wktIndex >= 0, "invalid fieldName " + fieldName + " in " + type);
      Preconditions.checkArgument(serde.type(wktIndex) == ValueType.STRING, fieldName + " is not string");
    } else if (type.isString()) {
      wktIndex = -1;
    } else {
      throw new IllegalArgumentException("not supported type " + type);
    }
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        if (wktIndex >= 0) {
          input = ((Object[])input)[wktIndex];
        }
        try {
          return strategy.createIndexableFields(reader.read(Objects.toString(input, null)));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
