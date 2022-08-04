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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Maps;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;

public enum ShapeOperation implements Serializable
{
  BBOX_INTERSECTS("BBoxIntersects") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return indexedShape.getBoundingBox().relate(queryShape).intersects();
    }
  },
  /**
   * Bounding box of the *indexed* shape, then {@link #IsWithin}.
   */
  BBOX_WITHIN("BBoxWithin", "BBoxCoveredBy") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      Rectangle bbox = indexedShape.getBoundingBox();
      return bbox.equals(queryShape) || bbox.relate(queryShape) == SpatialRelation.WITHIN;
    }
  },
  /**
   * Meets the "Covers" OGC definition (boundary-neutral).
   */
  CONTAINS("Contains", "Covers") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return indexedShape.equals(queryShape) || indexedShape.relate(queryShape) == SpatialRelation.CONTAINS;
    }
  },
  /**
   * Meets the "Intersects" OGC definition.
   */
  INTERSECTS("Intersects") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return indexedShape.relate(queryShape).intersects();
    }
  },
  /**
   * Meets the "Equals" OGC definition.
   */
  EQUALS("Equals", "IsEqualTo") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return indexedShape.equals(queryShape);
    }
  },
  /**
   * Meets the "Disjoint" OGC definition.
   */
  DISJOINT("Disjoint", "IsDisjointTo") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return !indexedShape.relate(queryShape).intersects();
    }
  },
  /**
   * Meets the "CoveredBy" OGC definition (boundary-neutral).
   */
  WITHIN("Within", "IsWithin", "CoveredBy") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return indexedShape.equals(queryShape) || indexedShape.relate(queryShape) == SpatialRelation.WITHIN;
    }
  },
  /**
   * Almost meets the "Overlaps" OGC definition, but boundary-neutral (boundary==interior).
   */
  OVERLAPS("Overlaps") {
    @Override
    public boolean evaluate(Shape indexedShape, Shape queryShape)
    {
      return indexedShape.relate(queryShape) == SpatialRelation.INTERSECTS;//not Contains or Within or Disjoint
    }
  };

  private static final Map<String, ShapeOperation> REGISTRY = Maps.newHashMap();

  static {
    for (ShapeOperation op : ShapeOperation.values()) {
      for (String name : op.names) {
        REGISTRY.put(name.toUpperCase(Locale.ROOT), op);
      }
    }
  }

  private final String[] names;

  ShapeOperation(String... names)
  {
    this.names = names;
  }

  @JsonCreator
  public static ShapeOperation from(String v)
  {
    return REGISTRY.get(v.toUpperCase(Locale.ROOT));
  }

  /**
   * Returns whether the relationship between indexedShape and queryShape is
   * satisfied by this operation.
   */
  public abstract boolean evaluate(Shape indexedShape, Shape queryShape);

  @JsonValue
  public String getName()
  {
    return names[0];
  }
}
