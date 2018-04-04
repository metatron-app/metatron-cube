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
import com.fasterxml.jackson.annotation.JsonValue;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.WKTReader;

/**
 */
public enum ShapeFormat
{
  GEOJSON {
    @Override
    public ShapeReader newReader(SpatialContext context)
    {
      return new GeoJSONReader(context, null);
    }
  },
  WKT {
    @Override
    public ShapeReader newReader(SpatialContext context)
    {
      return new WKTReader(context, null);
    }
  };

  public abstract ShapeReader newReader(SpatialContext context);

  @JsonValue
  public String getName()
  {
    return name().toLowerCase();
  }

  @JsonCreator
  public static ShapeFormat fromString(String name)
  {
    return name == null ? WKT : valueOf(name.toUpperCase());
  }
}
