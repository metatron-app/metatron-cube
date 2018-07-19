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
import com.google.common.base.Preconditions;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;

/**
 */
public enum PointQueryType
{
  DISTANCE {
    @Override
    public Query toQuery(String fieldName, double[] latitudes, double[] longitudes, double radiusMeters)
    {
      Preconditions.checkArgument(latitudes.length == 1 && longitudes.length == 1);
      return LatLonPoint.newDistanceQuery(fieldName, latitudes[0], longitudes[0], radiusMeters);
    }
  },
  BBOX {
    @Override
    public Query toQuery(String fieldName, double[] latitudes, double[] longitudes, double radiusMeters)
    {
      Preconditions.checkArgument(latitudes.length == 2 && longitudes.length == 2);
      return LatLonPoint.newBoxQuery(
          fieldName,
          Math.min(latitudes[0], latitudes[1]), Math.max(latitudes[0], latitudes[1]),
          Math.min(longitudes[0], longitudes[1]), Math.max(longitudes[0], longitudes[1])
      );
    }
  },
  POLYGON {
    @Override
    public Query toQuery(String fieldName, double[] latitudes, double[] longitudes, double radiusMeters)
    {
      // todo support hole
      return LatLonPoint.newPolygonQuery(fieldName, new Polygon(latitudes, longitudes));
    }
  };

  public abstract Query toQuery(String fieldName, double[] latitudes, double[] longitudes, double radiusMeters);

  @JsonValue
  public String getName()
  {
    return name().toLowerCase();
  }

  @JsonCreator
  public static PointQueryType fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }
}
