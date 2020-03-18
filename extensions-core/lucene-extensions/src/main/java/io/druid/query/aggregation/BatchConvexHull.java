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

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import io.druid.query.GeomUtils;
import org.locationtech.jts.algorithm.ConvexHull;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.Collections;
import java.util.List;

public class BatchConvexHull
{
  private static final int BATCH_INTERVAL = 4096;

  private Geometry current;
  private final List<Coordinate> coordinates = Lists.newArrayList();

  public void add(Geometry geometry)
  {
    Coordinate[] input = geometry instanceof Polygon ? ((Polygon) geometry).getExteriorRing().getCoordinates()
                                                     : geometry.getCoordinates();
    Collections.addAll(coordinates, input);
    if (coordinates.size() > BATCH_INTERVAL) {
      flush();
    }
  }

  public Geometry getConvexHull()
  {
    if (!coordinates.isEmpty()) {
      flush();
    }
    return current;
  }

  private void flush()
  {
    if (current != null) {
      Collections.addAll(coordinates, current.getCoordinates());
    }
    current = new ConvexHull(coordinates.toArray(new Coordinate[0]), GeomUtils.GEOM_FACTORY).getConvexHull();
    coordinates.clear();
  }
}
