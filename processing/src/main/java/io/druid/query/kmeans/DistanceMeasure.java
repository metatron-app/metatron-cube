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

package io.druid.query.kmeans;

import com.google.common.base.Preconditions;
import org.apache.lucene.util.SloppyMath;

public enum DistanceMeasure
{
  EUCLIDEAN {
    @Override
    public double distance(double[] point1, double[] point2)
    {
      double distance = 0;
      for (int j = 0; j < point1.length; j++) {
        distance += Math.pow(point1[j] - point2[j], 2);
      }
      return distance;
    }
  },
  HAVERSINE {
    @Override
    public double distance(double[] point1, double[] point2)
    {
      // point : x,y (lon,lat)
      Preconditions.checkArgument(point1.length == 2);
      return SloppyMath.haversinMeters(point1[1], point1[0], point2[1], point2[0]);
    }
  };

  public static DistanceMeasure of(String measure)
  {
    return measure == null ? EUCLIDEAN : valueOf(measure.toUpperCase());
  }

  public abstract double distance(double[] point1, double[] point2);

  public int findNearest(Centroid[] centroids, double[] values)
  {
    int nearest = -1;
    double minDistance = Double.MAX_VALUE;
    for (int i = 0; i < centroids.length; i++) {
      final double distance = distance(centroids[i].getCentroid(), values);
      if (nearest < 0 || distance < minDistance) {
        minDistance = distance;
        nearest = i;
      }
    }
    return nearest;
  }
}
