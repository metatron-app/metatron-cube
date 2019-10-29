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
      return Math.sqrt(distance);
    }

    @Override
    public boolean inDistance(double[] point1, double[] point2, double threshold)
    {
      for (int i = 0; i < point1.length; i++) {
        if (Math.abs(point1[i] - point2[i]) > threshold) {
          return false;
        }
      }
      return super.inDistance(point1, point2, threshold);
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

  public boolean inDistance(double[] point1, double[] point2, double threshold)
  {
    return distance(point1, point2) <= threshold;
  }

  public int findNearest(Centroid[] centroids, double[] values)
  {
    return findNearest(centroids, values, -1);
  }

  public int findNearest(Centroid[] centroids, double[] values, double maxDistance)
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
    if (maxDistance > 0 && minDistance > maxDistance) {
      return -1;
    }
    return nearest;
  }
}
