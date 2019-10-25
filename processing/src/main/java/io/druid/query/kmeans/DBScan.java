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

import com.google.common.collect.Lists;
import io.druid.collections.IntList;

import java.util.List;

// modified version of DBSCANClusterer
public class DBScan
{
  private final double eps;
  private final int minPts;
  private final DistanceMeasure measure;

  public DBScan(final double eps, final int minPts)
  {
    this(eps, minPts, DistanceMeasure.EUCLIDEAN);
  }

  public DBScan(final double eps, final int minPts, final DistanceMeasure measure)
  {
    this.eps = eps;
    this.minPts = minPts;
    this.measure = measure;
  }

  public double getEps()
  {
    return eps;
  }

  public int getMinPts()
  {
    return minPts;
  }

  /** Status of a point during the clustering process. */
  private enum Status {
    /** The point has is considered to be noise. */
    NOISE,
    /** The point is already part of a cluster. */
    PART_OF_CLUSTER
  }

  private static class Point
  {
    private final int index;
    private final double[] point;

    private Status status;
    private int count;

    private Point(int index, double[] point)
    {
      this.index = index;
      this.point = point;
    }
  }

  // todo : make iterable of iterable
  public List<List<Centroid>> cluster(final List<Centroid> centroids)
  {
    final List<List<Centroid>> clusters = Lists.newArrayList();
    final Point[] points = new Point[centroids.size()];
    for (int i = 0; i < points.length; i++) {
      points[i] = new Point(i, centroids.get(i).getPoint());
    }

    for (int i = 0; i < points.length; i++) {
      if (points[i].status != null) {
        continue;
      }
      IntList neighbors = getNeighbors(i, points);
      if (neighbors.size() + points[i].count >= minPts) {
        points[i].status = Status.PART_OF_CLUSTER;
        final IntList e = expandCluster(i, neighbors, points);
        final List<Centroid> cluster = Lists.newArrayList();
        for (int j = 0; j < e.size(); j++) {
          cluster.add(centroids.get(e.get(j)));
        }
        clusters.add(cluster);
      } else {
        points[i].status = Status.NOISE;
      }
    }

    return clusters;
  }

  private IntList expandCluster(final int point, final IntList neighbors, final Point[] points)
  {
    final IntList cluster = new IntList();
    cluster.add(point);
    for (int i = 0; i < neighbors.size(); i++) {
      final Point current = points[neighbors.get(i)];
      if (current.status == Status.PART_OF_CLUSTER) {
        continue;
      }
      if (current.status == null) {
        final IntList neighborsOfCurrent = getNeighbors(current.index, points);
        if (neighborsOfCurrent.size() + current.count >= minPts) {
          neighbors.addAll(neighborsOfCurrent);
        }
      }
      if (current.status != Status.PART_OF_CLUSTER) {
        current.status = Status.PART_OF_CLUSTER;
        cluster.add(current.index);
      }
    }
    return cluster;
  }

  private IntList getNeighbors(final int x, final Point[] points)
  {
    final IntList neighbors = new IntList();
    for (int i = 0; i < points.length; i++) {
      if (i == x || points[i].status != null) {
        continue;
      }
      if (measure.inDistance(points[x].point, points[i].point, eps)) {
        neighbors.add(points[i].index);
        points[i].count++;
      }
    }
    return neighbors;
  }
}
