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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.collections.IntList;
import io.druid.query.kmeans.Centroid;
import io.druid.query.kmeans.DistanceMeasure;

import java.util.Iterator;
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

  public Iterator<List<Centroid>> cluster(final List<Centroid> centroids)
  {
    final Point[] points = new Point[centroids.size()];
    for (int i = 0; i < points.length; i++) {
      points[i] = new Point(i, centroids.get(i).getPoint());
    }

    return Iterators.filter(new Iterator<List<Centroid>>()
    {
      private int index;

      @Override
      public boolean hasNext()
      {
        return index < points.length;
      }

      @Override
      public List<Centroid> next()
      {
        for (; index < points.length; index++) {
          if (points[index].status != null) {
            continue;
          }
          final IntList neighbors = getNeighbors(index, points);
          if (neighbors.size() + points[index].count >= minPts) {
            points[index].status = Status.PART_OF_CLUSTER;
            final IntList ids = expandCluster(index, neighbors, points);
            final List<Centroid> cluster = Lists.newArrayList();
            for (int j = 0; j < ids.size(); j++) {
              cluster.add(centroids.get(ids.get(j)));
            }
            return cluster;
          } else {
            points[index].status = Status.NOISE;
          }
        }
        return null;
      }
    }, Predicates.notNull());
  }

  // todo : can be an iterator ?
  private IntList expandCluster(final int point, final IntList neighbors, final Point[] points)
  {
    final IntList worker = new IntList();
    final IntList cluster = new IntList();
    cluster.add(point);
    for (int i = 0; i < neighbors.size(); i++) {
      final Point current = points[neighbors.get(i)];
      if (current.status == Status.PART_OF_CLUSTER) {
        continue;
      }
      if (current.status == null) {
        final IntList neighborsOfCurrent = getNeighbors(current.index, points, worker);
        if (neighborsOfCurrent.size() + current.count >= minPts) {
          neighbors.addAll(neighborsOfCurrent);
          i = neighbors.compact(i);
        }
        worker.clear();
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
    return getNeighbors(x, points, new IntList());
  }

  private IntList getNeighbors(final int x, final Point[] points, final IntList neighbors)
  {
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
