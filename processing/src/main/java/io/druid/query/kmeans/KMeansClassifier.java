/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Rows;
import io.druid.query.Classifier;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class KMeansClassifier implements Classifier
{
  private final List<String> metrics;
  private final Centroid[] centroids;
  private final DistanceMeasure measure;
  private final double[] point;
  private final String tagColumn;

  public KMeansClassifier(List<String> metrics, Centroid[] centroids, DistanceMeasure measure, String tagColumn)
  {
    this.metrics = metrics;
    this.centroids = centroids;
    this.measure = measure;
    this.tagColumn = Preconditions.checkNotNull(tagColumn, "'tagColumn' cannot be null");
    this.point = new double[metrics.size()];
  }

  @Override
  public int numClasses()
  {
    return centroids.length;
  }

  @Override
  public Function<Object[], Object[]> init(List<String> outputColumns)
  {
    final int[] indices = GuavaUtils.indexOf(outputColumns, metrics);
    return new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(Object[] input)
      {
        for (int i = 0; i < indices.length; i++) {
          point[i] = Rows.parseDouble(input[indices[i]]);
        }
        Object[] output = Arrays.copyOf(input, input.length + 1);
        output[input.length] = measure.findNearest(centroids, point);
        return output;
      }
    };
  }

  @Override
  public Map<String, Object> apply(Map<String, Object> input)
  {
    for (int i = 0; i < metrics.size(); i++) {
      point[i] = Rows.parseDouble(input.get(metrics.get(i)));
    }
    Map<String, Object> updatable = MapBasedRow.toUpdatable(input);
    updatable.put(tagColumn, measure.findNearest(centroids, point));
    return updatable;
  }
}
