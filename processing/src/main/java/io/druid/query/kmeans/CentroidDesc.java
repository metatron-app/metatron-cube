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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 */
public class CentroidDesc implements Comparable<CentroidDesc>
{
  private final int id;
  private final double[] sum;
  private int count;

  public CentroidDesc(
      @JsonProperty("id") int id,
      @JsonProperty("count") int count,
      @JsonProperty("sum") double[] sum
  )
  {
    this.id = id;
    this.count = count;
    this.sum = sum;
  }

  public CentroidDesc(int id, int length)
  {
    this.id = id;
    this.sum = new double[length];
  }

  @JsonProperty
  public int getId()
  {
    return id;
  }

  @JsonProperty
  public int getCount()
  {
    return count;
  }

  @JsonProperty
  public double[] getSum()
  {
    return sum;
  }

  @Override
  public int compareTo(CentroidDesc o)
  {
    return Integer.compare(id, o.id);
  }

  public void add(double[] values)
  {
    count++;
    for (int i = 0; i < values.length; i++) {
      sum[i] += values[i];
    }
  }

  public CentroidDesc merge(CentroidDesc mergee)
  {
    Preconditions.checkArgument(id == mergee.id);
    count += mergee.count;
    for (int i = 0; i < sum.length; i++) {
      sum[i] += mergee.sum[i];
    }
    return this;
  }

  public Centroid newCenter()
  {
    double[] centroid = new double[sum.length];
    for (int i = 0; i < centroid.length; i++) {
      centroid[i] = sum[i] / count;
    }
    return new Centroid(centroid);
  }
}
