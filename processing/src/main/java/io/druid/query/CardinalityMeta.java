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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;

import java.util.Arrays;
import java.util.function.DoubleSupplier;

public class CardinalityMeta
{
  private static final Logger LOG = new Logger(CardinalityMeta.class);

  public static CardinalityMeta of(HyperLogLogCollector[] source, int numRows)
  {
    return new CardinalityMeta(source, null, numRows);
  }

  private final HyperLogLogCollector[] collectors;
  private final double[][] params;
  private final int numRows;

  @JsonCreator
  public CardinalityMeta(
      @JsonProperty("collectors") HyperLogLogCollector[] collectors,
      @JsonProperty("params") double[][] params,
      @JsonProperty("numRows") int numRows
  )
  {
    this.collectors = collectors;
    this.params = params;
    this.numRows = numRows;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public HyperLogLogCollector[] getCollectors()
  {
    return collectors;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public double[][] getParams()
  {
    return params;
  }

  public static CardinalityMeta merge(Sequence<CardinalityMeta> sequence)
  {
    return sequence.accumulate(null, (p, m) -> p == null ? m : p.merge(m));
  }

  public CardinalityMeta merge(CardinalityMeta meta)
  {
    if (params != null) {
      return this;
    }
    if (meta.params != null) {
      return meta;
    }
    double[][] params = new double[collectors.length][];
    for (int i = 0; i < collectors.length; i++) {
      HyperLogLogCollector e1 = collectors[i];
      HyperLogLogCollector e2 = meta.collectors[i];
      double c1 = e1.estimateCardinality();
      double c2 = e2.estimateCardinality();
      double c3 = e1.fold(e2).estimateCardinality();
      params[i] = coeff(Math.max(c1, c2), c1 > c2 ? numRows : meta.numRows, c3, numRows + meta.numRows);
    }
    return new CardinalityMeta(null, params, 0);
  }

  public static double[] coeff(double c1, double r1, double c2, double r2)
  {
    double A = (r1 * Math.pow(c2, 2) - r2 * Math.pow(c1, 2)) / (2 * (r1 * c2 - r2 * c1));
    double C = r1 / (Math.pow(c1, 2) - 2 * A * c1);
    double B = r1 - C * Math.pow(c1 - A, 2);
    if (A > r1) {
      A = Double.POSITIVE_INFINITY;
      B = Math.min(1, c1 / r1);    // linear
    } else if (C == Double.POSITIVE_INFINITY) {
      A = Double.NEGATIVE_INFINITY;
      B = c1;                      // const
    }
    return new double[]{A, B, C};
  }

  public static double estimate(double[] c, double r)
  {
    return estimate(c, () -> r);
  }

  public static double estimate(double[] c, DoubleSupplier r)
  {
    return c == null ? Double.NaN :
           c[0] == Double.NEGATIVE_INFINITY ? c[1] :
           c[0] == Double.POSITIVE_INFINITY ? c[1] * r.getAsDouble() :
           Math.sqrt((r.getAsDouble() - c[1]) / c[2]) + c[0];
  }

  @Override
  public String toString()
  {
    if (params == null) {
      return "CardinalityMeta";
    }
    StringBuilder b = new StringBuilder();
    for (double[] doubles : params) {
      if (b.length() > 0) {
        b.append(", ");
      }
      b.append(Arrays.toString(doubles));
    }
    return String.format("CardinalityMeta [%s]", b);
  }
}
