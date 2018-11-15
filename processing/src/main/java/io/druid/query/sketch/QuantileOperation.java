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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yahoo.sketches.quantiles.ItemsSketch;

import java.util.List;

/**
 */
public enum QuantileOperation
{
  QUANTILES {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter == null) {
        return sketch.getQuantiles(DEFAULT_FRACTIONS);
      } else if (parameter instanceof Number) {
        double number = ((Number) parameter).doubleValue();
        if (number > 1) {
          number = number / sketch.getN();
        }
        return number > 1 ? sketch.getMaxValue() : sketch.getQuantile(number);
      } else if (parameter instanceof double[]) {
        return sketch.getQuantiles((double[]) parameter);
      } else if (parameter instanceof QuantileParam) {
        QuantileParam quantileParam = (QuantileParam) parameter;
        if (quantileParam.evenSpaced) {
          return sketch.getQuantiles(quantileParam.number); // even spaced
        }
        if (quantileParam.evenCounted) {
          final int limitNum = quantileParam.number;
          double[] quantiles = new double[(int) (sketch.getN() / limitNum) + 1];
          for (int i = 1; i < quantiles.length - 1; i++) {
            quantiles[i] = (double) limitNum * i / sketch.getN();
          }
          quantiles[quantiles.length - 1] = 1;
          return sketch.getQuantiles(quantiles);
        }
        if (quantileParam.slopedSpaced) {
          // sigma(0 ~ p) of (ax + b) = total
          // ((p * (p + 1)) / 2) * a + (p + 1) * b = total
          // a = 2 * (total - (p + 1) * b) / (p * (p + 1))
          int p = quantileParam.number - 2;
          double total = sketch.getN();
          double b = total / (p * 2);
          double a = (total - (p + 1) * b) * 2 / (p * (p + 1));
          double[] quantiles = new double[quantileParam.number];
          for (int i = 1; i <= p; i++) {
            quantiles[i] = (i > 1 ? quantiles[i - 1] : 0) + a * (i - 1) + b;
          }
          for (int i = 1; i <= p; i++) {
            quantiles[i] /= total;
          }
          quantiles[0] = 0;
          quantiles[quantiles.length - 1] = 1;
          return sketch.getQuantiles(quantiles);
        }
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  CDF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter instanceof RatioParam) {
        RatioParam ratioParam = (RatioParam) parameter;
        double[] ratio = sketch.getCDF(ratioParam.splits);
        if (ratioParam.ratioAsCount) {
          return asCounts(ratio, sketch.getN());
        }
        return ratio;
      } else if (parameter.getClass().isArray()) {
        return sketch.getCDF((Object[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  PMF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter instanceof RatioParam) {
        RatioParam ratioParam = (RatioParam) parameter;
        double[] ratio = sketch.getPMF(ratioParam.splits);
        if (ratioParam.ratioAsCount) {
          return asCounts(ratio, sketch.getN());
        }
        return ratio;
      } else if (parameter.getClass().isArray()) {
        return sketch.getPMF((Object[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  QUANTILES_CDF {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      QuantileRatioParam param = (QuantileRatioParam) parameter;
      Object[] splits = removeDuplicates((Object[]) QUANTILES.calculate(sketch, param.quantileParam));
      return ImmutableMap.of("splits", splits, "cdf", CDF.calculate(sketch, ratioParam(splits, param.ratioAsCount)));
    }
  },
  QUANTILES_PMF {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      QuantileRatioParam param = (QuantileRatioParam) parameter;
      Object[] splits = removeDuplicates((Object[]) QUANTILES.calculate(sketch, param.quantileParam));
      return ImmutableMap.of("splits", splits, "pmf", PMF.calculate(sketch, ratioParam(splits, param.ratioAsCount)));
    }
  },
  IQR {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      Object[] quantiles = sketch.getQuantiles(new double[]{0.25f, 0.75f});
      if (quantiles[0] instanceof Number && quantiles[1] instanceof Number) {
        return ((Number) quantiles[1]).doubleValue() - ((Number) quantiles[0]).doubleValue();
      }
      throw new IllegalArgumentException("IQR is possible only for numeric types");
    }
  };

  public abstract Object calculate(ItemsSketch sketch, Object parameter);

  private static Object[] removeDuplicates(Object[] splits)
  {
    List<Object> result = Lists.newArrayList();
    Object prev = null;
    for (Object split : splits) {
      if (prev == null || !prev.equals(split)) {
        result.add(split);
      }
      prev = split;
    }
    return result.toArray();
  }

  private static long[] asCounts(double[] ratio, long n)
  {
    long[] counts = new long[ratio.length];
    for (int i = 0; i < ratio.length; i++) {
      counts[i] = (long) (ratio[i] * n);
    }
    return counts;
  }

  @JsonValue
  public String getName()
  {
    return name();
  }

  @JsonCreator
  public static QuantileOperation fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }

  public static final double[] DEFAULT_FRACTIONS = new double[]{0d, 0.25d, 0.50d, 0.75d, 1.0d};

  public static final QuantileParam DEFAULT_QUANTILE_PARAM = evenSpaced(11);

  private static class RatioParam
  {
    final Object[] splits;
    final boolean ratioAsCount;

    private RatioParam(Object[] splits, boolean ratioAsCount)
    {
      this.splits = splits;
      this.ratioAsCount = ratioAsCount;
    }
  }

  public static RatioParam ratioParam(Object[] splits, boolean ratioAsCount)
  {
    return new RatioParam(splits, ratioAsCount);
  }

  private static class QuantileParam
  {
    final int number;
    final boolean evenSpaced;
    final boolean evenCounted;
    final boolean slopedSpaced;

    private QuantileParam(int number, boolean evenSpaced, boolean evenCounted, boolean slopedSpaced)
    {
      this.number = number;
      this.evenSpaced = evenSpaced;
      this.evenCounted = evenCounted;
      this.slopedSpaced = slopedSpaced;
    }
  }

  public static QuantileParam evenSpaced(int partition)
  {
    return new QuantileParam(partition, true, false, false);
  }

  public static QuantileParam evenCounted(int partition)
  {
    return new QuantileParam(partition, false, true, false);
  }

  public static QuantileParam slopedSpaced(int partition)
  {
    return new QuantileParam(partition, false, false, true);
  }

  public static QuantileParam valueOf(String strategy, int partition)
  {
    switch (strategy) {
      case "evenSpaced": return evenSpaced(partition);
      case "evenCounted": return evenCounted(partition);
      case "slopedSpaced": return slopedSpaced(partition);
      default: return slopedSpaced(partition);
    }
  }

  private static class QuantileRatioParam
  {
    final Object quantileParam;
    final boolean ratioAsCount;

    private QuantileRatioParam(Object quantileParam, boolean ratioAsCount)
    {
      this.quantileParam = quantileParam;
      this.ratioAsCount = ratioAsCount;
    }
  }

  public static QuantileRatioParam quantileRatioParam(Object quantileParam, boolean ratioAsCount)
  {
    return new QuantileRatioParam(quantileParam, ratioAsCount);
  }
}
