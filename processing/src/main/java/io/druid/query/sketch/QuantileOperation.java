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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yahoo.sketches.quantiles.ItemsSketch;

import java.lang.reflect.Array;
import java.util.List;

/**
 */
public enum QuantileOperation
{
  QUANTILES {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (sketch.getN() == 0) {
        return null;
      } else if (parameter == null) {
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
        Object[] quantiles;
        if (quantileParam.evenSpaced) {
          quantiles = sketch.getQuantiles(quantileParam.number); // even spaced
        } else if (quantileParam.evenCounted) {
          final int limitNum = quantileParam.number;
          double[] thresholds = new double[(int) (sketch.getN() / limitNum) + 1];
          for (int i = 1; i < thresholds.length - 1; i++) {
            thresholds[i] = (double) limitNum * i / sketch.getN();
          }
          thresholds[thresholds.length - 1] = 1;
          quantiles = sketch.getQuantiles(thresholds);
        } else if (quantileParam.slopedSpaced) {
          int max = quantileParam.maxThreshold;
          // sigma(0 ~ p) of (ax + b) = total
          // ((p * (p - 1)) / 2) * a + p * b = total
          int p = quantileParam.number - 1;
          double total = sketch.getN();
          double mean = total / p;
          double a = -1;
          double b = -1;
          if (max > 0) {
            // a * p + b = max * 0.95
            // ((p * (p - 1)) / 2) * a + p * (max * 0.95 - a * p) = total
            // ((p * (p - 1)) / 2 - p * p) * a + p * (max * 0.95) = total
            // a = (total - p * (max * 0.95)) / ((p * (-p - 1)) / 2)
            a = (total - p * (max * 0.95)) / ((p * (-p - 1)) / 2);
            b = max * 0.95 - a * p;
          }
          if (a < 0 || b < 0) {
            // a = (total - p * b) / (p * (p - 1)) * 2
            b = mean * 0.4;
            a = (total - p * b) / (p * (p - 1)) * 2;
          }
          double[] thresholds = new double[quantileParam.number];
          for (int i = 1; i < p; i++) {
            thresholds[i] = (i > 1 ? thresholds[i - 1] : 0) + a * (i - 1) + b;
          }
          for (int i = 1; i < p; i++) {
            thresholds[i] /= total;
          }
          thresholds[0] = 0;
          thresholds[thresholds.length - 1] = 1;
          quantiles = sketch.getQuantiles(thresholds);
        } else {
          throw new IllegalArgumentException("Invalid quantile param " + quantileParam);
        }
        if (quantileParam.dedup) {
          quantiles = removeDuplicates(quantiles, sketch.getMinValue().getClass());
        }
        return quantiles;
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  CDF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (sketch.getN() == 0) {
        return null;
      } else if (parameter instanceof RatioParam) {
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
      if (sketch.getN() == 0) {
        return null;
      } else if (parameter instanceof RatioParam) {
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
      if (sketch.getN() == 0) {
        return null;
      }
      Class<?> clazz = sketch.getMinValue().getClass();
      QuantileRatioParam param = (QuantileRatioParam) parameter;
      Object[] splits = removeDuplicates((Object[]) QUANTILES.calculate(sketch, param.quantileParam), clazz);
      return ImmutableMap.of("splits", splits, "cdf", CDF.calculate(sketch, ratioParam(splits, param.ratioAsCount)));
    }
  },
  QUANTILES_PMF {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (sketch.getN() == 0) {
        return null;
      }
      Class<?> clazz = sketch.getMinValue().getClass();
      QuantileRatioParam param = (QuantileRatioParam) parameter;
      Object[] splits = removeDuplicates((Object[]) QUANTILES.calculate(sketch, param.quantileParam), clazz);
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

  @SuppressWarnings("unchecked")
  public static <T> Object[] removeDuplicates(Object[] splits, Class<T> clazz)
  {
    List result = Lists.newArrayList();
    Object prev = null;
    for (Object split : splits) {
      if (prev == null || !prev.equals(split)) {
        result.add(split);
      }
      prev = split;
    }
    return result.toArray((Object[]) Array.newInstance(clazz, result.size()));
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

  public static final QuantileParam DEFAULT_QUANTILE_PARAM = evenSpaced(11, false);

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
    final int maxThreshold;
    final boolean evenSpaced;
    final boolean evenCounted;
    final boolean slopedSpaced;
    final boolean dedup;

    private QuantileParam(int number, int maxThreshold, boolean evenSpaced, boolean evenCounted, boolean slopedSpaced, boolean dedup)
    {
      this.number = number;
      this.maxThreshold = maxThreshold;
      this.evenSpaced = evenSpaced;
      this.evenCounted = evenCounted;
      this.slopedSpaced = slopedSpaced;
      this.dedup = dedup;
    }
  }

  public static QuantileParam of(String type, int partition, int maxThreshold, boolean dedup)
  {
    if ("evenSpaced".equals(type)) {
      return evenSpaced(partition, dedup);
    } else if ("evenCounted".equals(type)) {
      return evenCounted(partition, dedup);
    } else {
      return slopedSpaced(partition, maxThreshold, dedup);
    }
  }

  public static QuantileParam evenSpaced(int partition, boolean dedup)
  {
    return new QuantileParam(partition, -1, true, false, false, dedup);
  }

  public static QuantileParam evenCounted(int partition, boolean dedup)
  {
    return new QuantileParam(partition, -1, false, true, false, dedup);
  }

  public static QuantileParam slopedSpaced(int partition, int maxThreshold, boolean dedup)
  {
    return new QuantileParam(partition, maxThreshold, false, false, true, dedup);
  }

  public static QuantileParam valueOf(String strategy, int partition, int maxThreshold, boolean dedup)
  {
    switch (strategy) {
      case "evenSpaced": return evenSpaced(partition, dedup);
      case "evenCounted": return evenCounted(partition, dedup);
      case "slopedSpaced": return slopedSpaced(partition, maxThreshold, dedup);
      default: return slopedSpaced(partition, maxThreshold, dedup);
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
