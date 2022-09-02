/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

/***
 * This is from Hive (HiveCost)
 *
 * NOTE:<br>
 * 1. Hivecost normalizes cpu and io in to time.<br>
 * 2. CPU, IO cost is added together to find the query latency.<br>
 * 3. If query latency is equal then row count is compared.
 */
public class DruidCost implements RelOptCost
{
  public static final double TYNY_VALUE = 0.0001;

  // ~ Static fields/initializers ---------------------------------------------

  public static final DruidCost INFINITY = new DruidCost(
      Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)
  {
    @Override
    public String toString()
    {
      return "{inf}";
    }
  };

  public static final DruidCost HUGE = new DruidCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE)
  {
    @Override
    public String toString()
    {
      return "{huge}";
    }
  };

  public static final DruidCost ZERO = new DruidCost(0.0, 0.0, 0.0)
  {
    @Override
    public String toString()
    {
      return "{0}";
    }
  };

  public static final DruidCost TINY = new DruidCost(TYNY_VALUE, TYNY_VALUE, 0.0)
  {
    @Override
    public String toString()
    {
      return "{tiny}";
    }
  };

  public static final RelOptCostFactory FACTORY = new Factory();

  // ~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;
  final double rowCount;

  // ~ Constructors -----------------------------------------------------------

  private DruidCost(double rowCount, double cpu, double io)
  {
    this.rowCount = Math.max(TYNY_VALUE, rowCount);
    this.cpu = Math.max(TYNY_VALUE, cpu);
    this.io = io;
  }

  // ~ Methods ----------------------------------------------------------------

  public double getCpu()
  {
    return cpu;
  }

  public boolean isInfinite()
  {
    return this == INFINITY || rowCount == Double.POSITIVE_INFINITY
           || cpu == Double.POSITIVE_INFINITY || io == Double.POSITIVE_INFINITY;
  }

  public double getIo()
  {
    return io;
  }

  public boolean isLe(RelOptCost other)
  {
    if (cpu + io < other.getCpu() + other.getIo() ||
        cpu + io <= other.getCpu() + other.getIo() && rowCount <= other.getRows()) {
      return true;
    }
    return false;
  }

  public boolean isLt(RelOptCost other)
  {
    if (cpu + io < other.getCpu() + other.getIo() ||
        cpu + io <= other.getCpu() + other.getIo() && rowCount < other.getRows()) {
      return true;
    }
    return false;
  }

  public double getRows()
  {
    return rowCount;
  }

  public boolean equals(RelOptCost other)
  {
    return this == other || cpu + io == other.getCpu() + other.getIo() && rowCount == other.getRows();
  }

  public boolean isEqWithEpsilon(RelOptCost other)
  {
    return this == other ||
           Math.abs(io - other.getIo()) < RelOptUtil.EPSILON
           && Math.abs(cpu - other.getCpu()) < RelOptUtil.EPSILON
           && Math.abs(rowCount - other.getRows()) < RelOptUtil.EPSILON;
  }

  public RelOptCost minus(RelOptCost other)
  {
    if (this == INFINITY) {
      return this;
    }
    return new DruidCost(rowCount - other.getRows(), cpu - other.getCpu(), io - other.getIo());
  }

  public RelOptCost multiplyBy(double factor)
  {
    if (this == INFINITY) {
      return this;
    }
    return new DruidCost(rowCount * factor, cpu * factor, io * factor);
  }

  public double divideBy(RelOptCost cost)
  {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    double d = 1;
    double n = 0;
    if ((rowCount != 0) && !Double.isInfinite(rowCount) && (cost.getRows() != 0)
        && !Double.isInfinite(cost.getRows())) {
      d *= rowCount / cost.getRows();
      ++n;
    }
    if ((cpu != 0) && !Double.isInfinite(cpu) && (cost.getCpu() != 0)
        && !Double.isInfinite(cost.getCpu())) {
      d *= cpu / cost.getCpu();
      ++n;
    }
    if ((io != 0) && !Double.isInfinite(io) && (cost.getIo() != 0)
        && !Double.isInfinite(cost.getIo())) {
      d *= io / cost.getIo();
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other)
  {
    if ((this == INFINITY) || (other.isInfinite())) {
      return INFINITY;
    }
    return new DruidCost(rowCount + other.getRows(), cpu + other.getCpu(), io + other.getIo());
  }

  @Override
  public String toString()
  {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  private static class Factory implements RelOptCostFactory
  {
    private Factory() {}

    public RelOptCost makeCost(double rowCount, double cpu, double io)
    {
      return new DruidCost(rowCount, cpu, io);
    }

    public RelOptCost makeHugeCost()
    {
      return HUGE;
    }

    public DruidCost makeInfiniteCost()
    {
      return INFINITY;
    }

    public DruidCost makeTinyCost()
    {
      return TINY;
    }

    public DruidCost makeZeroCost()
    {
      return ZERO;
    }
  }
}
