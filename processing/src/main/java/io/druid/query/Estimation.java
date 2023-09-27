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

import it.unimi.dsi.fastutil.longs.Long2LongFunction;

public class Estimation
{
  public static final String ROWNUM = "$rownum";
  public static final String SELECTIVITY = "$selectivity";

  public static final double EPSILON = 0.00001d;

  public static Estimation unknown()
  {
    return of(Queries.UNKNOWN, 1f);
  }

  public static Estimation of(long estimated, double selectivity)
  {
    return new Estimation(estimated, selectivity);
  }

  public static Estimation of(long[] estimated)
  {
    return new Estimation(estimated[0], estimated[1] == 0 ? 1f : estimated[0] / (double) estimated[1]);
  }

  public static Estimation from(Query<?> query)
  {
    int rc = getRowCount(query);
    double selectivity = getSelectivity(query);
    return rc < 0 || selectivity < 0 ? null : of(rc, selectivity);
  }

  public static int getRowCount(Query<?> query)
  {
    return query.getContextInt(ROWNUM, -1);
  }

  public static double getSelectivity(Query<?> query)
  {
    return query.getContextDouble(SELECTIVITY, -1d);
  }

  long estimated;
  double selectivity;

  public Estimation(long estimated, double selectivity)
  {
    this.estimated = estimated;
    this.selectivity = selectivity;
  }

  public Estimation duplicate()
  {
    return new Estimation(estimated, selectivity);
  }

  public double origin()
  {
    return estimated / Math.max(0.0001f, selectivity);
  }

  public Estimation update(long update)
  {
    update = Math.max(1, update);
    selectivity = normalize(selectivity * update / estimated);
    estimated = update;
    return this;
  }

  public Estimation update(double update)
  {
    update = normalize(update);
    estimated = Math.max(1, (long) (estimated * update / selectivity));
    selectivity = normalize(update);
    return this;
  }

  public void update(Long2LongFunction update)
  {
    update(update.applyAsLong(estimated));
  }

  public boolean lt(Estimation estimation)
  {
    return estimated < estimation.estimated;
  }

  public boolean moreSelective(Estimation estimation)
  {
    return selectivity < estimation.selectivity;
  }

  public boolean eq(Estimation estimation, double epsilon)
  {
    return Math.abs(estimated - estimation.estimated) < epsilon;
  }

  public boolean gt(Estimation estimation)
  {
    return estimated > estimation.estimated;
  }

  public boolean lt(long threshold)
  {
    return threshold >= 0 && estimated < threshold;
  }

  public boolean lte(long threshold)
  {
    return threshold >= 0 && estimated <= threshold;
  }

  public boolean gt(long threshold)
  {
    return threshold >= 0 && estimated > threshold;
  }

  public boolean gte(long threshold)
  {
    return threshold >= 0 && estimated >= threshold;
  }

  public Estimation multiply(Estimation estimation)
  {
    estimated = Math.max(1, (long) (estimated * estimation.selectivity));
    selectivity = Math.min(selectivity, estimation.selectivity);
    return this;
  }

  public static long delta(Estimation left, Estimation right)
  {
    return Math.abs(left.estimated - right.estimated);
  }

  public static long max(Estimation left, Estimation right)
  {
    return Math.max(left.estimated, right.estimated);
  }

  private static final int ACCEPTABLE_MAX_LIMIT = 10000;

  public Estimation applyLimit(int limit)
  {
    // affects selectivity ?
    if (estimated > 0) {
      estimated = Math.min(limit, estimated);
    } else if (limit < ACCEPTABLE_MAX_LIMIT) {
      estimated = limit;
    }
    return this;
  }

  public double degrade()
  {
    return Math.pow(selectivity, 0.67f);  // bloom or forced filter, pessimistic
  }

  private static double normalize(double selectivity)
  {
    return Math.max(EPSILON, Math.min(1f, selectivity));
  }

  @Override
  public String toString()
  {
    return String.format("%d:%.5f", estimated, selectivity);
  }
}
