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
  public static Estimation unknown()
  {
    return of(Queries.UNKNOWN, 1f);
  }

  public static Estimation of(long estimated, float selectivity)
  {
    return new Estimation(estimated, selectivity);
  }

  public static Estimation of(long[] estimated)
  {
    return new Estimation(estimated[0], estimated[1] == 0 ? 1f : estimated[0] / (float) estimated[1]);
  }

  long estimated;
  float selectivity;

  public Estimation(long estimated, float selectivity)
  {
    this.estimated = estimated;
    this.selectivity = selectivity;
  }

  public Estimation duplicate()
  {
    return new Estimation(estimated, selectivity);
  }

  public Estimation update(long update)
  {
    update = Math.max(1, update);
    selectivity = normalize(selectivity * update / estimated);
    estimated = update;
    return this;
  }

  public Estimation update(float update)
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

  public boolean moreSelective(Estimation estimation)
  {
    return selectivity < estimation.selectivity;
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

  public long multiply(float selectivity)
  {
    return Math.max(1, (long) (estimated * selectivity));
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

  public Estimation degrade()
  {
    selectivity = Math.min(1f, selectivity * 1.2f);
    return this;
  }

  private static float normalize(float selectivity)
  {
    return Math.max(0.000001f, Math.min(1f, selectivity));
  }

  @Override
  public String toString()
  {
    return String.format("%d:%.3f", estimated, selectivity);
  }
}
