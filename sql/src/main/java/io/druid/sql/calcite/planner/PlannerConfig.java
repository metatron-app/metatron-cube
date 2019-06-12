/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.IAE;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.util.Map;
import java.util.Objects;

public class PlannerConfig
{
  public static final String CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT = "useApproximateCountDistinct";
  public static final String CTX_KEY_USE_APPROXIMATE_TOPN = "useApproximateTopN";
  public static final String CTX_KEY_USE_FALLBACK = "useFallback";
  public static final String CTX_KEY_USE_JOIN = "useJoin";
  public static final String CTX_KEY_USE_TRANSITIVE_FILTER_ON_JOIN = "useTransitiveFilterOnjoin";
  public static final String CTX_KEY_USE_PROJECT_JOIN_TRANSPOSE = "useProjectJoinTranspose";
  public static final String CTX_KEY_USE_JOIN_REORDERING = "useJoinReordering";
  public static final String CTX_KEY_DUMP_PLAN = "dumpPlan";

  @JsonProperty
  private Period metadataRefreshPeriod = new Period("PT1M");

  @JsonProperty
  private int maxSemiJoinRowsInMemory = 100000;

  @JsonProperty
  private int maxTopNLimit = 100000;

  @JsonProperty
  private int maxQueryCount = 8;

  @JsonProperty
  private int selectThreshold = 1000;

  @JsonProperty
  private boolean useApproximateCountDistinct = true;

  @JsonProperty
  private boolean useApproximateTopN = true;

  @JsonProperty
  private boolean useFallback = false;

  @JsonProperty
  private boolean requireTimeCondition = false;

  @JsonProperty
  private DateTimeZone sqlTimeZone = DateTimeZone.UTC;

  @JsonProperty
  private boolean joinEnabled = false;

  @JsonProperty
  private boolean transitiveFilterOnjoinEnabled = true;

  @JsonProperty
  private boolean projectJoinTransposeEnabled = false;  // should fix a bug (try tpch-7)

  @JsonProperty
  private boolean joinReorderingEnabled = false;

  @JsonProperty
  private boolean dumpPlan = false;

  public Period getMetadataRefreshPeriod()
  {
    return metadataRefreshPeriod;
  }

  public int getMaxSemiJoinRowsInMemory()
  {
    return maxSemiJoinRowsInMemory;
  }

  public int getMaxTopNLimit()
  {
    return maxTopNLimit;
  }

  public int getMaxQueryCount()
  {
    return maxQueryCount;
  }

  public int getSelectThreshold()
  {
    return selectThreshold;
  }

  public boolean isUseApproximateCountDistinct()
  {
    return useApproximateCountDistinct;
  }

  public boolean isUseApproximateTopN()
  {
    return useApproximateTopN;
  }

  public boolean isUseFallback()
  {
    return useFallback;
  }

  public boolean isJoinEnabled()
  {
    return joinEnabled;
  }

  public boolean isTransitiveFilterOnjoinEnabled()
  {
    return transitiveFilterOnjoinEnabled;
  }

  public boolean isProjectJoinTransposeEnabled()
  {
    return projectJoinTransposeEnabled;
  }

  public boolean isJoinReorderingEnabled()
  {
    return joinReorderingEnabled;
  }

  public boolean isRequireTimeCondition()
  {
    return requireTimeCondition;
  }

  public boolean isDumpPlan()
  {
    return dumpPlan;
  }

  public DateTimeZone getSqlTimeZone()
  {
    return sqlTimeZone;
  }

  public PlannerConfig withOverrides(final Map<String, Object> context)
  {
    if (context == null) {
      return this;
    }

    final PlannerConfig newConfig = new PlannerConfig();
    newConfig.metadataRefreshPeriod = getMetadataRefreshPeriod();
    newConfig.maxSemiJoinRowsInMemory = getMaxSemiJoinRowsInMemory();
    newConfig.maxTopNLimit = getMaxTopNLimit();
    newConfig.maxQueryCount = getMaxQueryCount();
    newConfig.selectThreshold = getSelectThreshold();
    newConfig.useApproximateCountDistinct = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT,
        isUseApproximateCountDistinct()
    );
    newConfig.useApproximateTopN = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_TOPN,
        isUseApproximateTopN()
    );
    newConfig.useFallback = getContextBoolean(
        context,
        CTX_KEY_USE_FALLBACK,
        isUseFallback()
    );
    newConfig.joinEnabled = getContextBoolean(
        context,
        CTX_KEY_USE_JOIN,
        isJoinEnabled()
    );
    newConfig.transitiveFilterOnjoinEnabled = getContextBoolean(
        context,
        CTX_KEY_USE_TRANSITIVE_FILTER_ON_JOIN,
        isTransitiveFilterOnjoinEnabled()
    );
    newConfig.projectJoinTransposeEnabled = getContextBoolean(
        context,
        CTX_KEY_USE_PROJECT_JOIN_TRANSPOSE,
        isProjectJoinTransposeEnabled()
    );
    newConfig.joinReorderingEnabled = getContextBoolean(
        context,
        CTX_KEY_USE_JOIN_REORDERING,
        isJoinReorderingEnabled()
    );
    newConfig.dumpPlan = getContextBoolean(context, CTX_KEY_DUMP_PLAN, isDumpPlan());
    newConfig.requireTimeCondition = isRequireTimeCondition();
    newConfig.sqlTimeZone = getSqlTimeZone();
    return newConfig;
  }

  private static boolean getContextBoolean(
      final Map<String, Object> context,
      final String parameter,
      final boolean defaultValue
  )
  {
    final Object value = context.get(parameter);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else {
      throw new IAE("Expected parameter[%s] to be boolean", parameter);
    }
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PlannerConfig that = (PlannerConfig) o;
    return maxSemiJoinRowsInMemory == that.maxSemiJoinRowsInMemory &&
           maxTopNLimit == that.maxTopNLimit &&
           maxQueryCount == that.maxQueryCount &&
           selectThreshold == that.selectThreshold &&
           useApproximateCountDistinct == that.useApproximateCountDistinct &&
           useApproximateTopN == that.useApproximateTopN &&
           useFallback == that.useFallback &&
           joinEnabled == that.joinEnabled &&
           transitiveFilterOnjoinEnabled == that.transitiveFilterOnjoinEnabled &&
           projectJoinTransposeEnabled == that.projectJoinTransposeEnabled &&
           joinReorderingEnabled == that.joinReorderingEnabled &&
           requireTimeCondition == that.requireTimeCondition &&
           dumpPlan == that.dumpPlan &&
           Objects.equals(metadataRefreshPeriod, that.metadataRefreshPeriod) &&
           Objects.equals(sqlTimeZone, that.sqlTimeZone);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(
        metadataRefreshPeriod,
        maxSemiJoinRowsInMemory,
        maxTopNLimit,
        maxQueryCount,
        selectThreshold,
        useApproximateCountDistinct,
        useApproximateTopN,
        useFallback,
        joinEnabled,
        transitiveFilterOnjoinEnabled,
        projectJoinTransposeEnabled,
        joinReorderingEnabled,
        requireTimeCondition,
        dumpPlan,
        sqlTimeZone
    );
  }

  @Override
  public String toString()
  {
    return "PlannerConfig{" +
           "metadataRefreshPeriod=" + metadataRefreshPeriod +
           ", maxSemiJoinRowsInMemory=" + maxSemiJoinRowsInMemory +
           ", maxTopNLimit=" + maxTopNLimit +
           ", maxQueryCount=" + maxQueryCount +
           ", selectThreshold=" + selectThreshold +
           ", useApproximateCountDistinct=" + useApproximateCountDistinct +
           ", useApproximateTopN=" + useApproximateTopN +
           ", useFallback=" + useFallback +
           ", joinEnabled=" + joinEnabled +
           ", transitiveFilterOnjoinEnabled=" + transitiveFilterOnjoinEnabled +
           ", projectJoinTransposeEnabled=" + projectJoinTransposeEnabled +
           ", joinReorderingEnabled=" + joinReorderingEnabled +
           ", requireTimeCondition=" + requireTimeCondition +
           ", dumpPlan=" + dumpPlan +
           ", sqlTimeZone=" + sqlTimeZone +
           '}';
  }
}
