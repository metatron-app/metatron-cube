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

package io.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.IAE;
import org.joda.time.DateTimeZone;

import java.util.Map;
import java.util.Objects;

public class PlannerConfig
{
  public static final String CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT = "useApproximateCountDistinct";
  public static final String CTX_KEY_USE_APPROXIMATE_TOPN = "useApproximateTopN";
  public static final String CTX_KEY_USE_JOIN = "useJoin";
  public static final String CTX_KEY_USE_JOIN_COMMUTE = "useJoinCommute";
  public static final String CTX_KEY_USE_JOIN_REORDERING = "useJoinReordering";
  public static final String CTX_KEY_USE_JOIN_REORDERING_BUSH = "useJoinReorderingBush";
  public static final String CTX_KEY_DUMP_PLAN = "dumpPlan";
  public static final String CTX_KEY_BINARY_OPERANDS_CAST_ADJUST = "binaryOperandsCastAdjust";

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
  private boolean requireTimeCondition = false;

  @JsonProperty
  private DateTimeZone sqlTimeZone = DateTimeZone.UTC;

  @JsonProperty
  private boolean joinEnabled = true;

  @JsonProperty
  private boolean dumpPlan = false;

  @JsonProperty
  private boolean binaryOperandsCastAdjust = false;

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

  public boolean isJoinEnabled()
  {
    return joinEnabled;
  }

  public boolean isRequireTimeCondition()
  {
    return requireTimeCondition;
  }

  public boolean isDumpPlan()
  {
    return dumpPlan;
  }

  public boolean isBinaryOperandsCastAdjust()
  {
    return binaryOperandsCastAdjust;
  }

  public DateTimeZone getSqlTimeZone()
  {
    return sqlTimeZone;
  }

  public PlannerConfig withOverrides(final Map<String, Object> context)
  {
    if (context == null || context.isEmpty()) {
      return this;
    }

    final PlannerConfig newConfig = new PlannerConfig();
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
    newConfig.joinEnabled = getContextBoolean(
        context,
        CTX_KEY_USE_JOIN,
        isJoinEnabled()
    );
    newConfig.dumpPlan = getContextBoolean(context, CTX_KEY_DUMP_PLAN, isDumpPlan());
    newConfig.binaryOperandsCastAdjust = getContextBoolean(
        context,
        CTX_KEY_BINARY_OPERANDS_CAST_ADJUST,
        isBinaryOperandsCastAdjust()
    );
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
    return maxTopNLimit == that.maxTopNLimit &&
           maxQueryCount == that.maxQueryCount &&
           selectThreshold == that.selectThreshold &&
           useApproximateCountDistinct == that.useApproximateCountDistinct &&
           useApproximateTopN == that.useApproximateTopN &&
           joinEnabled == that.joinEnabled &&
           requireTimeCondition == that.requireTimeCondition &&
           dumpPlan == that.dumpPlan &&
           binaryOperandsCastAdjust == that.binaryOperandsCastAdjust &&
           Objects.equals(sqlTimeZone, that.sqlTimeZone);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        maxTopNLimit,
        maxQueryCount,
        selectThreshold,
        useApproximateCountDistinct,
        useApproximateTopN,
        joinEnabled,
        requireTimeCondition,
        dumpPlan,
        binaryOperandsCastAdjust,
        sqlTimeZone
    );
  }

  @Override
  public String toString()
  {
    return "PlannerConfig{" +
           "maxTopNLimit=" + maxTopNLimit +
           ", maxQueryCount=" + maxQueryCount +
           ", selectThreshold=" + selectThreshold +
           ", useApproximateCountDistinct=" + useApproximateCountDistinct +
           ", useApproximateTopN=" + useApproximateTopN +
           ", joinEnabled=" + joinEnabled +
           ", requireTimeCondition=" + requireTimeCondition +
           ", dumpPlan=" + dumpPlan +
           ", binaryOperandsCastAdjust=" + binaryOperandsCastAdjust +
           ", sqlTimeZone=" + sqlTimeZone +
           '}';
  }
}
