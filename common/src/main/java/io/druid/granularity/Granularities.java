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

package io.druid.granularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.joda.time.Period;

import java.util.Map;

/**
 * This class was created b/c sometimes  static initializers of a class that use a subclass can deadlock.
 * See: #2979, #3979
 */
public class Granularities
{
  public static final Granularity SECOND = GranularityType.SECOND.getDefaultGranularity();
  public static final Granularity MINUTE = GranularityType.MINUTE.getDefaultGranularity();
  public static final Granularity FIVE_MINUTE = GranularityType.FIVE_MINUTE.getDefaultGranularity();
  public static final Granularity TEN_MINUTE = GranularityType.TEN_MINUTE.getDefaultGranularity();
  public static final Granularity FIFTEEN_MINUTE = GranularityType.FIFTEEN_MINUTE.getDefaultGranularity();
  public static final Granularity THIRTY_MINUTE = GranularityType.THIRTY_MINUTE.getDefaultGranularity();
  public static final Granularity HOUR = GranularityType.HOUR.getDefaultGranularity();
  public static final Granularity SIX_HOUR = GranularityType.SIX_HOUR.getDefaultGranularity();
  public static final Granularity DAY = GranularityType.DAY.getDefaultGranularity();
  public static final Granularity WEEK = GranularityType.WEEK.getDefaultGranularity();
  public static final Granularity MONTH = GranularityType.MONTH.getDefaultGranularity();
  public static final Granularity QUARTER = GranularityType.QUARTER.getDefaultGranularity();
  public static final Granularity YEAR = GranularityType.YEAR.getDefaultGranularity();
  public static final Granularity ALL = GranularityType.ALL.getDefaultGranularity();
  public static final Granularity NONE = GranularityType.NONE.getDefaultGranularity();

  public static Map<Period, GranularityType> MAPPING = Maps.newHashMap();

  static {
    for (GranularityType granularity : GranularityType.values()) {
      if (granularity.getPeriod() != null) {
        MAPPING.put(granularity.getPeriod(), granularity);
      }
    }
  }

  public static boolean isAll(Granularity granularity)
  {
    return granularity == null || ALL.equals(granularity);
  }

  private static final Map<Granularity, String> SERIALIZED = Maps.newConcurrentMap();

  public static String serialize(Granularity granularity, ObjectMapper mapper)
  {
    if (granularity != null) {
      return SERIALIZED.computeIfAbsent(granularity, k -> {
        try {
          return mapper.writeValueAsString(granularity);
        }
        catch (Exception e) {
          return null;
        }
      });
    }
    return null;
  }

  public static int getOnlyDurationIndex(Period period)
  {
    int index = -1;
    final int[] values = period.getValues();
    for (int i = 0; i < values.length; i++) {
      if (values[i] > 0) {
        if (index >= 0) {
          return -1;
        }
        index = i;
      }
    }
    return index;
  }
}
