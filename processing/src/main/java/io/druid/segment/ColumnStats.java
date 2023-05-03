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

package io.druid.segment;

import io.druid.data.Rows;
import io.druid.data.ValueType;

import java.util.Map;
import java.util.Objects;

public interface ColumnStats
{
  String MIN = "min";
  String MAX = "max";
  String NUM_ZEROS = "numZeros";
  String NUM_NULLS = "numNulls";

  static Object get(Map<String, Object> stats, ValueType type, String key)
  {
    switch (type) {
      case STRING: return ColumnStats.getString(stats, key);
      case DOUBLE: return ColumnStats.getDouble(stats, key);
      case FLOAT: return ColumnStats.getFloat(stats, key);
      case LONG: return ColumnStats.getLong(stats, key);
    }
    return null;
  }

  static String getString(Map<String, Object> stats, String key)
  {
    return stats == null ? null : Objects.toString(stats.get(key), null);
  }

  static Double getDouble(Map<String, Object> stats, String key)
  {
    Object parsed = stats == null ? null : Rows.parseDoubleIfPossible(stats.get(key));
    return parsed instanceof Double ? (Double) parsed : null;
  }

  static Float getFloat(Map<String, Object> stats, String key)
  {
    Object parsed = stats == null ? null : Rows.parseFloatIfPossible(stats.get(key));
    return parsed instanceof Float ? (Float) parsed : null;
  }

  static Long getLong(Map<String, Object> stats, String key)
  {
    Object parsed = stats == null ? null : Rows.parseLongIfPossible(stats.get(key));
    return parsed instanceof Long ? (Long) parsed : null;
  }
}
