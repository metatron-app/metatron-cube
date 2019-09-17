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

package io.druid.data;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.parsers.ParseException;
import io.druid.common.utils.StringUtils;
import org.joda.time.DateTime;

/**
 */
public class Rows
{
  public static Boolean parseBoolean(Object value)
  {
    final Comparable parsed = parseBooleanIfPossible(value);
    if (parsed == null || parsed instanceof Boolean) {
      return (Boolean) parsed;
    }
    throw new ParseException("Unable to parse boolean from value[%s]", value);
  }

  public static Boolean parseBoolean(Object value, Boolean defaultValue)
  {
    final Comparable parsed = parseBooleanIfPossible(value);
    if (parsed == null || parsed instanceof Boolean) {
      return (Boolean) parsed;
    }
    return defaultValue;
  }

  public static Comparable parseBooleanIfPossible(Object value)
  {
    if (StringUtils.isNullOrEmpty(value)) {
      return null;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue() != 0;
    } else if (value instanceof DateTime) {
      return (float) ((DateTime) value).getMillis() != 0;
    } else if (value instanceof String) {
      return Boolean.valueOf((String) value);
    } else {
      return value instanceof Comparable ? (Comparable) value : null;
    }
  }

  public static Float parseFloat(Object value)
  {
    final Comparable parsed = parseFloatIfPossible(value);
    if (parsed == null || parsed instanceof Float) {
      return (Float) parsed;
    }
    throw new ParseException("Unable to parse float from value[%s]", value);
  }

  public static Float parseFloat(Object value, Float defaultValue)
  {
    final Comparable parsed = parseFloatIfPossible(value);
    if (parsed == null || parsed instanceof Float) {
      return (Float) parsed;
    }
    return defaultValue;
  }

  public static Comparable parseFloatIfPossible(Object value)
  {
    if (StringUtils.isNullOrEmpty(value)) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof DateTime) {
      return (float) ((DateTime) value).getMillis();
    } else if (value instanceof String) {
      try {
        return tryParseFloat((String) value);
      }
      catch (Exception e) {
        // ignore
      }
    }
    return value instanceof Comparable ? (Comparable) value : null;
  }

  public static Double parseDouble(Object value)
  {
    final Comparable parsed = parseDoubleIfPossible(value);
    if (parsed == null || parsed instanceof Double) {
      return (Double) parsed;
    }
    throw new ParseException("Unable to parse double from value[%s]", value);
  }

  public static Double parseDouble(Object value, Double defaultValue)
  {
    final Comparable parsed = parseDoubleIfPossible(value);
    if (parsed == null || parsed instanceof Double) {
      return (Double) parsed;
    }
    return defaultValue;
  }

  public static Comparable parseDoubleIfPossible(Object value)
  {
    if (StringUtils.isNullOrEmpty(value)) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof DateTime) {
      return (double) ((DateTime) value).getMillis();
    } else if (value instanceof String) {
      try {
        return tryParseDouble((String) value);
      }
      catch (Exception e) {
        // ignore
      }
    }
    return value instanceof Comparable ? (Comparable) value : null;
  }

  public static Double round(Double value, int round)
  {
    if (value == null || Double.isInfinite(value) || Double.isNaN(value) || round <= 0) {
      return value;
    }
    double abs = Math.abs(value);
    while (abs < 1) {
      abs *= 10;
      round++;
    }
    double remains = Math.pow(10, round);
    return Math.round(value * remains) / remains;
  }

  public static Long parseLong(Object value)
  {
    final Comparable parsed = parseLongIfPossible(value);
    if (parsed == null || parsed instanceof Long) {
      return (Long) parsed;
    }
    throw new ParseException("Unable to parse long from value[%s]", value);
  }

  public static Long parseLong(Object value, Long defaultValue)
  {
    final Comparable parsed = parseLongIfPossible(value);
    if (parsed == null || parsed instanceof Long) {
      return (Long) parsed;
    }
    return defaultValue;
  }

  public static Comparable parseLongIfPossible(Object value)
  {
    if (StringUtils.isNullOrEmpty(value)) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof DateTime) {
      return ((DateTime) value).getMillis();
    } else if (value instanceof String) {
      try {
        return tryParseLong((String) value);
      }
      catch (Exception ex) {
        // ignore
      }
    }
    return value instanceof Comparable ? (Comparable) value : null;
  }

  // long -> double -> long can make different value
  public static long tryParseLong(final String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return 0;
    }
    int i = 0;
    final char first = value.charAt(0);
    if (first == '+' || first == '-') {
      i++;
    }
    boolean allDigit = true;
    boolean containsComma = false;
    for (; i < value.length(); i++) {
      final char aChar = value.charAt(i);
      if (aChar == ',') {
        containsComma = true;
      } else if (allDigit && !Character.isDigit(aChar)) {
        allDigit = false;
      }
    }
    final String target = containsComma ? removeCommaAndFirstPlus(value) : removeFirstPlus(value);
    if (allDigit) {
      Long longValue = Longs.tryParse(target);
      if (longValue != null) {
        return longValue;
      }
    }
    return Double.valueOf(target).longValue();
  }

  public static int tryParseInt(final String value)
  {
    return Ints.checkedCast(tryParseLong(value));
  }

  public static float tryParseFloat(final String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return 0f;
    }
    int i = 0;
    final char first = value.charAt(0);
    if (first == '+' || first == '-') {
      i++;
    }
    boolean allDigit = true;
    boolean containsComma = false;
    for (; i < value.length(); i++) {
      final char aChar = value.charAt(i);
      if (aChar == ',') {
        containsComma = true;
      } else if (allDigit && !Character.isDigit(aChar)) {
        allDigit = false;
      }
    }
    final String target = containsComma ? removeCommaAndFirstPlus(value) : removeFirstPlus(value);
    if (allDigit) {
      Long longValue = Longs.tryParse(target);
      if (longValue != null) {
        return longValue.floatValue();
      }
    }
    return Float.valueOf(target);
  }

  public static double tryParseDouble(final String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return 0d;
    }
    int i = 0;
    final char first = value.charAt(0);
    if (first == '+' || first == '-') {
      i++;
    }
    boolean allDigit = true;
    boolean containsComma = false;
    for (; i < value.length(); i++) {
      final char aChar = value.charAt(i);
      if (aChar == ',') {
        containsComma = true;
      } else if (allDigit && !Character.isDigit(aChar)) {
        allDigit = false;
      }
    }
    final String target = containsComma ? removeCommaAndFirstPlus(value) : removeFirstPlus(value);
    if (allDigit) {
      Long longValue = Longs.tryParse(target);
      if (longValue != null) {
        return longValue.doubleValue();
      }
    }
    return Double.valueOf(target);
  }

  private static String removeFirstPlus(final String value)
  {
    return value.charAt(0) == '+' ? value.substring(1) : value;
  }

  private static String removeCommaAndFirstPlus(final String value)
  {
    StringBuilder builder = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      final char aChar = value.charAt(i);
      if (i == 0 && aChar == '+') {
        continue;
      }
      if (aChar != ',') {
        builder.append(aChar);
      }
    }
    return builder.toString();
  }
}
