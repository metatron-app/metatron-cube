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
import io.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import java.util.Date;

/**
 */
public class Rows
{
  private static final String NULL = "null";
  private static final String NAN = "nan";

  public static Boolean parseBoolean(Object value)
  {
    final Object parsed = parseBooleanIfPossible(value);
    if (parsed == null || parsed instanceof Boolean) {
      return (Boolean) parsed;
    }
    throw new ParseException("Unable to parse boolean from value[%s]", value);
  }

  public static Boolean parseBoolean(Object value, Boolean defaultValue)
  {
    final Object parsed = parseBooleanIfPossible(value);
    if (parsed == null || parsed instanceof Boolean) {
      return (Boolean) parsed;
    }
    return defaultValue;
  }

  public static Object parseBooleanIfPossible(Object value)
  {
    if (value == null || value instanceof Boolean) {
      return value;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue() != 0;
    } else if (value instanceof String) {
      final String s = (String) value;
      if (s.isEmpty() || NULL.equalsIgnoreCase(s)) {
        return null;
      } else if (Character.isLetter(s.charAt(0))) {
        return Boolean.valueOf(s);
      } else if (s.length() == 1 && s.charAt(0) == '0') {
        return false;
      } else {
        final Long parsed = Longs.tryParse(s);
        return parsed != null && parsed != 0;
      }
    } else if (value instanceof DateTime) {
      return ((DateTime) value).getMillis() != 0;
    } else if (value instanceof Date) {
      return ((Date) value).getTime() != 0;
    } else {
      return value;
    }
  }

  public static Float parseFloat(Object value)
  {
    final Object parsed = parseFloatIfPossible(value);
    if (parsed == null || parsed instanceof Float) {
      return (Float) parsed;
    }
    throw new ParseException("Unable to parse float from value[%s]", value);
  }

  public static Float parseFloat(Object value, Float defaultValue)
  {
    final Object parsed = parseFloatIfPossible(value);
    if (parsed == null || parsed instanceof Float) {
      return (Float) parsed;
    }
    return defaultValue;
  }

  public static Object parseFloatIfPossible(Object value)
  {
    if (value == null || value instanceof Float) {
      return value;
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      final String s = (String) value;
      return s.isEmpty() || NULL.equalsIgnoreCase(s) ? null : NAN.equalsIgnoreCase(s) ? Float.NaN : tryParseFloat(s);
    } else if (value instanceof DateTime) {
      return (float) ((DateTime) value).getMillis();
    } else if (value instanceof Date) {
      return (float) ((Date) value).getTime();
    } else {
      return value;
    }
  }

  public static Double parseDouble(Object value)
  {
    final Object parsed = parseDoubleIfPossible(value);
    if (parsed == null || parsed instanceof Double) {
      return (Double) parsed;
    }
    throw new ParseException("Unable to parse double from value[%s]", value);
  }

  public static Double parseDouble(Object value, Double defaultValue)
  {
    final Object parsed = parseDoubleIfPossible(value);
    if (parsed == null || parsed instanceof Double) {
      return (Double) parsed;
    }
    return defaultValue;
  }

  public static Object parseDoubleIfPossible(Object value)
  {
    if (value == null || value instanceof Double) {
      return value;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      final String s = (String) value;
      return s.isEmpty() || NULL.equalsIgnoreCase(s) ? null : NAN.equalsIgnoreCase(s) ? Double.NaN : tryParseDouble(s);
    } else if (value instanceof DateTime) {
      return (double) ((DateTime) value).getMillis();
    } else if (value instanceof Date) {
      return (double) ((Date) value).getTime();
    } else {
      return value;
    }
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
    final Object parsed = parseLongIfPossible(value);
    if (parsed == null || parsed instanceof Long) {
      return (Long) parsed;
    }
    throw new ParseException("Unable to parse long from value[%s]", value);
  }

  public static Integer parseInt(Object value, Integer defaultValue)
  {
    final Object parsed = parseLongIfPossible(value);
    if (parsed == null || parsed instanceof Long) {
      return ((Long) parsed).intValue();
    }
    return defaultValue;
  }

  public static Long parseLong(Object value, Long defaultValue)
  {
    final Object parsed = parseLongIfPossible(value);
    if (parsed == null || parsed instanceof Long) {
      return (Long) parsed;
    }
    return defaultValue;
  }

  public static Object parseLongIfPossible(Object value)
  {
    if (value == null || value instanceof Long) {
      return value;
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      final String s = (String) value;
      return s.isEmpty() || NULL.equalsIgnoreCase(s) ? null : tryParseLong(s);
    } else if (value instanceof DateTime) {
      return ((DateTime) value).getMillis();
    } else if (value instanceof Date) {
      return ((Date) value).getTime();
    } else {
      return value;
    }
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
