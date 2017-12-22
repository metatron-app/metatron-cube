/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.parsers.ParseException;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;

/**
 */
public abstract class AbstractRow implements Row
{
  private static final Function<Object, String> TO_STRING_INCLUDING_NULL = new Function<Object, String>()
  {
    @Override
    public String apply(final Object o)
    {
      return String.valueOf(o);
    }
  };

  @Override
  public float getFloatMetric(String metric)
  {
    return parseFloat(metric, getRaw(metric));
  }

  @Override
  public double getDoubleMetric(String metric)
  {
    return parseDouble(metric, getRaw(metric));
  }

  @Override
  public long getLongMetric(String metric)
  {
    return parseLong(metric, getRaw(metric));
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    final Object dimValue = getRaw(dimension);

    if (dimValue == null) {
      return Collections.emptyList();
    } else if (dimValue instanceof List) {
      // guava's toString function fails on null objects, so please do not use it
      return Lists.transform(
          (List) dimValue,
          TO_STRING_INCLUDING_NULL
      );
    } else {
      return Collections.singletonList(String.valueOf(dimValue));
    }
  }

  @Override
  public long getTimestampFromEpoch()
  {
    throw new UnsupportedOperationException("getTimestampFromEpoch");
  }

  @Override
  public DateTime getTimestamp()
  {
    throw new UnsupportedOperationException("getTimestamp");
  }

  @Override
  public int compareTo(Row o)
  {
    throw new UnsupportedOperationException("compareTo");
  }

  public static float parseFloat(String column, Object value)
  {
    if (value == null) {
      return 0F;
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      try {
        return tryParseFloat((String) value);
      }
      catch (Exception e) {
        throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", column, value);
      }
    } else {
      throw new ParseException("Unknown type[%s]", value.getClass());
    }
  }

  public static double parseDouble(String column, Object value)
  {
    if (value == null) {
      return 0D;
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      try {
        return tryParseDouble((String) value);
      }
      catch (Exception e) {
        throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", column, value);
      }
    } else {
      throw new ParseException("Unknown type[%s]", value.getClass());
    }
  }

  public static long parseLong(String column, Object value)
  {
    if (value == null) {
      return 0L;
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      try {
        return tryParseLong((String) value);
      }
      catch (Exception e) {
        throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", column, value);
      }
    } else {
      throw new ParseException("Unknown type[%s]", value.getClass());
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
