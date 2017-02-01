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

package io.druid.query.ordering;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import org.joda.time.DateTimeConstants;

import java.util.Comparator;
import java.util.Objects;


public class StringComparators
{
  public static final String LEXICOGRAPHIC_NAME = "lexicographic";
  public static final String ALPHANUMERIC_NAME = "alphanumeric";
  public static final String INTEGER_NAME = "integer";
  public static final String FLOATING_POINT_NAME = "floatingpoint";
  public static final String DAY_OF_WEEK_NAME = "dayofweek";
  
  public static final LexicographicComparator LEXICOGRAPHIC = new LexicographicComparator();
  public static final AlphanumericComparator ALPHANUMERIC = new AlphanumericComparator();
  public static final IntegerComparator INTEGER = new IntegerComparator();
  public static final FloatingPointComparator FLOATING_POINT = new FloatingPointComparator();
  public static final DayOfWeekComparator DAY_OF_WEEK = new DayOfWeekComparator();
    
  @JsonTypeInfo(use=Id.NAME, include=As.PROPERTY, property="type", defaultImpl = LexicographicComparator.class)
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = StringComparators.LEXICOGRAPHIC_NAME, value = LexicographicComparator.class),
      @JsonSubTypes.Type(name = StringComparators.ALPHANUMERIC_NAME, value = AlphanumericComparator.class),
      @JsonSubTypes.Type(name = StringComparators.INTEGER_NAME, value = IntegerComparator.class),
      @JsonSubTypes.Type(name = StringComparators.FLOATING_POINT_NAME, value = FloatingPointComparator.class),
      @JsonSubTypes.Type(name = StringComparators.DAY_OF_WEEK_NAME, value = DayOfWeekComparator.class)
  })
  public static interface StringComparator extends Comparator<String>
  {
  }

  public static abstract class AbstractStringComparator implements Comparator<String>
  {
    @Override
    public final int compare(String s, String s2)
    {
      if (Objects.equals(s, s2)) {
        return 0;
      }
      // null first
      if (s == null) {
        return -1;
      }
      if (s2 == null) {
        return 1;
      }
      return _compare(s, s2);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return true;
    }

    public abstract int _compare(String s, String s2);
  }

  public static class LexicographicComparator implements StringComparator
  {
    @Override
    public int compare(String s, String s2)
    {
      // Avoid conversion to bytes for equal references
      if(s == s2){
        return 0;
      }
      // null first
      if (s == null) {
        return -1;
      }
      if (s2 == null) {
        return 1;
      }

      return UnsignedBytes.lexicographicalComparator().compare(
          StringUtils.toUtf8(s),
          StringUtils.toUtf8(s2)
      );
    }
    
    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      
      return true;
    }
    
    @Override
    public String toString()
    {
      return StringComparators.LEXICOGRAPHIC_NAME;
    }
  }
  
  public static class AlphanumericComparator implements StringComparator
  {
    // This code is based on https://github.com/amjjd/java-alphanum, see
    // NOTICE file for more information
    public int compare(String str1, String str2)
    {
      int[] pos =
      { 0, 0 };

      if (str1 == null)
      {
        return -1;
      } else if (str2 == null)
      {
        return 1;
      } else if (str1.length() == 0)
      {
        return str2.length() == 0 ? 0 : -1;
      } else if (str2.length() == 0)
      {
        return 1;
      }

      while (pos[0] < str1.length() && pos[1] < str2.length())
      {
        int ch1 = str1.codePointAt(pos[0]);
        int ch2 = str2.codePointAt(pos[1]);

        int result = 0;

        if (isDigit(ch1))
        {
          result = isDigit(ch2) ? compareNumbers(str1, str2, pos) : -1;
        } else
        {
          result = isDigit(ch2) ? 1 : compareNonNumeric(str1, str2, pos);
        }

        if (result != 0)
        {
          return result;
        }
      }

      return str1.length() - str2.length();
    }

    private int compareNumbers(String str0, String str1, int[] pos)
    {
      int delta = 0;
      int zeroes0 = 0, zeroes1 = 0;
      int ch0 = -1, ch1 = -1;

      // Skip leading zeroes, but keep a count of them.
      while (pos[0] < str0.length() && isZero(ch0 = str0.codePointAt(pos[0])))
      {
        zeroes0++;
        pos[0] += Character.charCount(ch0);
      }
      while (pos[1] < str1.length() && isZero(ch1 = str1.codePointAt(pos[1])))
      {
        zeroes1++;
        pos[1] += Character.charCount(ch1);
      }

      // If one sequence contains more significant digits than the
      // other, it's a larger number. In case they turn out to have
      // equal lengths, we compare digits at each position; the first
      // unequal pair determines which is the bigger number.
      while (true)
      {
        boolean noMoreDigits0 = (ch0 < 0) || !isDigit(ch0);
        boolean noMoreDigits1 = (ch1 < 0) || !isDigit(ch1);

        if (noMoreDigits0 && noMoreDigits1)
        {
          return delta != 0 ? delta : zeroes0 - zeroes1;
        } else if (noMoreDigits0)
        {
          return -1;
        } else if (noMoreDigits1)
        {
          return 1;
        } else if (delta == 0 && ch0 != ch1)
        {
          delta = valueOf(ch0) - valueOf(ch1);
        }

        if (pos[0] < str0.length())
        {
          ch0 = str0.codePointAt(pos[0]);
          if (isDigit(ch0))
          {
            pos[0] += Character.charCount(ch0);
          } else
          {
            ch0 = -1;
          }
        } else
        {
          ch0 = -1;
        }

        if (pos[1] < str1.length())
        {
          ch1 = str1.codePointAt(pos[1]);
          if (isDigit(ch1))
          {
            pos[1] += Character.charCount(ch1);
          } else
          {
            ch1 = -1;
          }
        } else
        {
          ch1 = -1;
        }
      }
    }

    private boolean isDigit(int ch)
    {
      return (ch >= '0' && ch <= '9') ||
          (ch >= '\u0660' && ch <= '\u0669') ||
          (ch >= '\u06F0' && ch <= '\u06F9') ||
          (ch >= '\u0966' && ch <= '\u096F') ||
          (ch >= '\uFF10' && ch <= '\uFF19');
    }

    private boolean isZero(int ch)
    {
      return ch == '0' || ch == '\u0660' || ch == '\u06F0' || ch == '\u0966' || ch == '\uFF10';
    }

    private int valueOf(int digit)
    {
      if (digit <= '9')
      {
        return digit - '0';
      }
      if (digit <= '\u0669')
      {
        return digit - '\u0660';
      }
      if (digit <= '\u06F9')
      {
        return digit - '\u06F0';
      }
      if (digit <= '\u096F')
      {
        return digit - '\u0966';
      }
      if (digit <= '\uFF19')
      {
        return digit - '\uFF10';
      }

      return digit;
    }

    private int compareNonNumeric(String str0, String str1, int[] pos)
    {
      // find the end of both non-numeric substrings
      int start0 = pos[0];
      int ch0 = str0.codePointAt(pos[0]);
      pos[0] += Character.charCount(ch0);
      while (pos[0] < str0.length() && !isDigit(ch0 = str0.codePointAt(pos[0])))
      {
        pos[0] += Character.charCount(ch0);
      }

      int start1 = pos[1];
      int ch1 = str1.codePointAt(pos[1]);
      pos[1] += Character.charCount(ch1);
      while (pos[1] < str1.length() && !isDigit(ch1 = str1.codePointAt(pos[1])))
      {
        pos[1] += Character.charCount(ch1);
      }

      // compare the substrings
      return String.CASE_INSENSITIVE_ORDER.compare(str0.substring(start0, pos[0]), str1.substring(start1, pos[1]));
    }
    
    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      
      return true;
    }
    
    @Override
    public String toString()
    {
      return StringComparators.ALPHANUMERIC_NAME;
    }
  }

  public static class IntegerComparator extends AbstractStringComparator implements StringComparator
  {
    @Override
    public final int _compare(String s, String s2)
    {
      // Avoid conversion to bytes for equal references
      if(s == s2){
        return 0;
      }
      // null first
      if (s == null) {
        return -1;
      }
      if (s2 == null) {
        return 1;
      }

      return Ints.compare(Integer.valueOf(s), Integer.valueOf(s2));
    }

    @Override
    public String toString()
    {
      return StringComparators.INTEGER_NAME;
    }
  }

  public static class FloatingPointComparator extends AbstractStringComparator implements StringComparator
  {
    @Override
    public final int _compare(String s, String s2)
    {
      // Avoid conversion to bytes for equal references
      if(s == s2){
        return 0;
      }
      // null first
      if (s == null) {
        return -1;
      }
      if (s2 == null) {
        return 1;
      }

      // try quick path
      Long l1 = s.indexOf('.') < 0 ? Longs.tryParse(s) : null;
      Long l2 = s2.indexOf('.') < 0 ? Longs.tryParse(s2) : null;

      if (l1 != null) {
        if (l2 != null) {
          return Longs.compare(l1, l2);
        } else {
          return Double.compare(l1, Double.valueOf(s2));
        }
      }
      if (l2 != null) {
        return Double.compare(Double.valueOf(s), l2);
      } else {
        return Double.compare(Double.valueOf(s), Double.valueOf(s2));
      }
    }

    @Override
    public String toString()
    {
      return StringComparators.FLOATING_POINT_NAME;
    }
  }

  public static class DayOfWeekComparator extends AbstractStringComparator implements StringComparator
  {
    @Override
    public int _compare(String s1, String s2)
    {
      return Ints.compare(toCode(s1), toCode(s2));
    }

    private int toCode(String s)
    {
      switch (s.toUpperCase()) {
        case "MONDAY": return DateTimeConstants.MONDAY;
        case "TUESDAY": return DateTimeConstants.TUESDAY;
        case "WEDNESDAY": return DateTimeConstants.WEDNESDAY;
        case "THURSDAY": return DateTimeConstants.THURSDAY;
        case "FRIDAY": return DateTimeConstants.FRIDAY;
        case "SATURDAY": return DateTimeConstants.SATURDAY;
        case "SUNDAY": return DateTimeConstants.SUNDAY;
        default: return DateTimeConstants.SUNDAY + 1;
      }
    }

    @Override
    public String toString()
    {
      return StringComparators.DAY_OF_WEEK_NAME;
    }
  }

  public static StringComparator makeComparator(String type)
  {
    switch (type) {
      case StringComparators.LEXICOGRAPHIC_NAME:
        return LEXICOGRAPHIC;
      case StringComparators.ALPHANUMERIC_NAME:
        return ALPHANUMERIC;
      case StringComparators.INTEGER_NAME:
        return INTEGER;
      case StringComparators.FLOATING_POINT_NAME:
        return FLOATING_POINT;
      case StringComparators.DAY_OF_WEEK_NAME:
        return DAY_OF_WEEK;
      default:
        throw new IAE("Unknown string comparator[%s]", type);
    }
  }
}
