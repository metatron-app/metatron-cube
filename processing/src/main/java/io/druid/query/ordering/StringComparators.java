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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.netty.util.internal.StringUtil;
import org.joda.time.DateTimeConstants;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigDecimal;
import java.text.DateFormatSymbols;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;


public class StringComparators
{
  public static final String LEXICOGRAPHIC_NAME = "lexicographic";
  public static final String ALPHANUMERIC_NAME = "alphanumeric";
  public static final String INTEGER_NAME = "integer";
  public static final String LONG_NAME = "long";
  public static final String FLOATING_POINT_NAME = "floatingpoint";
  public static final String NUMERIC_NAME = "numeric";
  public static final String DAY_OF_WEEK_NAME = "dayofweek";
  public static final String MONTH_NAME = "month";
  public static final String DATETIME_NAME = "datetime";

  public static final String STRING_ARRAY_NAME = "stringarray";

  public static final LexicographicComparator LEXICOGRAPHIC = new LexicographicComparator();
  public static final AlphanumericComparator ALPHANUMERIC = new AlphanumericComparator();
  public static final IntegerComparator INTEGER = new IntegerComparator();
  public static final LongComparator LONG = new LongComparator();
  public static final FloatingPointComparator FLOATING_POINT = new FloatingPointComparator();
  public static final NumericComparator NUMERIC = new NumericComparator();
  public static final DayOfWeekComparator DAY_OF_WEEK = new DayOfWeekComparator(null);
  public static final MonthComparator MONTH = new MonthComparator(null);

  public static boolean isLexicographicString(String ordering)
  {
    return ordering.equalsIgnoreCase(StringComparators.LEXICOGRAPHIC_NAME) ||
           ordering.equalsIgnoreCase(ValueDesc.STRING_TYPE);
  }

  public static StringComparator revert(final StringComparator comparator)
  {
    return new StringComparator()
    {
      @Override
      public int compare(String o1, String o2)
      {
        return -comparator.compare(o1, o2);
      }
    };
  }

  public static abstract class AbstractStringComparator implements StringComparator
  {
    @Override
    public final int compare(String s, String s2)
    {
      // Avoid conversion to bytes for equal references
      if (s == s2) {
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

    protected abstract int _compare(String s, String s2);
  }

  public static class LexicographicComparator extends AbstractStringComparator
  {
    @Override
    protected final int _compare(String s, String s2)
    {
      return UnsignedBytes.lexicographicalComparator().compare(
          StringUtils.toUtf8(s),
          StringUtils.toUtf8(s2)
      );
    }

    @Override
    public String toString()
    {
      return StringComparators.LEXICOGRAPHIC_NAME;
    }
  }

  public static class StringArrayComparator extends AbstractStringComparator
  {
    private final String format;
    private final char separator;
    private final StringComparator[] comparators;

    public StringArrayComparator(String format, char separator, StringComparator[] comparators)
    {
      this.format = format;
      this.separator = separator;
      this.comparators = comparators;
    }

    @Override
    protected final int _compare(String s1, String s2)
    {
      int i1 = 0;
      int i2 = 0;
      int index = 0;
      while (true) {
        int index1 = s1.indexOf(separator, i1);
        int index2 = s2.indexOf(separator, i2);
        if (index1 >= 0 && index2 >= 0) {
          int compare = _compareElement(s1.substring(i1, index1), s2.substring(i2, index2), index++);
          if (compare != 0) {
            return compare;
          }
          i1 = index1 + 1;
          i2 = index2 + 1;
        } else if (index1 < 0 && index2 < 0) {
          return _compareElement(s1.substring(i1), s2.substring(i2), index);
        } else {
          return index2 < 0 ? 1 : -1;
        }
      }
    }

    private int _compareElement(String e1, String e2, int index)
    {
      if (comparators.length > 0) {
        return comparators[index].compare(e1, e2);
      } else {
        return e1.compareTo(e2);
      }
    }

    @Override
    public String toString()
    {
      return StringComparators.STRING_ARRAY_NAME + "(" + format + ")";
    }
  }

  public static class AlphanumericComparator extends AbstractStringComparator
  {
    // This code is based on https://github.com/amjjd/java-alphanum, see
    // NOTICE file for more information
    @Override
    protected final int _compare(String str1, String str2)
    {
      if (str1 == null) {
        return -1;
      } else if (str2 == null) {
        return 1;
      } else if (str1.length() == 0) {
        return str2.length() == 0 ? 0 : -1;
      } else if (str2.length() == 0) {
        return 1;
      }

      final int[] pos = {0, 0};

      while (pos[0] < str1.length() && pos[1] < str2.length()) {
        final int ch1 = str1.codePointAt(pos[0]);
        final int ch2 = str2.codePointAt(pos[1]);

        final int result;

        if (isDigit(ch1)) {
          result = isDigit(ch2) ? compareNumbers(str1, str2, pos) : -1;
        } else {
          result = isDigit(ch2) ? 1 : compareNonNumeric(str1, str2, pos);
        }

        if (result != 0) {
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
      while (pos[0] < str0.length() && isZero(ch0 = str0.codePointAt(pos[0]))) {
        zeroes0++;
        pos[0] += Character.charCount(ch0);
      }
      while (pos[1] < str1.length() && isZero(ch1 = str1.codePointAt(pos[1]))) {
        zeroes1++;
        pos[1] += Character.charCount(ch1);
      }

      // If one sequence contains more significant digits than the
      // other, it's a larger number. In case they turn out to have
      // equal lengths, we compare digits at each position; the first
      // unequal pair determines which is the bigger number.
      while (true) {
        boolean noMoreDigits0 = (ch0 < 0) || !isDigit(ch0);
        boolean noMoreDigits1 = (ch1 < 0) || !isDigit(ch1);

        if (noMoreDigits0 && noMoreDigits1) {
          return delta != 0 ? delta : zeroes0 - zeroes1;
        } else if (noMoreDigits0) {
          return -1;
        } else if (noMoreDigits1) {
          return 1;
        } else if (delta == 0 && ch0 != ch1) {
          delta = valueOf(ch0) - valueOf(ch1);
        }

        if (pos[0] < str0.length()) {
          ch0 = str0.codePointAt(pos[0]);
          if (isDigit(ch0)) {
            pos[0] += Character.charCount(ch0);
          } else {
            ch0 = -1;
          }
        } else {
          ch0 = -1;
        }

        if (pos[1] < str1.length()) {
          ch1 = str1.codePointAt(pos[1]);
          if (isDigit(ch1)) {
            pos[1] += Character.charCount(ch1);
          } else {
            ch1 = -1;
          }
        } else {
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
      if (digit <= '9') {
        return digit - '0';
      }
      if (digit <= '\u0669') {
        return digit - '\u0660';
      }
      if (digit <= '\u06F9') {
        return digit - '\u06F0';
      }
      if (digit <= '\u096F') {
        return digit - '\u0966';
      }
      if (digit <= '\uFF19') {
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
      while (pos[0] < str0.length() && !isDigit(ch0 = str0.codePointAt(pos[0]))) {
        pos[0] += Character.charCount(ch0);
      }

      int start1 = pos[1];
      int ch1 = str1.codePointAt(pos[1]);
      pos[1] += Character.charCount(ch1);
      while (pos[1] < str1.length() && !isDigit(ch1 = str1.codePointAt(pos[1]))) {
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
    protected final int _compare(String s, String s2)
    {
      return Ints.compare(Integer.valueOf(s), Integer.valueOf(s2));
    }

    @Override
    public String toString()
    {
      return StringComparators.INTEGER_NAME;
    }
  }

  public static class LongComparator extends AbstractStringComparator implements StringComparator
  {
    @Override
    protected final int _compare(String s, String s2)
    {
      return Longs.compare(Long.valueOf(s), Long.valueOf(s2));
    }

    @Override
    public String toString()
    {
      return StringComparators.LONG_NAME;
    }
  }

  public static class FloatingPointComparator extends AbstractStringComparator implements StringComparator
  {
    @Override
    protected final int _compare(String s, String s2)
    {
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

  public static class NumericComparator extends AbstractStringComparator implements StringComparator
  {
    @Override
    protected final int _compare(String o1, String o2)
    {
      // Creating a BigDecimal from a String is expensive (involves copying the String into a char[])
      // Converting the String to a Long first is faster.
      // We optimize here with the assumption that integer values are more common than floating point.
      Long long1 = GuavaUtils.tryParseLong(o1);
      Long long2 = GuavaUtils.tryParseLong(o2);

      if (long1 != null && long2 != null) {
        return Long.compare(long1, long2);
      }

      final BigDecimal bd1 = long1 == null ? convertStringToBigDecimal(o1) : new BigDecimal(long1);
      final BigDecimal bd2 = long2 == null ? convertStringToBigDecimal(o2) : new BigDecimal(long2);

      if (bd1 != null && bd2 != null) {
        return bd1.compareTo(bd2);
      }

      if (bd1 == null && bd2 == null) {
        // both Strings are unparseable, just compare lexicographically to have a well-defined ordering
        return StringComparators.LEXICOGRAPHIC.compare(o1, o2);
      }

      if (bd1 == null) {
        return -1;
      } else {
        return 1;
      }
    }

    @Override
    public String toString()
    {
      return StringComparators.NUMERIC_NAME;
    }
  }

  private static BigDecimal convertStringToBigDecimal(String input)
  {
    // treat unparseable Strings as nulls
    BigDecimal bd = null;
    try {
      bd = new BigDecimal(input);
    }
    catch (NumberFormatException ex) {
      // ignore
    }
    return bd;
  }

  public static class DayOfWeekComparator extends AbstractStringComparator implements StringComparator
  {
    private final String language;
    private final Map<String, Integer> codes = Maps.newHashMap();

    private DayOfWeekComparator(String type)
    {
      Locale locale = type == null ? Locale.getDefault(Locale.Category.FORMAT) : new Locale(type);
      DateFormatSymbols symbols = new DateFormatSymbols(locale);
      String[] wd = symbols.getWeekdays();
      String[] swd = symbols.getShortWeekdays();

      codes.put(wd[Calendar.MONDAY].toUpperCase(), DateTimeConstants.MONDAY);
      codes.put(swd[Calendar.MONDAY].toUpperCase(), DateTimeConstants.MONDAY);

      codes.put(wd[Calendar.TUESDAY].toUpperCase(), DateTimeConstants.TUESDAY);
      codes.put(swd[Calendar.TUESDAY].toUpperCase(), DateTimeConstants.TUESDAY);

      codes.put(wd[Calendar.WEDNESDAY].toUpperCase(), DateTimeConstants.WEDNESDAY);
      codes.put(swd[Calendar.WEDNESDAY].toUpperCase(), DateTimeConstants.WEDNESDAY);

      codes.put(wd[Calendar.THURSDAY].toUpperCase(), DateTimeConstants.THURSDAY);
      codes.put(swd[Calendar.THURSDAY].toUpperCase(), DateTimeConstants.THURSDAY);

      codes.put(wd[Calendar.FRIDAY].toUpperCase(), DateTimeConstants.FRIDAY);
      codes.put(swd[Calendar.FRIDAY].toUpperCase(), DateTimeConstants.FRIDAY);

      codes.put(wd[Calendar.SATURDAY].toUpperCase(), DateTimeConstants.SATURDAY);
      codes.put(swd[Calendar.SATURDAY].toUpperCase(), DateTimeConstants.SATURDAY);

      codes.put(wd[Calendar.SUNDAY].toUpperCase(), DateTimeConstants.SUNDAY);
      codes.put(swd[Calendar.SUNDAY].toUpperCase(), DateTimeConstants.SUNDAY);

      language = locale.getLanguage();
    }

    @Override
    public int _compare(String s1, String s2)
    {
      return Ints.compare(toCode(s1), toCode(s2));
    }

    private int toCode(String s)
    {
      Integer code = codes.get(s.toUpperCase());
      return code == null ? 0 : code;
    }

    @Override
    public String toString()
    {
      return StringComparators.DAY_OF_WEEK_NAME + "." + language;
    }
  }

  public static class MonthComparator extends AbstractStringComparator implements StringComparator
  {
    private final String language;
    private final Map<String, Integer> codes = Maps.newHashMap();

    private MonthComparator(String type)
    {
      Locale locale = type == null ? Locale.getDefault(Locale.Category.FORMAT) : new Locale(type);
      DateFormatSymbols symbols = new DateFormatSymbols(locale);
      String[] wd = symbols.getMonths();
      String[] swd = symbols.getShortMonths();

      codes.put(wd[Calendar.JANUARY].toUpperCase(), DateTimeConstants.JANUARY);
      codes.put(swd[Calendar.JANUARY].toUpperCase(), DateTimeConstants.JANUARY);

      codes.put(wd[Calendar.FEBRUARY].toUpperCase(), DateTimeConstants.FEBRUARY);
      codes.put(swd[Calendar.FEBRUARY].toUpperCase(), DateTimeConstants.FEBRUARY);

      codes.put(wd[Calendar.MARCH].toUpperCase(), DateTimeConstants.MARCH);
      codes.put(swd[Calendar.MARCH].toUpperCase(), DateTimeConstants.MARCH);

      codes.put(wd[Calendar.APRIL].toUpperCase(), DateTimeConstants.APRIL);
      codes.put(swd[Calendar.APRIL].toUpperCase(), DateTimeConstants.APRIL);

      codes.put(wd[Calendar.MAY].toUpperCase(), DateTimeConstants.MAY);
      codes.put(swd[Calendar.MAY].toUpperCase(), DateTimeConstants.MAY);

      codes.put(wd[Calendar.JUNE].toUpperCase(), DateTimeConstants.JUNE);
      codes.put(swd[Calendar.JUNE].toUpperCase(), DateTimeConstants.JUNE);

      codes.put(wd[Calendar.JULY].toUpperCase(), DateTimeConstants.JULY);
      codes.put(swd[Calendar.JULY].toUpperCase(), DateTimeConstants.JULY);

      codes.put(wd[Calendar.AUGUST].toUpperCase(), DateTimeConstants.AUGUST);
      codes.put(swd[Calendar.AUGUST].toUpperCase(), DateTimeConstants.AUGUST);

      codes.put(wd[Calendar.SEPTEMBER].toUpperCase(), DateTimeConstants.SEPTEMBER);
      codes.put(swd[Calendar.SEPTEMBER].toUpperCase(), DateTimeConstants.SEPTEMBER);

      codes.put(wd[Calendar.OCTOBER].toUpperCase(), DateTimeConstants.OCTOBER);
      codes.put(swd[Calendar.OCTOBER].toUpperCase(), DateTimeConstants.OCTOBER);

      codes.put(wd[Calendar.NOVEMBER].toUpperCase(), DateTimeConstants.NOVEMBER);
      codes.put(swd[Calendar.NOVEMBER].toUpperCase(), DateTimeConstants.NOVEMBER);

      codes.put(wd[Calendar.DECEMBER].toUpperCase(), DateTimeConstants.DECEMBER);
      codes.put(swd[Calendar.DECEMBER].toUpperCase(), DateTimeConstants.DECEMBER);

      language = locale.getLanguage();
    }

    @Override
    public int _compare(String s1, String s2)
    {
      return Ints.compare(toCode(s1), toCode(s2));
    }

    private int toCode(String s)
    {
      Integer code = codes.get(s.toUpperCase());
      return code == null ? 0 : code;
    }

    @Override
    public String toString()
    {
      return StringComparators.MONTH_NAME + "." + language;
    }
  }

  public static class DateTimeComparator extends AbstractStringComparator
  {
    private final String format;
    private final DateTimeFormatter formatter;

    public DateTimeComparator(String format)
    {
      this.format = format;
      this.formatter = JodaUtils.toTimeFormatterQuoted(format);
    }

    @Override
    protected int _compare(String s, String s2)
    {
      return Longs.compare(formatter.parseMillis(s), formatter.parseMillis(s2));
    }

    @Override
    public String toString()
    {
      return StringComparators.DATETIME_NAME + "(" + format + ")";
    }
  }

  public static String validate(String type)
  {
    if (type != null) {
      makeComparator(type);
    }
    return type;
  }

  public static Comparator makeComparator(ValueDesc sourceType)
  {
    return makeComparator(sourceType, null);
  }

  // used in sketch aggregator
  public static Comparator makeComparator(ValueDesc sourceType, List<OrderingSpec> orderingSpecs)
  {
    boolean defaultOrdering = GuavaUtils.isNullOrEmpty(orderingSpecs);
    if (sourceType.isPrimitive()) {
      Preconditions.checkArgument(defaultOrdering || orderingSpecs.size() == 1);
      if (defaultOrdering) {
        return sourceType.comparator();
      }
      OrderingSpec orderingSpec = orderingSpecs.get(0);
      Comparator comparator = makeComparator(orderingSpec.getDimensionOrder());
      if (orderingSpec.getDirection() == Direction.DESCENDING) {
        comparator = Ordering.from(comparator).reverse();
      }
      return comparator;
    }
    Preconditions.checkArgument(sourceType.isStruct());
    String[] descriptive = TypeUtils.splitDescriptiveType(sourceType.typeName());
    List<String> elements = TypeUtils.splitWithEscape(descriptive[1], ',');
    Comparator[] comparators = new Comparator[elements.size()];
    if (defaultOrdering) {
      Arrays.fill(comparators, Ordering.natural().nullsFirst());
      return toStructComparator(comparators);
    }
    Preconditions.checkArgument(elements.size() >= orderingSpecs.size(), "not matching number of elements");
    for (int i = 0; i < elements.size(); i++) {
      ValueType elementType = ValueType.ofPrimitive(elements.get(i).trim());
      OrderingSpec orderingSpec = i < orderingSpecs.size() ? orderingSpecs.get(i) : null;
      if (orderingSpec != null) {
        String ordering = orderingSpec.getDimensionOrder();
        comparators[i] = elementType == ValueType.STRING ? makeComparator(ordering) : elementType.comparator();
        if (orderingSpec.getDirection() == Direction.DESCENDING) {
          comparators[i] = Ordering.from(comparators[i]).reverse();
        }
      } else {
        comparators[i] = elementType.comparator();
      }
    }
    return toStructComparator(comparators);
  }

  private static Comparator toStructComparator(final Comparator[] cx)
  {
    return new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(Object[] o1, Object[] o2)
      {
        int compare = 0;
        for (int i = 0; compare == 0 && i < cx.length; i++) {
          compare = cx[i].compare(o1[i], o2[i]);
        }
        return compare;
      }
    };
  }

  public static StringComparator makeComparator(String type)
  {
    StringComparator comparator = tryMakeComparator(type, LEXICOGRAPHIC);
    if (comparator == null) {
      throw new IAE("Unknown string comparator [%s]", type);
    }
    return comparator;
  }

  public static StringComparator tryMakeComparator(String type, StringComparator nullValue)
  {
    boolean descending = false;
    String lowerCased = type.toLowerCase();
    if (lowerCased.endsWith(":asc")) {
      type = type.substring(0, type.length() - 4);
    } else if (lowerCased.endsWith(":desc")) {
      type = type.substring(0, type.length() - 5);
      descending = true;
    }
    return _toComparator(type, nullValue, descending);
  }

  private static StringComparator _toComparator(String type, StringComparator nullValue, boolean descending)
  {
    StringComparator comparator = StringUtil.isNullOrEmpty(type) ? nullValue : _toStringComparator(type);
    if (comparator != null && descending) {
      comparator = revert(comparator);
    }
    return comparator;
  }

  private static StringComparator _toStringComparator(String type)
  {
    String lowerCased = type.toLowerCase();
    switch (lowerCased) {
      case StringComparators.LEXICOGRAPHIC_NAME:
        return LEXICOGRAPHIC;
      case StringComparators.ALPHANUMERIC_NAME:
        return ALPHANUMERIC;
      case StringComparators.INTEGER_NAME:
        return INTEGER;
      case StringComparators.LONG_NAME:
        return LONG;
      case StringComparators.FLOATING_POINT_NAME:
        return FLOATING_POINT;
      case StringComparators.NUMERIC_NAME:
        return NUMERIC;
      case StringComparators.DAY_OF_WEEK_NAME:
        return DAY_OF_WEEK;
      case StringComparators.MONTH_NAME:
        return MONTH;
      default:
        if (lowerCased.startsWith(StringComparators.DAY_OF_WEEK_NAME + ".")) {
          return new DayOfWeekComparator(type.substring(DAY_OF_WEEK_NAME.length() + 1));
        }
        if (lowerCased.startsWith(StringComparators.MONTH_NAME + ".")) {
          return new MonthComparator(type.substring(MONTH_NAME.length() + 1));
        }
        if (lowerCased.startsWith(StringComparators.DATETIME_NAME + "(")) {
          int seek = TypeUtils.seekWithEscape(type, DATETIME_NAME.length() + 1, ')');
          if (seek < 0) {
            throw new IllegalArgumentException("not matching ')' in " + type);
          }
          return new DateTimeComparator(type.substring(DATETIME_NAME.length() + 1, seek));
        }
        if (lowerCased.startsWith(StringComparators.STRING_ARRAY_NAME + "(")) {
          int seek = TypeUtils.seekWithEscape(type, STRING_ARRAY_NAME.length() + 1, ')');
          if (seek < 0) {
            throw new IllegalArgumentException("not matching ')' in " + type);
          }
          String argument = type.substring(STRING_ARRAY_NAME.length() + 1, seek);
          List<String> splits = TypeUtils.splitWithEscape(argument, ',');
          Preconditions.checkArgument(!splits.isEmpty() && splits.get(0).length() == 1, "separator should be a char");
          String separator = splits.get(0);
          List<StringComparator> comparators = Lists.newArrayList();
          for (int i = 1; i < splits.size(); i++) {
            comparators.add(makeComparator(splits.get(i)));
          }
          return new StringArrayComparator(argument, separator.charAt(0), comparators.toArray(new StringComparator[0]));
        }
        return null;
    }
  }

  public static String asComparatorName(char separator, List<DimensionSpec> dimensionSpecs)
  {
    StringBuilder builder = new StringBuilder();
    for (DimensionSpec dimension : dimensionSpecs) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      if (dimension instanceof DimensionSpecWithOrdering) {
        DimensionSpecWithOrdering explicit = (DimensionSpecWithOrdering) dimension;
        builder.append(explicit.getOrdering());
        if (explicit.getDirection() == Direction.DESCENDING) {
          builder.append(":desc");
        }
      } else {
        builder.append("lexicographic");
      }
    }
    return STRING_ARRAY_NAME + "(" + separator + "," + builder.toString() + ")";
  }
}
