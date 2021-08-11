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

package io.druid.common.utils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeUtils;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.QuarterFieldType;
import org.joda.time.chrono.WeekInMonthFieldType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import sun.util.calendar.ZoneInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.TreeSet;

/**
 */
public class JodaUtils
{
  // limit intervals such that duration millis fits in a long
  public static final long MAX_INSTANT = Long.MAX_VALUE / 2;
  public static final long MIN_INSTANT = Long.MIN_VALUE / 2;

  private static final Comparator<Interval> INTERVAL_BY_START_THEN_END = new Comparator<Interval>()
  {
    private final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd();

    @Override
    public int compare(Interval lhs, Interval rhs)
    {
      if (lhs.getChronology().equals(rhs.getChronology())) {
        int compare = Long.compare(lhs.getStartMillis(), rhs.getStartMillis());
        if (compare == 0) {
          return Long.compare(lhs.getEndMillis(), rhs.getEndMillis());
        }
        return compare;
      }
      return comparator.compare(lhs, rhs);
    }
  };

  private static final Comparator<Interval> INTERVAL_BY_END_THEN_START = new Comparator<Interval>()
  {
    private final Comparator<Interval> comparator = Comparators.intervalsByEndThenStart();

    @Override
    public int compare(Interval lhs, Interval rhs)
    {
      if (lhs.getChronology().equals(rhs.getChronology())) {
        int compare = Long.compare(lhs.getEndMillis(), rhs.getEndMillis());
        if (compare == 0) {
          return Long.compare(lhs.getStartMillis(), rhs.getStartMillis());
        }
        return compare;
      }
      return comparator.compare(lhs, rhs);
    }
  };

  public static Comparator<Interval> intervalsByStartThenEnd()
  {
    return INTERVAL_BY_START_THEN_END;
  }

  public static Comparator<Interval> intervalsByEndThenStart()
  {
    return INTERVAL_BY_END_THEN_START;
  }

  public static ArrayList<Interval> condenseIntervals(Iterable<Interval> intervals)
  {
    ArrayList<Interval> retVal = Lists.newArrayList();

    TreeSet<Interval> sortedIntervals = Sets.newTreeSet(JodaUtils.intervalsByStartThenEnd());
    for (Interval interval : intervals) {
      sortedIntervals.add(interval);
    }

    if (sortedIntervals.isEmpty()) {
      return Lists.newArrayList();
    }

    Iterator<Interval> intervalsIter = sortedIntervals.iterator();
    Interval currInterval = intervalsIter.next();
    while (intervalsIter.hasNext()) {
      Interval next = intervalsIter.next();

      if (currInterval.overlaps(next) || currInterval.abuts(next)) {
        currInterval = new Interval(currInterval.getStart(), next.getEnd());
      } else {
        retVal.add(currInterval);
        currInterval = next;
      }
    }
    retVal.add(currInterval);

    return retVal;
  }

  public static Interval umbrellaInterval(Iterable<Interval> intervals)
  {
    return umbrellaInterval(intervals.iterator());
  }

  public static Interval umbrellaInterval(Iterator<Interval> intervals)
  {
    if (!intervals.hasNext()) {
      throw new IllegalArgumentException("Empty list of intervals");
    }
    Interval interval = intervals.next();
    DateTime min = interval.getStart();
    DateTime max = interval.getEnd();

    while (intervals.hasNext()) {
      Interval other = intervals.next();
      DateTime start = other.getStart();
      if (start.isBefore(min)) {
        min = start;
      }
      DateTime end = other.getEnd();
      if (end.isAfter(max)) {
        max = end;
      }
    }
    return new Interval(min, max);
  }

  public static boolean contains(final long timestamp, final Iterable<Interval> intervals)
  {
    return Iterables.any(
        intervals, new Predicate<Interval>()
        {
          @Override
          public boolean apply(Interval input)
          {
            return input.contains(timestamp);
          }
        }
    );
  }

  public static boolean overlaps(final Interval i, Iterable<Interval> intervals)
  {
    return Iterables.any(
        intervals, new Predicate<Interval>()
        {
          @Override
          public boolean apply(Interval input)
          {
            return input.overlaps(i);
          }
        }
    );
  }

  public static List<Interval> overlapping(final Interval interval, Iterable<Interval> intervals)
  {
    return Lists.newArrayList(Iterables.filter(
        intervals, new Predicate<Interval>()
        {
          @Override
          public boolean apply(Interval input)
          {
            return input.overlaps(interval);
          }
        }
    ));
  }

  public static Iterable<Interval> split(final Granularity granularity, final Iterable<Interval> intervals)
  {
    if (granularity == null) {
      return intervals;
    }
    return GuavaUtils.explode(
        intervals,
        new Function<Interval, Iterable<Interval>>()
        {
          @Override
          public Iterable<Interval> apply(Interval input)
          {
            return granularity.getIterable(input);
          }
        }
    );
  }

  public static Period toPeriod(String string)
  {
    Period period = new Period();
    int prev = 0;
    char[] chars = string.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (!Character.isDigit(chars[i])) {
        int value = Integer.parseInt(string.substring(prev, i));
        switch (chars[i]) {
          case 'y':
          case 'Y':
            period = period.plusYears(value);
            break;
          case 'M':
            period = period.plusMonths(value);
            break;
          case 'w':
          case 'W':
            period = period.plusWeeks(value);
            break;
          case 'd':
          case 'D':
            period = period.plusDays(value);
            break;
          case 'h':
          case 'H':
            period = period.plusHours(value);
            break;
          case 'm':
            period = period.plusMinutes(value);
            break;
          case 's':
            period = period.plusSeconds(value);
            break;
          default:
            throw new IllegalArgumentException("Not supported time unit " + chars[i]);
        }
        for (i++; i < chars.length && !Character.isDigit(chars[i]); i++) {
        }
        prev = i;
      }
    }
    return period;
  }

  // Z instead of ZZ (for compatible output with com.ibm.icu.SimpleDateFormat)
  public static final String STANDARD_PARSER_FORMAT = "yyyy-MM-dd[('T'|' ')HH:mm:ss][.SSS][Z]";
  public static final DateTimeFormatter STANDARD_PARSER = toTimeFormatter(STANDARD_PARSER_FORMAT);

  public static final String STANDARD_PRINTER_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  public static final DateTimeFormatter STANDARD_PRINTER = toTimeFormatter(STANDARD_PRINTER_FORMAT);

  public static DateTimeFormatter toTimeFormatterQuoted(String formatString)
  {
    Preconditions.checkArgument(!StringUtils.isNullOrEmpty(formatString));
    Preconditions.checkArgument(formatString.charAt(0) == '\'', "not quoted");
    int index = formatString.lastIndexOf('\'');
    if (index < 0) {
      throw new IllegalArgumentException("not matching quote end in " + formatString);
    }
    String timeZone = null;
    String locale = null;
    if (index + 1 < formatString.length()) {
      String substring = formatString.substring(index + 1).trim();
      if (substring.charAt(0) == ',') {
        substring = substring.substring(1, substring.length());
      }
      if (!substring.isEmpty()) {
        String[] parameters = substring.split(",");
        timeZone = parameters[0].trim();
        locale = parameters.length > 1 ? parameters[1].trim() : null;
      }
    }
    return toTimeFormatter(formatString.substring(1, index), timeZone, locale);
  }

  public static DateTimeFormatter toTimeFormatter(String formatString)
  {
    return toTimeFormatter(formatString, null, null);
  }

  public static DateTimeFormatter toTimeFormatter(String formatString, String timeZone, String locale)
  {
    DateTimeFormatterBuilder b = new DateTimeFormatterBuilder();
    int prev = 0;
    boolean escape = false;
    for (int i = 0; i < formatString.length(); ) {
      char c = formatString.charAt(i);
      if (c == '\'') {
        escape = !escape;
      }
      if (escape) {
        i++;
        continue;
      }
      if (c == '[') {
        if (i > prev) {
          b.append(DateTimeFormat.forPattern(formatString.substring(prev, i)));
        }
        int seek = TypeUtils.seekWithEscape(formatString, i + 1, ']');  // don't support nested optionals
        if (seek < 0) {
          throw new IAE("not matching ']' in %s", formatString);
        }
        // there is no optional printer
        b.appendOptional(toTimeFormatter(formatString.substring(i + 1, seek), timeZone, locale).getParser());
        prev = i = seek + 1;
      } else if (c == '(') {
        if (i > prev) {
          b.append(DateTimeFormat.forPattern(formatString.substring(prev, i)));
        }
        int seek = TypeUtils.seekWithEscape(formatString, i + 1, ')');  // don't support nested optionals
        if (seek < 0) {
          throw new IAE("not matching ')' in %s", formatString);
        }
        List<DateTimeParser> formatters = Lists.newArrayList();
        for (String element : formatString.substring(i + 1, seek).split("\\|")) {
          formatters.add(toTimeFormatter(element, timeZone, locale).getParser());
        }
        b.append(null, formatters.toArray(new DateTimeParser[0]));
        prev = i = seek + 1;
      } else if (c == 'q' || c == 'Q') {
        if (i > prev) {
          b.append(DateTimeFormat.forPattern(formatString.substring(prev, i)));
        }
        int length = 0;
        for (; i < formatString.length() && formatString.charAt(i) == c; i++) {
          length++;
        }
        if (length >= 3) {
          b.appendLiteral('Q');
          b.appendDecimal(new QuarterFieldType(), length - 2, 1);
        } else {
          b.appendDecimal(new QuarterFieldType(), length, 1);
        }
        prev = i;
      } else if (c == 'W') {
        int length = 0;
        for (; i < formatString.length() && formatString.charAt(i) == c; i++) {
          length++;
        }
        b.appendDecimal(new WeekInMonthFieldType(), length, 1);
        prev = i;
      } else {
        i++;
      }
    }
    if (prev < formatString.length()) {
      b.append(DateTimeFormat.forPattern(formatString.substring(prev, formatString.length())));
    }
    DateTimeFormatter formatter = b.toFormatter();
    if (locale != null) {
      formatter = formatter.withLocale(toLocale(locale));
    }
    if (timeZone != null) {
      formatter = formatter.withZone(toTimeZone(timeZone));
    }
    return formatter;
  }

  // DateTimeZone.forID cannot handle abbreviations like PST
  public static DateTimeZone toTimeZone(String timeZone)
  {
    if (timeZone == null) {
      return null;
    }
    TimeZone tz = ZoneInfo.getTimeZone(timeZone);
    if (tz != null) {
      return DateTimeZone.forTimeZone(tz);
    }
    try {
      return DateTimeZone.forID(timeZone);
    }
    catch (IllegalArgumentException e) {
      // ignore
    }
    return DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZone));
  }

  public static Locale toLocale(String locale)
  {
    return locale == null ? null : new Locale(locale);
  }

  public static String printTo(DateTimeFormatter formatter, StringBuilder builder, long instant)
  {
    try {
      builder.setLength(0);
      formatter.printTo(builder, instant);
    }
    catch (IOException ex) {
      // StringBuilder does not throw IOException
    }
    return builder.toString();
  }

  public static String printTo(DateTimeFormatter formatter, StringBuilder builder, DateTime instant)
  {
    try {
      builder.setLength(0);
      formatter.printTo(builder, instant);
    }
    catch (IOException ex) {
      // StringBuilder does not throw IOException
    }
    return builder.toString();
  }
}
