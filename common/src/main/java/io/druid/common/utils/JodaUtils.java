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

package io.druid.common.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Locale;
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
        int compare = Longs.compare(lhs.getStartMillis(), rhs.getStartMillis());
        if (compare == 0) {
          return Longs.compare(lhs.getEndMillis(), rhs.getEndMillis());
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
        int compare = Longs.compare(lhs.getEndMillis(), rhs.getEndMillis());
        if (compare == 0) {
          return Longs.compare(lhs.getStartMillis(), rhs.getStartMillis());
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
    ArrayList<DateTime> startDates = Lists.newArrayList();
    ArrayList<DateTime> endDates = Lists.newArrayList();

    for (Interval interval : intervals) {
      startDates.add(interval.getStart());
      endDates.add(interval.getEnd());
    }

    DateTime minStart = minDateTime(startDates.toArray(new DateTime[startDates.size()]));
    DateTime maxEnd = maxDateTime(endDates.toArray(new DateTime[endDates.size()]));

    if (minStart == null || maxEnd == null) {
      throw new IllegalArgumentException("Empty list of intervals");
    }
    return new Interval(minStart, maxEnd);
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

  public static DateTime minDateTime(DateTime... times)
  {
    if (times == null) {
      return null;
    }

    switch (times.length) {
      case 0:
        return null;
      case 1:
        return times[0];
      default:
        DateTime min = times[0];
        for (int i = 1; i < times.length; ++i) {
          min = min.isBefore(times[i]) ? min : times[i];
        }
        return min;
    }
  }

  public static DateTime maxDateTime(DateTime... times)
  {
    if (times == null) {
      return null;
    }

    switch (times.length) {
      case 0:
        return null;
      case 1:
        return times[0];
      default:
        DateTime max = times[0];
        for (int i = 1; i < times.length; ++i) {
          max = max.isAfter(times[i]) ? max : times[i];
        }
        return max;
    }
  }

  public static long toDuration(String string)
  {
    DateTime duration = new DateTime(0);
    int prev = 0;
    char[] chars = string.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (!Character.isDigit(chars[i])) {
        int value = Integer.parseInt(string.substring(prev, i));
        switch (chars[i]) {
          case 'y':
          case 'Y':
            duration = duration.plusYears(value);
            break;
          case 'M':
            duration = duration.plusMonths(value);
            break;
          case 'w':
          case 'W':
            duration = duration.plusWeeks(value);
            break;
          case 'd':
          case 'D':
            duration = duration.plusDays(value);
            break;
          case 'h':
          case 'H':
            duration = duration.plusHours(value);
            break;
          case 'm':
            duration = duration.plusMinutes(value);
            break;
          case 's':
            duration = duration.plusSeconds(value);
            break;
          default:
            throw new IllegalArgumentException("Not supported time unit " + chars[i]);
        }
        for (i++; i < chars.length && !Character.isDigit(chars[i]); i++) {
        }
        prev = i;
      }
    }
    return duration.getMillis();
  }

  public static DateTimeFormatter toTimeFormatter(String formatString)
  {
    return toTimeFormatter(formatString, null, null);
  }

  public static DateTimeFormatter toTimeFormatter(String formatString, String locale, String timeZone)
  {
    DateTimeFormatterBuilder b = new DateTimeFormatterBuilder();
    int prev = 0;
    boolean escape = false;
    for (int i = 0; i < formatString.length(); i++) {
      char c = formatString.charAt(i);
      if (c == '\'') {
        escape = !escape;
      }
      if (escape) {
        continue;
      }
      if (c == '[') {
        if (i > prev) {
          b.append(DateTimeFormat.forPattern(formatString.substring(prev, i)));
          prev = i + 1;
        }
      } else if (c == ']') {
        if (i > prev) {
          b.appendOptional(DateTimeFormat.forPattern(formatString.substring(prev, i)).getParser());
          prev = i + 1;
        }
      }
    }
    if (prev < formatString.length()) {
      b.append(DateTimeFormat.forPattern(formatString.substring(prev, formatString.length())));
    }
    DateTimeFormatter formatter = b.toFormatter();
    if (locale != null) {
      formatter = formatter.withLocale(new Locale(locale));
    }
    if (timeZone != null) {
      formatter = formatter.withZone(DateTimeZone.forID(timeZone));
    }
    return formatter;
  }
}
