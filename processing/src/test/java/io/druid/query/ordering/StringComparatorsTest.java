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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormatSymbols;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class StringComparatorsTest
{

  @Test
  public void testLocaledTimes()
  {
    testDayOfWeek(null);
    testDayOfWeek("ko");
    testDayOfWeek("en");

    testMonth(null);
    testMonth("ko");
    testMonth("en");
  }

  private void testDayOfWeek(String language)
  {
    boolean defaultLocale = language == null;
    DateFormatSymbols symbols = defaultLocale ? new DateFormatSymbols() : new DateFormatSymbols(new Locale(language));
    StringComparators.StringComparator comparator = StringComparators.makeComparator(
        defaultLocale ? "dayofweek" : "dayofweek." + language
    );
    for (String[] dayOfWeek : new String[][]{symbols.getWeekdays(), symbols.getShortWeekdays()}) {
      List<String> expected = Arrays.<String>asList(
          dayOfWeek[Calendar.MONDAY],
          dayOfWeek[Calendar.TUESDAY],
          dayOfWeek[Calendar.WEDNESDAY],
          dayOfWeek[Calendar.THURSDAY],
          dayOfWeek[Calendar.FRIDAY],
          dayOfWeek[Calendar.SATURDAY],
          dayOfWeek[Calendar.SUNDAY]
      );
      List<String> shuffle = Lists.newArrayList(expected);
      Collections.shuffle(shuffle);
      Collections.sort(shuffle, comparator);
      Assert.assertEquals(expected, shuffle);
    }
  }

  private void testMonth(String language)
  {
    boolean defaultLocale = language == null;
    DateFormatSymbols symbols = defaultLocale ? new DateFormatSymbols() : new DateFormatSymbols(new Locale(language));
    StringComparators.StringComparator comparator = StringComparators.makeComparator(
        defaultLocale ? "month" : "month." + language
    );
    for (String[] months : new String[][]{symbols.getMonths(), symbols.getShortMonths()}) {
      List<String> expected = Arrays.<String>asList(
          months[Calendar.JANUARY],
          months[Calendar.FEBRUARY],
          months[Calendar.MARCH],
          months[Calendar.APRIL],
          months[Calendar.MAY],
          months[Calendar.JUNE],
          months[Calendar.JULY],
          months[Calendar.AUGUST],
          months[Calendar.SEPTEMBER],
          months[Calendar.OCTOBER],
          months[Calendar.NOVEMBER],
          months[Calendar.DECEMBER]
      );
      List<String> shuffle = Lists.newArrayList(expected);
      Collections.shuffle(shuffle);
      Collections.sort(shuffle, comparator);
      Assert.assertEquals(expected, shuffle);
    }
  }
}
