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
    StringComparator comparator = StringComparators.makeComparator(
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
    StringComparator comparator = StringComparators.makeComparator(
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

  @Test
  public void testStringArray()
  {
    List<String> x1 = Arrays.asList("spot", "total_market", "upfront");
    List<String> x2 = Arrays.asList(
        "automotive",
        "business",
        "entertainment",
        "health",
        "mezzanine",
        "news",
        "premium",
        "technology",
        "travel"
    );

    List<String> cartesian = Lists.newArrayList();
    for (String a : x1) {
      for (String y : x2) {
        cartesian.add(a + "\u0001" + y);
      }
    }
    List<String> sorted = Lists.newArrayList(cartesian);
    Collections.shuffle(cartesian);
    Collections.sort(cartesian, StringComparators.makeComparator("stringarray.\u0001"));
    Assert.assertEquals(sorted, cartesian);
  }

  @Test
  public void testDateTime()
  {
    List<String> x = Arrays.asList("09 01 2018", "03 02 2017", "12 03 2013", "04 21 2015", "06 11 2019");
    Collections.sort(x, StringComparators.makeComparator("datetime.'MM dd yyyy'"));
    Assert.assertEquals(Arrays.asList("12 03 2013", "04 21 2015", "03 02 2017", "09 01 2018", "06 11 2019"), x);

    x = Arrays.asList("Oct 01 2018", "Mar 02 2017", "Dec 03 2013", "Apr 21 2015", "Jun 11 2019");
    Collections.sort(x, StringComparators.makeComparator("datetime.'MMM dd yyyy'"));
    Assert.assertEquals(Arrays.asList("Dec 03 2013", "Apr 21 2015", "Mar 02 2017", "Oct 01 2018", "Jun 11 2019"), x);

    x = Arrays.asList("01 Oct 2018", "02 Mar 2017", "03 Dec 2013", "21 Apr 2015", "11 Jun 2019");
    Collections.sort(x, StringComparators.makeComparator("datetime.'dd MMM yyyy'"));
    Assert.assertEquals(Arrays.asList("03 Dec 2013", "21 Apr 2015", "02 Mar 2017", "01 Oct 2018", "11 Jun 2019"), x);

    x = Arrays.asList("01 10월,2018", "02 3월,2017", "03 12월,2013", "21 4월,2015", "11 6월,2019");
    Collections.sort(x, StringComparators.makeComparator("datetime.'dd MMM,yyyy',UTC,ko"));
    Assert.assertEquals(Arrays.asList("03 12월,2013", "21 4월,2015", "02 3월,2017", "01 10월,2018", "11 6월,2019"), x);

    x = Arrays.asList("Week 45, 2012", "Week 36, 2013", "Week 01, 2014", "Week 03, 2013", "Week 17, 2013");
    Collections.sort(x, StringComparators.makeComparator("datetime.''Week' w, xxxx'"));
    Assert.assertEquals(Arrays.asList("Week 45, 2012", "Week 03, 2013", "Week 17, 2013", "Week 36, 2013", "Week 01, 2014"), x);
  }
}
