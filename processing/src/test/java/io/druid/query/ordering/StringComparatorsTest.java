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

package io.druid.query.ordering;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.ValueDesc;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormatSymbols;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class StringComparatorsTest
{
  @Test
  public void testNull()
  {
    StringComparator comparator = StringComparators.LEXICOGRAPHIC;
    Assert.assertEquals(0, comparator.compare(null, ""));
    Assert.assertEquals(0, comparator.compare("", null));
    Assert.assertEquals(0, comparator.compare(null, null));
    Assert.assertEquals(0, comparator.compare("", ""));

    Assert.assertEquals(-1, comparator.compare("", "x"));
    Assert.assertEquals(-1, comparator.compare(null, "x"));
    Assert.assertEquals(1, comparator.compare("x", ""));
    Assert.assertEquals(1, comparator.compare("x", null));
  }

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
    Comparator comparator = StringComparators.makeComparator(
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
    Comparator comparator = StringComparators.makeComparator(
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
    Collections.sort(cartesian, StringComparators.makeComparator("stringarray(\u0001)"));
    Assert.assertEquals(sorted, cartesian);
  }

  @Test
  public void testStringArrayComplex()
  {
    List<String> x1 = Arrays.asList("spot", "total_market", "upfront");
    List<String> x2 = Arrays.asList(
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    );

    List<String> cartesian = Lists.newArrayList();
    for (String a : x1) {
      for (String y : x2) {
        cartesian.add(a + "\u0001" + y);
      }
    }
    List<String> expected = Arrays.asList(
        "upfrontMonday", "upfrontTuesday", "upfrontWednesday", "upfrontThursday", "upfrontFriday", "upfrontSaturday",
        "upfrontSunday", "total_marketMonday", "total_marketTuesday", "total_marketWednesday", "total_marketThursday",
        "total_marketFriday", "total_marketSaturday", "total_marketSunday", "spotMonday", "spotTuesday",
        "spotWednesday", "spotThursday", "spotFriday", "spotSaturday", "spotSunday"
    );
    Collections.shuffle(cartesian);

    String name = "stringarray(\u0001,lexicographic:desc,dayofweek.en)";
    Comparator c = StringComparators.makeComparator(name);
    Assert.assertEquals(name, c.toString());
    Collections.sort(cartesian, c);
    Assert.assertEquals(expected, cartesian);
  }

  @Test
  public void testDateTime()
  {
    List<String> x = Arrays.asList("09 01 2018", "03 02 2017", "12 03 2013", "04 21 2015", "06 11 2019");
    Collections.sort(x, StringComparators.makeComparator("datetime('MM dd yyyy')"));
    Assert.assertEquals(Arrays.asList("12 03 2013", "04 21 2015", "03 02 2017", "09 01 2018", "06 11 2019"), x);

    x = Arrays.asList("Oct 01 2018", "Mar 02 2017", "Dec 03 2013", "Apr 21 2015", "Jun 11 2019");
    Collections.sort(x, StringComparators.makeComparator("datetime('MMM dd yyyy')"));
    Assert.assertEquals(Arrays.asList("Dec 03 2013", "Apr 21 2015", "Mar 02 2017", "Oct 01 2018", "Jun 11 2019"), x);

    x = Arrays.asList("01 Oct 2018", "02 Mar 2017", "03 Dec 2013", "21 Apr 2015", "11 Jun 2019");
    Collections.sort(x, StringComparators.makeComparator("datetime('dd MMM yyyy')"));
    Assert.assertEquals(Arrays.asList("03 Dec 2013", "21 Apr 2015", "02 Mar 2017", "01 Oct 2018", "11 Jun 2019"), x);

    x = Arrays.asList("01 10월,2018", "02 3월,2017", "03 12월,2013", "21 4월,2015", "11 6월,2019");
    Collections.sort(x, StringComparators.makeComparator("datetime('dd MMM,yyyy',UTC,ko)"));
    Assert.assertEquals(Arrays.asList("03 12월,2013", "21 4월,2015", "02 3월,2017", "01 10월,2018", "11 6월,2019"), x);

    x = Arrays.asList("Week 45, 2012", "Week 36, 2013", "Week 01, 2014", "Week 03, 2013", "Week 17, 2013");
    Collections.sort(x, StringComparators.makeComparator("datetime(''Week' w, xxxx')"));
    Assert.assertEquals(Arrays.asList("Week 45, 2012", "Week 03, 2013", "Week 17, 2013", "Week 36, 2013", "Week 01, 2014"), x);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStructType()
  {
    Set x1 = Sets.newHashSet(100L, 200L, 300L);
    Set x2 = Sets.newHashSet("aaa", "bbb", "ccc");
    Set x3 = Sets.newHashSet("Thursday", "Friday");
    List<Object[]> arrayList = Lists.newArrayList(
        Iterables.transform(
            Sets.cartesianProduct(x1, x2, x3), new Function<List<Object>, Object[]>()
            {
              @Override
              public Object[] apply(List<Object> input)
              {
                return input.toArray(new Object[3]);
              }
            }
        )
    );
    Collections.shuffle(arrayList);
    Object[][] values = arrayList.toArray(new Object[0][]);

    Comparator<Object[]> c = StringComparators.makeComparator(ValueDesc.of("struct(long,string,string)"), null);
    Arrays.sort(values, c);

    Object[][] expected = new Object[][] {
        a(100L, "aaa", "Friday"), a(100L, "aaa", "Thursday"), a(100L, "bbb", "Friday"), a(100L, "bbb", "Thursday"),
        a(100L, "ccc", "Friday"), a(100L, "ccc", "Thursday"), a(200L, "aaa", "Friday"), a(200L, "aaa", "Thursday"),
        a(200L, "bbb", "Friday"), a(200L, "bbb", "Thursday"), a(200L, "ccc", "Friday"), a(200L, "ccc", "Thursday"),
        a(300L, "aaa", "Friday"), a(300L, "aaa", "Thursday"), a(300L, "bbb", "Friday"), a(300L, "bbb", "Thursday"),
        a(300L, "ccc", "Friday"), a(300L, "ccc", "Thursday")
    };
    Assert.assertArrayEquals(expected, values);


  }

  private static Object[] a(Object... x)
  {
    return x;
  }
}
